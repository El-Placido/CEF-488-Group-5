/*
 * coordinator.c
 * =============
 * Project 4: Distributed Data Processing System — Coordinator (master)
 *
 * RESPONSIBILITIES
 * -----------------
 *   1. Read the input file and split it into fixed-size chunks.
 *   2. Listen on a UDP socket for worker registrations.
 *   3. ACCEPT NEW WORKERS AT ANY TIME — dynamic registration. A worker
 *      that registers after processing has started is immediately
 *      handed the next pending chunk.
 *   4. Hand out chunks from a work queue; whenever a worker's result
 *      arrives, send it the next chunk (dynamic load balancing — the
 *      same principle as Lab 5, applied here over UDP).
 *   5. Detect dead/slow workers with a per-chunk timeout (checked once
 *      per epoll_wait() timeout tick) and REASSIGN their chunk to
 *      another worker — this is the fault-tolerance requirement.
 *   6. Detect workers that send MSG_LEAVE and remove them from the
 *      worker table without losing the work they had in flight (their
 *      current chunk is re-queued exactly like a fault).
 *   7. When every chunk has a result, REDUCE all partial results into
 *      the final answer, print it, and tell every connected worker
 *      MSG_NO_MORE_WORK so they exit cleanly.
 *
 * WHY epoll ON A UDP SOCKET?
 * ----------------------------
 * UDP is datagram-oriented and connectionless, but the coordinator
 * still must not block while waiting for one specific worker's
 * datagram, because many other workers may have data ready at the same
 * time, and some workers may be slow or dead. We register the single
 * UDP socket with epoll in edge-triggered mode and drain all pending
 * datagrams (recvfrom in a loop until EAGAIN) on every wake-up. This
 * lets the coordinator manage an arbitrary number of workers from one
 * non-blocking event loop, exactly like Lab 4's epoll HTTP server.
 *
 * FAULT TOLERANCE DESIGN
 * ------------------------
 * Every chunk that is "in flight" (sent to some worker, no result yet)
 * has a `sent_at` timestamp recorded in the chunk table. Once per
 * second (epoll_wait timeout = 1000ms) the coordinator scans all
 * in-flight chunks; any chunk whose age exceeds WORKER_TIMEOUT_SEC is
 * assumed lost — either the worker died, or the UDP packet (task or
 * result) was dropped. The chunk goes back to the front of the pending
 * queue and is handed to the next worker that asks for work. This
 * single mechanism transparently covers BOTH "worker crashed" and
 * "packet lost on an unreliable network" — exactly the fault tolerance
 * UDP requires, since UDP itself guarantees neither delivery nor
 * worker liveness.
 *
 * USAGE
 * -----
 *   ./coordinator -f <input_file> -p <port> -t <task> [-c <chunk_bytes>]
 *
 *   -f <file>     Input dataset to process (required)
 *   -p <port>     UDP port to listen on (default: 9100)
 *   -t <task>     Task name: wordcount | linecount | sumnumbers | charcount
 *   -c <bytes>    Chunk size in bytes (default: 1024, max: 1024)
 *
 * CEF488 Group Project — Project 4: Distributed Data Processing System
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <ctype.h>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "tasks.h"

/* ====================================================================
 * Configuration constants
 * ==================================================================== */
#define MAX_WORKERS         64     /* maximum tracked worker slots       */
#define MAX_CHUNKS          131072   /* maximum chunks (file size limit)   */
#define WORKER_TIMEOUT_SEC  8      /* chunk considered lost after this   */
#define EPOLL_TICK_MS       1000   /* epoll_wait timeout: check once/sec */

/* ====================================================================
 * Chunk state — one entry per chunk of the input file
 * ==================================================================== */
typedef enum {
    CHUNK_PENDING,    /* not yet sent to any worker                     */
    CHUNK_IN_FLIGHT,  /* sent, waiting for a result                     */
    CHUNK_DONE        /* result received and folded into the total      */
} chunk_state_t;

typedef struct {
    uint32_t       chunk_id;
    uint32_t       data_len;
    char           data[MAX_CHUNK_BYTES];
    chunk_state_t  state;
    int            assigned_worker;   /* index into workers[], or -1     */
    time_t         sent_at;           /* when last sent (for timeout)    */
    int64_t        result;            /* filled once state == CHUNK_DONE */
} chunk_t;

/* ====================================================================
 * Worker state — one entry per registered worker
 * ==================================================================== */
typedef struct {
    int                in_use;        /* 0 = empty slot                  */
    uint32_t           worker_id;
    struct sockaddr_in addr;          /* worker's UDP address             */
    int                current_chunk; /* index into chunks[], or -1      */
    time_t             last_seen;     /* last datagram received from it  */
    int                chunks_done;   /* how many chunks it has finished */
} worker_t;

/* ====================================================================
 * Globals
 * ==================================================================== */
static volatile sig_atomic_t g_running = 1;
static void sig_handler(int s) { (void)s; g_running = 0; }

static chunk_t   g_chunks[MAX_CHUNKS];
static int       g_num_chunks  = 0;
static int       g_chunks_done = 0;

static worker_t  g_workers[MAX_WORKERS];
static uint32_t  g_next_worker_id = 1;

static int64_t   g_accumulator;
static uint8_t   g_task_id;
static uint32_t  g_seq_counter = 1;

/* ====================================================================
 * split_file_into_chunks()
 *
 * Reads `filename` into memory, then cuts it into chunks of AT MOST
 * `chunk_bytes` bytes — but every chunk boundary is adjusted backward
 * to the nearest preceding whitespace character (space, tab, newline).
 *
 * WHY THIS MATTERS
 * -----------------
 * A naive fixed-byte split can cut a token in half: e.g. the number
 * "-123" might be split into "-1" in one chunk and "23" in the next,
 * which are then summed as two SEPARATE numbers (-1 + 23 = 22 instead
 * of -123) — a genuinely wrong result, not just an off-by-one. Cutting
 * only at whitespace guarantees every whitespace-delimited token
 * (word or number) is delivered whole to exactly one chunk, making
 * map_wordcount(), map_sumnumbers(), and map_charcount() all exact.
 * map_linecount() was already exact since it only counts '\n' bytes.
 *
 * Returns the number of chunks created, or -1 on I/O error.
 * ==================================================================== */
static int split_file_into_chunks(const char *filename, uint32_t chunk_bytes)
{
    FILE *fp = fopen(filename, "rb");
    if (!fp) { perror("fopen"); return -1; }

    /* Read the whole file into a heap buffer */
    if (fseek(fp, 0, SEEK_END) != 0) { perror("fseek"); fclose(fp); return -1; }
    long fsize = ftell(fp);
    if (fsize < 0) { perror("ftell"); fclose(fp); return -1; }
    rewind(fp);

    char *filebuf = malloc((size_t)fsize > 0 ? (size_t)fsize : 1);
    if (!filebuf) { fclose(fp); return -1; }

    size_t read_total = fread(filebuf, 1, (size_t)fsize, fp);
    fclose(fp);

    int    n   = 0;
    size_t pos = 0;

    while (pos < read_total && n < MAX_CHUNKS) {
        size_t remaining = read_total - pos;
        size_t take = (remaining < chunk_bytes) ? remaining : chunk_bytes;
        size_t end  = pos + take;   /* tentative chunk end (exclusive) */

        /*
         * If we are not at the very end of the file, walk `end`
         * backward until it sits right after a whitespace character
         * (so the chunk does not end mid-token). If the chunk is
         * smaller than one token (extremely small chunk_bytes), we
         * fall back to the original fixed cut to guarantee progress.
         */
        if (end < read_total) {
            size_t scan = end;
            while (scan > pos && !isspace((unsigned char)filebuf[scan - 1]))
                scan--;
            if (scan > pos) end = scan;   /* found a safe boundary       */
            /* else: no whitespace in this whole window — cut as-is     */
        }

        size_t this_len = end - pos;
        if (this_len > chunk_bytes) this_len = chunk_bytes; /* safety clamp */

        memcpy(g_chunks[n].data, filebuf + pos, this_len);
        g_chunks[n].chunk_id        = (uint32_t)n;
        g_chunks[n].data_len        = (uint32_t)this_len;
        g_chunks[n].state           = CHUNK_PENDING;
        g_chunks[n].assigned_worker = -1;
        g_chunks[n].sent_at         = 0;
        g_chunks[n].result          = 0;
        n++;

        pos += this_len;
        if (this_len == 0) break;   /* safety: avoid infinite loop      */
    }

    free(filebuf);

    if (pos < read_total) {
        fprintf(stderr,
                "[COORD] WARNING: MAX_CHUNKS (%d) reached — only %zu of %zu "
                "bytes were loaded! Increase MAX_CHUNKS or chunk size (-c) "
                "to process the full file.\n",
                MAX_CHUNKS, pos, read_total);
    }

    return n;
}

/* ====================================================================
 * find_or_create_worker()
 *
 * Looks up a worker by its UDP address.  If not found, allocates a
 * fresh slot — this IS the "dynamic worker registration" feature:
 * workers may appear at any time during processing and are accepted
 * immediately on their first MSG_REGISTER.
 * ==================================================================== */
static int find_worker_by_addr(const struct sockaddr_in *addr)
{
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (g_workers[i].in_use &&
            g_workers[i].addr.sin_addr.s_addr == addr->sin_addr.s_addr &&
            g_workers[i].addr.sin_port        == addr->sin_port) {
            return i;
        }
    }
    return -1;
}

static int allocate_worker_slot(const struct sockaddr_in *addr)
{
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (!g_workers[i].in_use) {
            g_workers[i].in_use        = 1;
            g_workers[i].worker_id     = g_next_worker_id++;
            g_workers[i].addr          = *addr;
            g_workers[i].current_chunk = -1;
            g_workers[i].last_seen     = time(NULL);
            g_workers[i].chunks_done   = 0;
            return i;
        }
    }
    return -1;   /* table full */
}

/* ====================================================================
 * find_pending_chunk()
 * Returns the index of the first CHUNK_PENDING entry, or -1 if none.
 * ==================================================================== */
static int find_pending_chunk(void)
{
    for (int i = 0; i < g_num_chunks; i++)
        if (g_chunks[i].state == CHUNK_PENDING)
            return i;
    return -1;
}

/* ====================================================================
 * send_task_to_worker()
 *
 * Sends chunk `ci` to worker `wi`.  Marks the chunk IN_FLIGHT and
 * records the send timestamp for the fault-tolerance timeout scan.
 * ==================================================================== */
static void send_task_to_worker(int sock, int wi, int ci)
{
    msg_header  hdr;
    task_payload payload;

    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_type  = MSG_TASK;
    hdr.task_id   = g_task_id;
    hdr.seq_num   = g_seq_counter++;
    hdr.chunk_id  = (uint32_t)g_chunks[ci].chunk_id;
    header_to_network(&hdr);

    payload.data_len = htonl(g_chunks[ci].data_len);
    memcpy(payload.data, g_chunks[ci].data, g_chunks[ci].data_len);

    char buf[sizeof(msg_header) + sizeof(task_payload)];
    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), &payload,
           sizeof(payload.data_len) + g_chunks[ci].data_len);

    size_t send_len = sizeof(hdr) + sizeof(payload.data_len)
                     + g_chunks[ci].data_len;

    sendto(sock, buf, send_len, 0,
           (struct sockaddr *)&g_workers[wi].addr,
           sizeof(g_workers[wi].addr));

    g_chunks[ci].state           = CHUNK_IN_FLIGHT;
    g_chunks[ci].assigned_worker = wi;
    g_chunks[ci].sent_at         = time(NULL);
    g_workers[wi].current_chunk  = ci;

    fprintf(stdout,
            "[COORD] → worker %u: chunk %u (%u bytes)\n",
            g_workers[wi].worker_id, g_chunks[ci].chunk_id,
            g_chunks[ci].data_len);
}

/* ====================================================================
 * try_assign_work()
 *
 * If there is a pending chunk, send it to the given worker.
 * If there is NO pending work left, send MSG_NO_MORE_WORK instead so
 * the worker can exit gracefully (used both at the end of the job and
 * when a worker registers after all chunks are already assigned/done).
 * ==================================================================== */
static void try_assign_work(int sock, int wi)
{
    int ci = find_pending_chunk();
    if (ci != -1) {
        send_task_to_worker(sock, wi, ci);
        return;
    }

    /* Nothing pending right now */
    if (g_chunks_done >= g_num_chunks) {
        msg_header hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.msg_type = MSG_NO_MORE_WORK;
        hdr.seq_num  = g_seq_counter++;
        hdr.chunk_id = CHUNK_ID_NONE;
        header_to_network(&hdr);
        sendto(sock, &hdr, sizeof(hdr), 0,
               (struct sockaddr *)&g_workers[wi].addr,
               sizeof(g_workers[wi].addr));
        fprintf(stdout, "[COORD] → worker %u: NO_MORE_WORK (job complete)\n",
                g_workers[wi].worker_id);
    }
    /* else: all remaining chunks are in-flight elsewhere; this worker
     * will be given work as soon as something becomes available again
     * (next time it sends a heartbeat or another worker times out). */
}

/* ====================================================================
 * handle_register()
 * ==================================================================== */
static void handle_register(int sock, const struct sockaddr_in *peer)
{
    int wi = find_worker_by_addr(peer);
    if (wi == -1) {
        wi = allocate_worker_slot(peer);
        if (wi == -1) {
            fprintf(stderr, "[COORD] Worker table full — rejecting new worker\n");
            return;
        }
        fprintf(stdout, "[COORD] New worker registered: id=%u from %s:%d\n",
                g_workers[wi].worker_id,
                inet_ntoa(peer->sin_addr), ntohs(peer->sin_port));
    } else {
        /* Worker re-registering (e.g. rejoined after leaving) */
        g_workers[wi].last_seen = time(NULL);
        fprintf(stdout, "[COORD] Worker id=%u re-registered\n",
                g_workers[wi].worker_id);
    }

    /* ACK with the assigned worker_id */
    msg_header hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_type = MSG_REGISTER_ACK;
    hdr.task_id  = g_task_id;
    hdr.seq_num  = g_seq_counter++;
    hdr.chunk_id = CHUNK_ID_NONE;
    header_to_network(&hdr);

    register_ack_payload ack;
    ack.worker_id = htonl(g_workers[wi].worker_id);

    char buf[sizeof(hdr) + sizeof(ack)];
    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), &ack, sizeof(ack));
    sendto(sock, buf, sizeof(buf), 0,
           (struct sockaddr *)peer, sizeof(*peer));

    /* Immediately hand it work (dynamic registration: join anytime) */
    try_assign_work(sock, wi);
}

/* ====================================================================
 * handle_result()
 * ==================================================================== */
static void handle_result(int sock, const struct sockaddr_in *peer,
                          const msg_header *hdr, const char *body, size_t body_len)
{
    int wi = find_worker_by_addr(peer);
    if (wi == -1) {
        fprintf(stderr, "[COORD] Result from unknown worker — ignoring\n");
        return;
    }
    g_workers[wi].last_seen = time(NULL);

    uint32_t chunk_id = hdr->chunk_id;
    if (chunk_id >= (uint32_t)g_num_chunks) {
        fprintf(stderr, "[COORD] Result for bogus chunk_id %u — ignoring\n",
                chunk_id);
        return;
    }

    /*
     * De-duplication: if this chunk is already CHUNK_DONE (e.g. the
     * coordinator's timeout fired and reassigned it to another worker
     * who already replied, and THEN this late/duplicate datagram from
     * the original worker arrives), we ACK politely but do not double-
     * count the result.
     */
    if (g_chunks[chunk_id].state == CHUNK_DONE) {
        fprintf(stdout,
                "[COORD] Duplicate/late result for chunk %u from worker %u — "
                "ignoring (already counted)\n",
                chunk_id, g_workers[wi].worker_id);
    } else if (body_len < sizeof(result_payload)) {
        fprintf(stderr, "[COORD] Malformed result payload — ignoring\n");
        return;
    } else {
        result_payload rp;
        memcpy(&rp, body, sizeof(rp));
        int64_t value = (int64_t)ntoh64((uint64_t)rp.result_value);

        g_chunks[chunk_id].state  = CHUNK_DONE;
        g_chunks[chunk_id].result = value;
        g_chunks_done++;

        const task_descriptor *t = get_task(g_task_id);
        g_accumulator = t->reduce(g_accumulator, value);

        g_workers[wi].chunks_done++;
        fprintf(stdout,
                "[COORD] ← worker %u: chunk %u result=%lld  (%d/%d chunks done)\n",
                g_workers[wi].worker_id, chunk_id, (long long)value,
                g_chunks_done, g_num_chunks);
    }

    g_workers[wi].current_chunk = -1;

    /* ACK the result, then immediately give this worker more work */
    msg_header ack_hdr;
    memset(&ack_hdr, 0, sizeof(ack_hdr));
    ack_hdr.msg_type = MSG_RESULT_ACK;
    ack_hdr.seq_num  = g_seq_counter++;
    ack_hdr.chunk_id = hdr->chunk_id;
    header_to_network(&ack_hdr);
    sendto(sock, &ack_hdr, sizeof(ack_hdr), 0,
           (struct sockaddr *)peer, sizeof(*peer));

    try_assign_work(sock, wi);
}

/* ====================================================================
 * handle_leave()
 *
 * A worker leaving gracefully (MSG_LEAVE) is removed from the table.
 * If it had a chunk in flight, that chunk is re-queued immediately
 * rather than waiting for the timeout — a graceful departure should
 * not cost us the full WORKER_TIMEOUT_SEC delay.
 * ==================================================================== */
static void handle_leave(const struct sockaddr_in *peer)
{
    int wi = find_worker_by_addr(peer);
    if (wi == -1) return;

    fprintf(stdout, "[COORD] Worker %u is leaving gracefully\n",
            g_workers[wi].worker_id);

    if (g_workers[wi].current_chunk != -1) {
        int ci = g_workers[wi].current_chunk;
        fprintf(stdout,
                "[COORD] Re-queuing chunk %u (worker left before finishing)\n",
                g_chunks[ci].chunk_id);
        g_chunks[ci].state           = CHUNK_PENDING;
        g_chunks[ci].assigned_worker = -1;
    }

    g_workers[wi].in_use = 0;
}

/* ====================================================================
 * handle_heartbeat()
 * Just refresh last_seen; used by idle workers waiting for new chunks
 * after a NO_MORE_WORK race (a worker registered right as the job
 * finished) is not the typical case, but heartbeats let an idle worker
 * remain known to the coordinator without consuming a chunk slot.
 * ==================================================================== */
static void handle_heartbeat(const struct sockaddr_in *peer)
{
    int wi = find_worker_by_addr(peer);
    if (wi != -1) g_workers[wi].last_seen = time(NULL);
}

/* ====================================================================
 * scan_for_timeouts()
 *
 * FAULT TOLERANCE: called once per epoll tick.  Any IN_FLIGHT chunk
 * whose `sent_at` is older than WORKER_TIMEOUT_SEC is assumed lost —
 * either the worker died or a packet (task or result) was dropped on
 * the unreliable UDP transport.  We free the worker's slot (assume
 * dead) and re-queue its chunk.
 * ==================================================================== */
static void scan_for_timeouts(void)
{
    time_t now = time(NULL);

    for (int i = 0; i < g_num_chunks; i++) {
        if (g_chunks[i].state != CHUNK_IN_FLIGHT) continue;
        if (now - g_chunks[i].sent_at < WORKER_TIMEOUT_SEC) continue;

        int wi = g_chunks[i].assigned_worker;
        fprintf(stdout,
                "[COORD] TIMEOUT: chunk %u (worker %u) — assuming worker dead, "
                "re-queuing\n",
                g_chunks[i].chunk_id,
                (wi >= 0 && g_workers[wi].in_use) ? g_workers[wi].worker_id : 0);

        if (wi >= 0 && g_workers[wi].in_use) {
            g_workers[wi].in_use = 0;   /* drop the presumed-dead worker */
        }

        g_chunks[i].state           = CHUNK_PENDING;
        g_chunks[i].assigned_worker = -1;
    }
}

/* ====================================================================
 * redistribute_pending()
 *
 * After a timeout frees up a chunk, push it to any IDLE registered
 * worker immediately, rather than waiting for that worker's next
 * message.  Without this, a chunk could sit PENDING forever if every
 * remaining worker is already busy with a different chunk and never
 * sends another message to trigger try_assign_work().
 * ==================================================================== */
static void redistribute_pending(int sock)
{
    for (int wi = 0; wi < MAX_WORKERS; wi++) {
        if (!g_workers[wi].in_use)              continue;
        if (g_workers[wi].current_chunk != -1)  continue;   /* already busy */
        if (find_pending_chunk() == -1)         break;       /* nothing left */
        try_assign_work(sock, wi);
    }
}

/* ====================================================================
 * print_worker_stats()
 * ==================================================================== */
static void print_worker_stats(void)
{
    fprintf(stdout, "\n[COORD] ---- Worker contribution summary ----\n");
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (!g_workers[i].in_use && g_workers[i].chunks_done == 0) continue;
        fprintf(stdout, "  worker %-4u : %d chunk(s) completed\n",
                g_workers[i].worker_id, g_workers[i].chunks_done);
    }
    fprintf(stdout, "----------------------------------------\n\n");
}

/* ====================================================================
 * main
 * ==================================================================== */
int main(int argc, char *argv[])
{
    const char *filename    = NULL;
    const char *port_str    = "9100";
    const char *task_str    = "wordcount";
    uint32_t    chunk_bytes = MAX_CHUNK_BYTES;

    int opt;
    while ((opt = getopt(argc, argv, "f:p:t:c:")) != -1) {
        switch (opt) {
        case 'f': filename    = optarg;       break;
        case 'p': port_str    = optarg;       break;
        case 't': task_str    = optarg;       break;
        case 'c': chunk_bytes = (uint32_t)atoi(optarg); break;
        default:
            fprintf(stderr,
                "Usage: %s -f <file> [-p port] [-t task] [-c chunk_bytes]\n"
                "  task: wordcount | linecount | sumnumbers | charcount\n",
                argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (!filename) {
        fprintf(stderr, "Error: -f <input_file> is required\n");
        return EXIT_FAILURE;
    }
    if (chunk_bytes == 0 || chunk_bytes > MAX_CHUNK_BYTES)
        chunk_bytes = MAX_CHUNK_BYTES;

    /* Resolve task name → task_id */
    if      (strcmp(task_str, "wordcount")  == 0) g_task_id = TASK_WORDCOUNT;
    else if (strcmp(task_str, "linecount")  == 0) g_task_id = TASK_LINECOUNT;
    else if (strcmp(task_str, "sumnumbers") == 0) g_task_id = TASK_SUMNUMBERS;
    else if (strcmp(task_str, "charcount")  == 0) g_task_id = TASK_CHARCOUNT;
    else {
        fprintf(stderr, "Unknown task '%s'\n", task_str);
        return EXIT_FAILURE;
    }

    const task_descriptor *t = get_task(g_task_id);
    g_accumulator = t->identity;

    /* ── Signal handling ── */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* ── Split the input file ── */
    g_num_chunks = split_file_into_chunks(filename, chunk_bytes);
    if (g_num_chunks < 0) return EXIT_FAILURE;
    fprintf(stdout,
            "[COORD] Loaded '%s' → %d chunk(s) of up to %u bytes  [task=%s]\n",
            filename, g_num_chunks, chunk_bytes, task_name(g_task_id));

    /* ── Create the UDP socket ── */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock == -1) { perror("socket"); return EXIT_FAILURE; }

    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)atoi(port_str));

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind"); close(sock); return EXIT_FAILURE;
    }

    /* Non-blocking — required for the epoll edge-triggered drain loop */
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    fprintf(stdout, "[COORD] Listening for workers on UDP port %s\n", port_str);

    /* ── epoll setup ── */
    int epfd = epoll_create1(0);
    if (epfd == -1) { perror("epoll_create1"); close(sock); return EXIT_FAILURE; }

    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = sock;
    epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);

    struct epoll_event events[8];

    /* ── Main event loop ── */
    while (g_running && g_chunks_done < g_num_chunks) {

        int nready = epoll_wait(epfd, events, 8, EPOLL_TICK_MS);

        if (nready == -1) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        if (nready == 0) {
            /* Tick with no I/O — this is our fault-tolerance heartbeat */
            scan_for_timeouts();
            redistribute_pending(sock);
            continue;
        }

        /* Drain ALL pending datagrams (edge-triggered requirement) */
        for (;;) {
            char buf[MAX_DATAGRAM];
            struct sockaddr_in peer;
            socklen_t peer_len = sizeof(peer);

            ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                                 (struct sockaddr *)&peer, &peer_len);
            if (n == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                perror("recvfrom");
                break;
            }
            if ((size_t)n < sizeof(msg_header)) continue;   /* too short */

            msg_header hdr;
            memcpy(&hdr, buf, sizeof(hdr));
            header_to_host(&hdr);

            const char *body     = buf + sizeof(hdr);
            size_t      body_len = (size_t)n - sizeof(hdr);

            switch (hdr.msg_type) {
            case MSG_REGISTER:
                handle_register(sock, &peer);
                break;
            case MSG_RESULT:
                handle_result(sock, &peer, &hdr, body, body_len);
                break;
            case MSG_LEAVE:
                handle_leave(&peer);
                break;
            case MSG_HEARTBEAT:
                handle_heartbeat(&peer);
                break;
            default:
                fprintf(stderr, "[COORD] Unknown msg_type 0x%02x\n",
                        hdr.msg_type);
                break;
            }
        }

        /* Also run the timeout scan opportunistically after I/O bursts */
        scan_for_timeouts();
        redistribute_pending(sock);
    }

    /* ── Job complete: notify any still-connected workers ── */
    fprintf(stdout, "\n[COORD] All %d chunks processed — finishing up.\n",
            g_chunks_done);

    for (int wi = 0; wi < MAX_WORKERS; wi++) {
        if (!g_workers[wi].in_use) continue;
        msg_header hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.msg_type = MSG_NO_MORE_WORK;
        hdr.seq_num  = g_seq_counter++;
        hdr.chunk_id = CHUNK_ID_NONE;
        header_to_network(&hdr);
        sendto(sock, &hdr, sizeof(hdr), 0,
               (struct sockaddr *)&g_workers[wi].addr,
               sizeof(g_workers[wi].addr));
    }

    print_worker_stats();

    fprintf(stdout, "[COORD] ============================================\n");
    fprintf(stdout, "[COORD] FINAL RESULT (%s) = %lld\n",
            task_name(g_task_id), (long long)g_accumulator);
    fprintf(stdout, "[COORD] ============================================\n");

    close(sock);
    close(epfd);
    return EXIT_SUCCESS;
}
