/*
 * coordinator.c — Distributed Data Processing System
 *
 * Responsibilities:
 *   1. Listen for worker registrations on COORDINATOR_PORT (UDP).
 *   2. Split a text file into fixed-size chunks.
 *   3. Assign chunks to registered workers.
 *   4. Collect results and aggregate them.
 *   5. Detect dead workers via heartbeat timeouts and reassign their chunks.
 *   6. Use epoll for non-blocking I/O so many workers can be served at once.
 *
 * Additional requirements satisfied:
 *   - Dynamic worker registration  (workers join/leave at any time)
 *   - Fault tolerance              (missed heartbeats → chunk reassignment)
 *   - Non-blocking I/O + epoll     (single-threaded event loop)
 *   - Configurable task type       (passed as a command-line argument)
 */

#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <arpa/inet.h>
#include <endian.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "protocol.h"
#include "net_util.h"

/* ─── Chunk state machine ─────────────────────────────────────────────────── */
typedef enum {
    CHUNK_FREE      = 0,  /* not yet assigned                                 */
    CHUNK_ASSIGNED  = 1,  /* sent to a worker, awaiting result                */
    CHUNK_DONE      = 2   /* result received and stored                       */
} chunk_state_t;

typedef struct {
    uint32_t     id;                    /* chunk sequence number              */
    chunk_state_t state;
    uint16_t     assigned_worker;       /* worker ID currently processing     */
    time_t       assign_time;           /* when it was assigned (for timeout) */
    char         data[MAX_CHUNK_SIZE];  /* raw bytes of this chunk            */
    uint32_t     data_len;
    int64_t      result;                /* aggregated result value            */
} chunk_t;

/* ─── Worker registry ─────────────────────────────────────────────────────── */
typedef struct {
    int               active;           /* 1 if this slot is in use           */
    uint16_t          id;
    struct sockaddr_in addr;            /* address to send chunks/ACKs to     */
    time_t            last_heartbeat;   /* last time we heard from this worker*/
    uint32_t          current_chunk;    /* chunk ID assigned (0 = none)       */
} worker_t;

/* ─── Coordinator global state ───────────────────────────────────────────── */
#define MAX_CHUNKS 1024

static worker_t  workers[MAX_WORKERS];
static chunk_t   chunks[MAX_CHUNKS];
static int       total_chunks   = 0;
static int       done_chunks    = 0;
static int64_t   grand_total    = 0;
static task_type_t task_type    = TASK_WORD_COUNT;  /* default               */
static uint16_t  next_worker_id = 1;
static uint16_t  coord_seq      = 0;                /* outbound sequence num */

/* ─── Forward declarations ───────────────────────────────────────────────── */
static void handle_register   (int sock, const uint8_t *payload, uint32_t plen,
                                struct sockaddr_in *from);
static void handle_chunk_ack  (const uint8_t *payload, uint32_t plen,
                                uint16_t worker_id);
static void handle_result     (int sock, const uint8_t *payload, uint32_t plen,
                                uint16_t worker_id, struct sockaddr_in *from);
static void handle_heartbeat  (int sock, uint16_t worker_id,
                                struct sockaddr_in *from);
static void assign_pending_chunks(int sock);
static void check_worker_timeouts(int sock);
static void send_ack          (int sock, msg_type_t type, uint16_t worker_id,
                                uint16_t seq, const void *payload,
                                uint32_t plen, struct sockaddr_in *dest);
static int  load_file_chunks  (const char *filename);
static void print_final_result(void);

/* ═══════════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr,
            "Usage: %s <input_file> [task: 1=word_count 2=sum 3=line_count]\n",
            argv[0]);
        return 1;
    }

    /* Parse optional task type */
    if (argc >= 3) {
        int t = atoi(argv[2]);
        if (t >= 1 && t <= 3) task_type = (task_type_t)t;
        else {
            fprintf(stderr, "Invalid task type. Use 1, 2, or 3.\n");
            return 1;
        }
    }

    const char *task_names[] = { "", "WORD_COUNT", "SUM_NUMBERS", "LINE_COUNT" };
    printf("[coordinator] Starting. Task=%s  File=%s\n",
           task_names[task_type], argv[1]);

    /* Load and split the input file into chunks */
    if (load_file_chunks(argv[1]) < 0) {
        fprintf(stderr, "[coordinator] Failed to load file.\n");
        return 1;
    }
    printf("[coordinator] Loaded %d chunks from '%s'\n", total_chunks, argv[1]);

    /* Create and bind the UDP socket */
    int sock = create_udp_socket(COORDINATOR_PORT);
    if (sock < 0) return 1;

    /* Make the socket non-blocking for use with epoll */
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    printf("[coordinator] Listening on UDP port %d\n", COORDINATOR_PORT);

    /* ── epoll setup ─────────────────────────────────────────────────── */
    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); close(sock); return 1; }

    struct epoll_event ev;
    ev.events  = EPOLLIN;   /* watch for incoming datagrams                  */
    ev.data.fd = sock;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev) < 0) {
        perror("epoll_ctl"); close(sock); close(epfd); return 1;
    }

    printf("[coordinator] epoll event loop running (timeout=%ds)\n",
           HEARTBEAT_INTERVAL);

    /* ── Main event loop ──────────────────────────────────────────────── */
    uint8_t buf[sizeof(msg_header_t) + sizeof(payload_chunk_t) + 64];

    while (done_chunks < total_chunks) {

        /* Wait up to HEARTBEAT_INTERVAL seconds for an incoming datagram */
        struct epoll_event events[8];
        int nev = epoll_wait(epfd, events, 8, HEARTBEAT_INTERVAL * 1000);

        if (nev < 0) {
            if (errno == EINTR) continue;  /* interrupted by signal — retry  */
            perror("epoll_wait"); break;
        }

        /* Process every ready fd (we only have one, but be correct) */
        for (int i = 0; i < nev; i++) {
            if (!(events[i].events & EPOLLIN)) continue;

            struct sockaddr_in from;
            socklen_t from_len = sizeof(from);
            ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                                 (struct sockaddr *)&from, &from_len);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                perror("recvfrom"); continue;
            }

            msg_header_t hdr;
            const uint8_t *payload = parse_header(buf, (size_t)n, &hdr);
            if (!payload) {
                fprintf(stderr, "[coordinator] malformed packet, ignored\n");
                continue;
            }

            /* Dispatch to the appropriate handler */
            switch ((msg_type_t)hdr.type) {
            case MSG_REGISTER:
                handle_register(sock, payload, hdr.payload_len, &from);
                break;
            case MSG_CHUNK_ACK:
                handle_chunk_ack(payload, hdr.payload_len, hdr.worker_id);
                break;
            case MSG_RESULT:
                handle_result(sock, payload, hdr.payload_len,
                              hdr.worker_id, &from);
                break;
            case MSG_HEARTBEAT:
                handle_heartbeat(sock, hdr.worker_id, &from);
                break;
            default:
                fprintf(stderr, "[coordinator] unknown msg type %d\n", hdr.type);
            }
        }

        /* Periodic duties: assign free chunks, detect dead workers */
        assign_pending_chunks(sock);
        check_worker_timeouts(sock);
    }

    print_final_result();

    /* Broadcast shutdown to all active workers */
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (!workers[i].active) continue;
        uint8_t out[sizeof(msg_header_t)];
        int len = build_message(out, sizeof(out), MSG_SHUTDOWN,
                                coord_seq++, 0, NULL, 0);
        udp_send_simple(sock, out, len, &workers[i].addr);
        printf("[coordinator] sent SHUTDOWN to worker %d\n", workers[i].id);
    }

    close(sock);
    close(epfd);
    return 0;
}

/* ─── Handler: MSG_REGISTER ─────────────────────────────────────────────── */
static void handle_register(int sock, const uint8_t *payload, uint32_t plen,
                             struct sockaddr_in *from)
{
    if (plen < sizeof(payload_register_t)) {
        fprintf(stderr, "[coordinator] REGISTER payload too short\n");
        return;
    }
    const payload_register_t *reg = (const payload_register_t *)payload;
    uint16_t listen_port = ntohs(reg->listen_port);

    /* Find a free slot */
    int slot = -1;
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (!workers[i].active) { slot = i; break; }
    }
    if (slot < 0) {
        fprintf(stderr, "[coordinator] no worker slots available\n");
        return;
    }

    /* Fill in the worker record */
    workers[slot].active          = 1;
    workers[slot].id              = next_worker_id;
    workers[slot].addr            = *from;
    workers[slot].addr.sin_port   = htons(listen_port); /* use declared port */
    workers[slot].last_heartbeat  = now_sec();
    workers[slot].current_chunk   = 0;

    printf("[coordinator] Worker %d registered from %s:%d\n",
           next_worker_id, inet_ntoa(from->sin_addr), listen_port);

    /* Reply with the assigned ID and task type */
    payload_register_ack_t ack;
    ack.assigned_id = htons(next_worker_id);
    ack.task_type   = (uint8_t)task_type;

    send_ack(sock, MSG_REGISTER_ACK, next_worker_id, coord_seq++,
             &ack, sizeof(ack), &workers[slot].addr);

    next_worker_id++;
}

/* ─── Handler: MSG_CHUNK_ACK ─────────────────────────────────────────────── */
static void handle_chunk_ack(const uint8_t *payload, uint32_t plen,
                              uint16_t worker_id)
{
    if (plen < sizeof(payload_chunk_ack_t)) return;
    const payload_chunk_ack_t *ca = (const payload_chunk_ack_t *)payload;
    uint32_t chunk_id = ntohl(ca->chunk_id);

    if (chunk_id < (uint32_t)total_chunks) {
        printf("[coordinator] Worker %d ACKed chunk %u\n", worker_id, chunk_id);
    }
}

/* ─── Handler: MSG_RESULT ─────────────────────────────────────────────────── */
static void handle_result(int sock, const uint8_t *payload, uint32_t plen,
                           uint16_t worker_id, struct sockaddr_in *from)
{
    if (plen < sizeof(payload_result_t)) return;
    const payload_result_t *res = (const payload_result_t *)payload;

    uint32_t chunk_id = ntohl(res->chunk_id);
    int64_t  value    = (int64_t)be64toh((uint64_t)res->value);

    if (chunk_id >= (uint32_t)total_chunks) {
        fprintf(stderr, "[coordinator] result for unknown chunk %u\n", chunk_id);
        return;
    }
    if (chunks[chunk_id].state == CHUNK_DONE) {
        /* Duplicate result (retransmit from worker) — just re-ACK */
        goto send_result_ack;
    }

    chunks[chunk_id].state  = CHUNK_DONE;
    chunks[chunk_id].result = value;
    grand_total += value;
    done_chunks++;

    printf("[coordinator] Chunk %u result from worker %d: %lld  '%s'  "
           "(%d/%d done)\n",
           chunk_id, worker_id, (long long)value, res->detail,
           done_chunks, total_chunks);

    /* Free the worker slot for new chunks */
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].active && workers[i].id == worker_id) {
            workers[i].current_chunk = 0;
            break;
        }
    }

send_result_ack:;
    payload_result_ack_t rack;
    rack.chunk_id = htonl(chunk_id);
    send_ack(sock, MSG_RESULT_ACK, worker_id, coord_seq++,
             &rack, sizeof(rack), from);
}

/* ─── Handler: MSG_HEARTBEAT ─────────────────────────────────────────────── */
static void handle_heartbeat(int sock, uint16_t worker_id,
                              struct sockaddr_in *from)
{
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].active && workers[i].id == worker_id) {
            workers[i].last_heartbeat = now_sec();

            /* Reply so worker knows coordinator is alive */
            uint8_t out[sizeof(msg_header_t)];
            int len = build_message(out, sizeof(out), MSG_HEARTBEAT_ACK,
                                    coord_seq++, worker_id, NULL, 0);
            udp_send_simple(sock, out, len, from);
            return;
        }
    }
    /* Unknown worker sent heartbeat — ignore it */
}

/* ─── Assign any unassigned chunks to idle workers ───────────────────────── */
static void assign_pending_chunks(int sock)
{
    for (int c = 0; c < total_chunks; c++) {
        if (chunks[c].state != CHUNK_FREE) continue;

        /* Find an idle worker */
        int slot = -1;
        for (int w = 0; w < MAX_WORKERS; w++) {
            if (workers[w].active && workers[w].current_chunk == 0) {
                slot = w; break;
            }
        }
        if (slot < 0) return; /* no idle workers right now */

        /* Build and send MSG_CHUNK_ASSIGN */
        payload_chunk_t pc;
        pc.chunk_id = htonl((uint32_t)c);
        pc.data_len = htonl(chunks[c].data_len);
        memcpy(pc.data, chunks[c].data, chunks[c].data_len);

        uint8_t out[sizeof(msg_header_t) + sizeof(payload_chunk_t)];
        int len = build_message(out, sizeof(out), MSG_CHUNK_ASSIGN,
                                coord_seq++, workers[slot].id,
                                &pc, sizeof(pc));
        if (udp_send_simple(sock, out, len, &workers[slot].addr) > 0) {
            chunks[c].state           = CHUNK_ASSIGNED;
            chunks[c].assigned_worker = workers[slot].id;
            chunks[c].assign_time     = now_sec();
            workers[slot].current_chunk = (uint32_t)c + 1; /* +1 so 0 = none */
            printf("[coordinator] Assigned chunk %d to worker %d\n",
                   c, workers[slot].id);
        }
    }
}

/* ─── Fault tolerance: detect timed-out workers, reassign their chunks ───── */
static void check_worker_timeouts(int sock)
{
    time_t now = now_sec();
    for (int w = 0; w < MAX_WORKERS; w++) {
        if (!workers[w].active) continue;

        if ((now - workers[w].last_heartbeat) > WORKER_TIMEOUT) {
            printf("[coordinator] Worker %d timed out — removing.\n",
                   workers[w].id);

            /* Reassign any chunk this worker held */
            for (int c = 0; c < total_chunks; c++) {
                if (chunks[c].state == CHUNK_ASSIGNED &&
                    chunks[c].assigned_worker == workers[w].id) {
                    printf("[coordinator] Reassigning chunk %d (was with worker %d)\n",
                           c, workers[w].id);
                    chunks[c].state = CHUNK_FREE;
                    chunks[c].assigned_worker = 0;
                }
            }

            workers[w].active = 0;
            memset(&workers[w], 0, sizeof(workers[w]));
        }
    }
    (void)sock; /* sock reserved for future use (e.g., notifying peers) */
}

/* ─── Simple ACK/reply helper ────────────────────────────────────────────── */
static void send_ack(int sock, msg_type_t type, uint16_t worker_id,
                     uint16_t seq, const void *payload, uint32_t plen,
                     struct sockaddr_in *dest)
{
    uint8_t out[sizeof(msg_header_t) + 128];
    int len = build_message(out, sizeof(out), type, seq, worker_id,
                            payload, plen);
    if (len > 0) udp_send_simple(sock, out, len, dest);
}

/* ─── Split input file into MAX_CHUNK_SIZE byte chunks ─────────────────── */
static int load_file_chunks(const char *filename)
{
    FILE *fp = fopen(filename, "rb");
    if (!fp) { perror("fopen"); return -1; }

    total_chunks = 0;
    while (!feof(fp) && total_chunks < MAX_CHUNKS) {
        size_t n = fread(chunks[total_chunks].data, 1, MAX_CHUNK_SIZE, fp);
        if (n == 0) break;
        chunks[total_chunks].id       = (uint32_t)total_chunks;
        chunks[total_chunks].data_len = (uint32_t)n;
        chunks[total_chunks].state    = CHUNK_FREE;
        total_chunks++;
    }
    fclose(fp);
    return total_chunks > 0 ? 0 : -1;
}

/* ─── Print aggregated result at the end ────────────────────────────────── */
static void print_final_result(void)
{
    const char *task_names[] = { "", "Word Count", "Sum of Numbers", "Line Count" };
    printf("\n══════════════════════════════════════════\n");
    printf("  FINAL RESULT — %s\n", task_names[task_type]);
    printf("  Total chunks processed : %d\n", total_chunks);
    printf("  Aggregated value       : %lld\n", (long long)grand_total);
    printf("══════════════════════════════════════════\n\n");
}
