/*
 * coordinator.c - Distributed Data Processing System: Coordinator (Master)
 *
 * Responsibilities:
 *   1. Listen for incoming TCP connections from workers.
 *   2. Accept dynamic worker registration; assign each a unique ID.
 *   3. Split the input file into equal-sized chunks.
 *   4. Distribute chunks to registered workers via MSG_TASK_ASSIGN.
 *   5. Monitor heartbeats; detect dead workers and reassign their chunks.
 *   6. Collect results; aggregate and print the final answer.
 *   7. Use epoll for non-blocking I/O so many workers can be handled.
 *
 * Usage:
 *   ./coordinator <filename> <task_type> [chunk_size_bytes]
 *     task_type : 1=word_count  2=sum_numbers  3=line_count
 *     chunk_size_bytes defaults to 4096 if omitted.
 *
 * Protocol: TCP with binary framing (see protocol.h).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "net_utils.h"

/* ─── Constants ─────────────────────────────────────────────────────────── */
#define MAX_EVENTS      128          /* Max epoll events per iteration       */
#define DEFAULT_CHUNK   4096         /* Default chunk size in bytes          */

/* ─── Chunk state machine ───────────────────────────────────────────────── */
typedef enum {
    CHUNK_PENDING   = 0,   /* Not yet assigned to any worker               */
    CHUNK_ASSIGNED  = 1,   /* Sent to a worker; awaiting result            */
    CHUNK_DONE      = 2,   /* Result received and acknowledged             */
} ChunkState;

typedef struct {
    int        chunk_id;
    ChunkState state;
    char      *data;        /* Pointer into the mmap'd / malloc'd buffer   */
    size_t     len;
    int        assigned_to; /* worker_id currently holding this chunk      */
    time_t     assigned_at; /* Unix timestamp when last assigned           */
    int        retries;     /* How many times we've reassigned this chunk  */
} Chunk;

/* ─── Worker state ──────────────────────────────────────────────────────── */
typedef enum {
    WS_REGISTERED = 0,
    WS_BUSY       = 1,
    WS_IDLE       = 2,
    WS_DEAD       = 3,
} WorkerState;

typedef struct {
    int         fd;             /* TCP socket fd for this worker           */
    uint16_t    id;             /* Assigned worker ID                      */
    WorkerState state;
    time_t      last_heartbeat; /* Last time we heard from this worker     */
    int         current_chunk;  /* chunk_id currently assigned (-1 = none) */
    char        hostname[64];
} Worker;

/* ─── Globals ───────────────────────────────────────────────────────────── */
static Worker  workers[MAX_WORKERS];
static int     worker_count  = 0;
static uint16_t next_worker_id = 1;

static Chunk  *chunks        = NULL;
static int     total_chunks  = 0;
static int     chunks_done   = 0;

static int64_t *results      = NULL;  /* One result value per chunk        */
static int      epoll_fd     = -1;
static int      listen_fd    = -1;
static volatile int running  = 1;

/* ─── Signal handler ────────────────────────────────────────────────────── */
static void handle_sigint(int sig)
{
    (void)sig;
    running = 0;
}

/* ─── Worker lookup helpers ─────────────────────────────────────────────── */
/* Find worker by socket fd */
static Worker *find_worker_by_fd(int fd)
{
    for (int i = 0; i < worker_count; i++)
        if (workers[i].fd == fd && workers[i].state != WS_DEAD)
            return &workers[i];
    return NULL;
}

/* Find any idle (registered but not busy) worker */
static Worker *find_idle_worker(void)
{
    for (int i = 0; i < worker_count; i++)
        if (workers[i].state == WS_IDLE)
            return &workers[i];
    return NULL;
}

/* Mark a worker as dead and free its slot */
static void mark_worker_dead(Worker *w)
{
    LOG("Worker %d (%s) marked DEAD", w->id, w->hostname);

    /* If it held a chunk, put that chunk back to PENDING so it gets reassigned */
    if (w->current_chunk >= 0 && w->current_chunk < total_chunks) {
        Chunk *c = &chunks[w->current_chunk];
        if (c->state == CHUNK_ASSIGNED) {
            LOG("  → Reassigning chunk %d (was held by worker %d)",
                c->chunk_id, w->id);
            c->state       = CHUNK_PENDING;
            c->assigned_to = -1;
            c->retries++;
        }
    }

    /* Remove from epoll and close socket */
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, w->fd, NULL);
    close(w->fd);
    w->fd           = -1;
    w->state        = WS_DEAD;
    w->current_chunk = -1;
}

/* ─── Chunking ──────────────────────────────────────────────────────────── */
/*
 * load_and_split_file()
 *   Reads the entire file into memory, then creates Chunk descriptors
 *   each pointing to a `chunk_size`-byte slice of the buffer.
 *   The last chunk may be smaller.
 *   Returns 0 on success, -1 on error.
 */
static char *file_buffer = NULL;
static size_t file_size  = 0;

static int load_and_split_file(const char *filename, size_t chunk_size)
{
    FILE *fp = fopen(filename, "rb");
    if (!fp) { perror("fopen"); return -1; }

    /* Determine file size */
    fseek(fp, 0, SEEK_END);
    file_size = (size_t)ftell(fp);
    rewind(fp);

    if (file_size == 0) {
        fprintf(stderr, "Error: input file is empty.\n");
        fclose(fp); return -1;
    }

    /* Read entire file */
    file_buffer = malloc(file_size + 1);
    if (!file_buffer) { perror("malloc"); fclose(fp); return -1; }
    if (fread(file_buffer, 1, file_size, fp) != file_size) {
        perror("fread"); fclose(fp); free(file_buffer); return -1;
    }
    file_buffer[file_size] = '\0';
    fclose(fp);

    /* Calculate number of chunks */
    total_chunks = (int)((file_size + chunk_size - 1) / chunk_size);
    chunks  = calloc(total_chunks, sizeof(Chunk));
    results = calloc(total_chunks, sizeof(int64_t));
    if (!chunks || !results) { perror("calloc"); return -1; }

    /*
     * Create chunk descriptors with whitespace-aligned boundaries.
     * Each boundary is extended forward until a whitespace character so
     * that words and numbers are never split between two chunks.
     */
    size_t offset = 0;
    int    ci     = 0;
    /* Re-allocate with a generous upper bound; actual count may be <= total_chunks */
    free(chunks);
    chunks  = calloc((size_t)(total_chunks + 2), sizeof(Chunk));
    free(results);
    results = calloc((size_t)(total_chunks + 2), sizeof(int64_t));
    if (!chunks || !results) { perror("calloc"); return -1; }

    while (offset < file_size) {
        size_t end = offset + chunk_size;
        if (end >= file_size) {
            end = file_size;          /* Last chunk: take everything remaining */
        } else {
            /* Advance 'end' forward until we land on a whitespace boundary */
            while (end < file_size && file_buffer[end] != ' '
                   && file_buffer[end] != '\n' && file_buffer[end] != '\t'
                   && file_buffer[end] != '\r') {
                end++;
            }
            /* Include the whitespace character itself in this chunk */
            if (end < file_size) end++;
        }

        chunks[ci].chunk_id    = ci;
        chunks[ci].state       = CHUNK_PENDING;
        chunks[ci].data        = file_buffer + offset;
        chunks[ci].len         = end - offset;
        chunks[ci].assigned_to = -1;
        chunks[ci].assigned_at = 0;
        chunks[ci].retries     = 0;
        ci++;
        offset = end;
    }
    total_chunks = ci;

    LOG("File '%s' (%zu bytes) split into %d whitespace-aligned chunks (~%zu bytes each)",
        filename, file_size, total_chunks, chunk_size);
    return 0;
}

/* ─── Task dispatch ─────────────────────────────────────────────────────── */
/*
 * assign_chunk_to_worker()
 *   Sends a MSG_TASK_ASSIGN to the worker, embedding:
 *     - TaskAssignPayload struct
 *     - raw chunk bytes immediately after the struct
 */
static int assign_chunk_to_worker(Worker *w, Chunk *c, TaskType task)
{
    /* Build a combined buffer: TaskAssignPayload + chunk data */
    size_t pay_total = sizeof(TaskAssignPayload) + c->len;
    char  *pay_buf   = malloc(pay_total);
    if (!pay_buf) { perror("malloc"); return -1; }

    TaskAssignPayload *tap = (TaskAssignPayload *)pay_buf;
    tap->chunk_id  = htons((uint16_t)c->chunk_id);
    tap->chunk_len = htonl((uint32_t)c->len);
    tap->task_type = (uint8_t)task;
    memcpy(pay_buf + sizeof(TaskAssignPayload), c->data, c->len);

    int rc = send_msg(w->fd, MSG_TASK_ASSIGN, task, 0,
                      w->id, (uint16_t)c->chunk_id,
                      pay_buf, (uint32_t)pay_total);
    free(pay_buf);

    if (rc < 0) {
        LOG("Failed to send chunk %d to worker %d", c->chunk_id, w->id);
        return -1;
    }

    /* Update state */
    c->state       = CHUNK_ASSIGNED;
    c->assigned_to = w->id;
    c->assigned_at = time(NULL);
    w->state        = WS_BUSY;
    w->current_chunk = c->chunk_id;

    LOG("Assigned chunk %d (%zu bytes) → worker %d", c->chunk_id, c->len, w->id);
    return 0;
}

/* Try to dispatch pending chunks to any idle worker */
static void dispatch_pending_chunks(TaskType task)
{
    for (int ci = 0; ci < total_chunks; ci++) {
        if (chunks[ci].state != CHUNK_PENDING) continue;

        Worker *w = find_idle_worker();
        if (!w) break;  /* No idle workers right now */

        if (assign_chunk_to_worker(w, &chunks[ci], task) < 0)
            mark_worker_dead(w);
    }
}

/* ─── Heartbeat / timeout check ─────────────────────────────────────────── */
static void check_worker_timeouts(TaskType task)
{
    int did_kill = 0;
    time_t now = time(NULL);
    for (int i = 0; i < worker_count; i++) {
        Worker *w = &workers[i];
        if (w->state == WS_DEAD) continue;
        if (now - w->last_heartbeat > WORKER_TIMEOUT) {
            LOG("Worker %d timed out (last HB %lds ago)",
                w->id, (long)(now - w->last_heartbeat));
            mark_worker_dead(w);
            did_kill = 1;
        }
    }
    /* Immediately dispatch any chunks freed by dead workers */
    if (did_kill) dispatch_pending_chunks(task);
}

/* ─── Message handlers (one per message type received) ──────────────────── */

/* Handle MSG_REGISTER: assign ID, send ACK, set worker IDLE */
static void handle_register(int fd, PktHeader *hdr, void *payload)
{
    if (worker_count >= MAX_WORKERS) {
        LOG("Too many workers – rejecting fd %d", fd);
        close(fd);
        return;
    }

    RegisterPayload *rp = (RegisterPayload *)payload;

    Worker *w = &workers[worker_count++];
    memset(w, 0, sizeof(*w));
    w->fd             = fd;
    w->id             = next_worker_id++;
    w->state          = WS_IDLE;
    w->last_heartbeat = time(NULL);
    w->current_chunk  = -1;
    /* Copy hostname safely: null-terminate then memcpy to avoid truncation warning */
    rp->hostname[sizeof(rp->hostname) - 1] = '\0';
    memcpy(w->hostname, rp->hostname, sizeof(w->hostname));

    LOG("Worker registered: id=%d hostname=%s", w->id, w->hostname);

    /* Send ACK with assigned ID */
    RegisterAckPayload ack;
    ack.assigned_id = htons(w->id);
    send_msg(fd, MSG_REGISTER_ACK, 0, hdr->seq_num,
             w->id, 0, &ack, sizeof(ack));
}

/* Handle MSG_RESULT: store result, mark chunk done, send ACK */
static void handle_result(Worker *w, PktHeader *hdr, void *payload)
{
    ResultPayload *rp = (ResultPayload *)payload;
    int cid           = ntohs(rp->chunk_id);

    if (cid < 0 || cid >= total_chunks) {
        LOG("Worker %d sent result for invalid chunk %d", w->id, cid);
        return;
    }

    /* Only accept if we were expecting it */
    if (chunks[cid].state != CHUNK_ASSIGNED) {
        LOG("Duplicate/late result for chunk %d from worker %d (ignored)",
            cid, w->id);
    } else {
        int64_t val   = (int64_t)be64toh((uint64_t)rp->result_value);
        results[cid]  = val;
        chunks[cid].state = CHUNK_DONE;
        chunks_done++;
        LOG("Result chunk %d = %lld  (%d/%d done)",
            cid, (long long)val, chunks_done, total_chunks);
    }

    /* Acknowledge receipt */
    send_msg(w->fd, MSG_RESULT_ACK, 0, hdr->seq_num,
             w->id, (uint16_t)cid, NULL, 0);

    /* Worker is now free */
    w->state         = WS_IDLE;
    w->current_chunk = -1;
}

/* Handle MSG_HEARTBEAT: reset timeout timer, send ACK */
static void handle_heartbeat(Worker *w, PktHeader *hdr)
{
    w->last_heartbeat = time(NULL);
    send_msg(w->fd, MSG_HEARTBEAT_ACK, 0, hdr->seq_num,
             w->id, 0, NULL, 0);
}

/* Handle MSG_WORKER_LEAVE: clean shutdown from worker side */
/*
 * Note: dispatch_pending_chunks() is called by the caller after this returns
 * so any chunk freed here will be immediately redistributed.
 */
static void handle_worker_leave(Worker *w)
{
    LOG("Worker %d (%s) sent LEAVE message", w->id, w->hostname);
    /* If it was processing a chunk, put the chunk back to pending */
    if (w->current_chunk >= 0) {
        int cid = w->current_chunk;
        if (chunks[cid].state == CHUNK_ASSIGNED) {
            LOG("  → Returning chunk %d to PENDING queue", cid);
            chunks[cid].state       = CHUNK_PENDING;
            chunks[cid].assigned_to = -1;
        }
    }
    mark_worker_dead(w);
}

/* ─── Aggregation & output ──────────────────────────────────────────────── */
static void print_final_result(TaskType task)
{
    int64_t total = 0;
    for (int i = 0; i < total_chunks; i++)
        total += results[i];

    printf("\n============================\n");
    switch (task) {
        case TASK_WORD_COUNT:
            printf("  TOTAL WORD COUNT : %lld\n", (long long)total);
            break;
        case TASK_SUM_NUMBERS:
            printf("  TOTAL SUM        : %lld\n", (long long)total);
            break;
        case TASK_LINE_COUNT:
            printf("  TOTAL LINE COUNT : %lld\n", (long long)total);
            break;
        default:
            printf("  TOTAL RESULT     : %lld\n", (long long)total);
    }
    printf("============================\n\n");
}

/* Send MSG_SHUTDOWN to all live workers */
static void shutdown_workers(void)
{
    for (int i = 0; i < worker_count; i++) {
        if (workers[i].state != WS_DEAD) {
            send_msg(workers[i].fd, MSG_SHUTDOWN, 0, 0,
                     workers[i].id, 0, NULL, 0);
        }
    }
}

/* ─── Main event loop ───────────────────────────────────────────────────── */
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr,
            "Usage: %s <filename> <task_type> [chunk_size]\n"
            "  task_type: 1=word_count  2=sum_numbers  3=line_count\n",
            argv[0]);
        return 1;
    }

    const char *filename   = argv[1];
    TaskType    task       = (TaskType)atoi(argv[2]);
    size_t      chunk_size = (argc >= 4) ? (size_t)atoi(argv[3]) : DEFAULT_CHUNK;

    if (task < TASK_WORD_COUNT || task > TASK_LINE_COUNT) {
        fprintf(stderr, "Invalid task type %d\n", (int)task);
        return 1;
    }

    signal(SIGINT,  handle_sigint);
    signal(SIGPIPE, SIG_IGN);   /* Don't crash on broken-pipe writes */

    /* ── Load & split the file ── */
    if (load_and_split_file(filename, chunk_size) < 0) return 1;

    /* ── Create listening socket ── */
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(COORD_PORT);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(listen_fd, 16) < 0) { perror("listen"); return 1; }
    set_nonblocking(listen_fd);
    LOG("Coordinator listening on port %d", COORD_PORT);

    /* ── Create epoll instance ── */
    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) { perror("epoll_create1"); return 1; }

    /* Register the listen socket as level-triggered so we never miss a connect */
    struct epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

    struct epoll_event events[MAX_EVENTS];

    /* ── Main loop ── */
    while (running) {

        /* epoll_wait with 1-second timeout so we can do periodic checks */
        int nev = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);

        if (nev < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait"); break;
        }

        /* ── Handle I/O events ── */
        for (int i = 0; i < nev; i++) {
            int fd = events[i].data.fd;

            /* ---- New connection(s) on listening socket ---- */
            if (fd == listen_fd) {
                /* Loop to drain all pending connections (level-triggered) */
                while (1) {
                    struct sockaddr_in caddr;
                    socklen_t clen = sizeof(caddr);
                    int cfd = accept(listen_fd,
                                     (struct sockaddr *)&caddr, &clen);
                    if (cfd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;   /* No more pending connections */
                        perror("accept");
                        break;
                    }
                    /*
                     * Keep worker fds in blocking mode with a generous timeout.
                     * Only the listen_fd needs O_NONBLOCK.  epoll works fine
                     * on blocking fds (it tells us when data IS ready, after
                     * which the blocking recv will return immediately).
                     */
                    set_socket_timeout(cfd, 10);

                    struct epoll_event cev;
                    cev.events  = EPOLLIN;
                    cev.data.fd = cfd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cfd, &cev);
                    LOG("New connection from %s (fd=%d)",
                        inet_ntoa(caddr.sin_addr), cfd);
                }
                continue;
            }

            /* ---- Data from a worker ---- */
            if (events[i].events & (EPOLLHUP | EPOLLERR)) {
                /* Socket error or hang-up – treat as dead worker */
                Worker *w = find_worker_by_fd(fd);
                if (w) {
                    mark_worker_dead(w);
                    dispatch_pending_chunks(task);   /* redistribute freed chunk */
                } else {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                }
                continue;
            }

            if (events[i].events & EPOLLIN) {
                PktHeader hdr;
                char      pay_buf[MAX_CHUNK_SIZE + sizeof(TaskAssignPayload) + 64];

                /* Read one complete message (header + payload) */
                if (recv_msg(fd, &hdr, pay_buf, sizeof(pay_buf)) < 0) {
                    Worker *w = find_worker_by_fd(fd);
                    if (w) mark_worker_dead(w);
                    else { epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL); close(fd); }
                    continue;
                }

                Worker *w = find_worker_by_fd(fd);

                switch (hdr.msg_type) {
                    case MSG_REGISTER:
                        handle_register(fd, &hdr, pay_buf);
                        break;
                    case MSG_TASK_ACK:
                        /* Worker acknowledged chunk receipt – no action needed */
                        break;
                    case MSG_RESULT:
                        if (w) handle_result(w, &hdr, pay_buf);
                        break;
                    case MSG_HEARTBEAT:
                        if (w) handle_heartbeat(w, &hdr);
                        break;
                    case MSG_WORKER_LEAVE:
                        if (w) {
                            handle_worker_leave(w);
                            dispatch_pending_chunks(task);
                        }
                        break;
                    default:
                        LOG("Unknown message type %d from fd %d",
                            hdr.msg_type, fd);
                }
            }
        } /* end event loop */

        /* ── Periodic: check heartbeat timeouts ── */
        check_worker_timeouts(task);

        /* ── Periodic: dispatch pending chunks to idle workers ── */
        dispatch_pending_chunks(task);

        /* ── Check if all chunks are done ── */
        if (chunks_done == total_chunks && total_chunks > 0) {
            LOG("All %d chunks processed!", total_chunks);
            print_final_result(task);
            shutdown_workers();
            break;
        }

        /* ── Print progress every ~10 seconds (use a counter) ── */
        static int tick = 0;
        if (++tick % 10 == 0)
            LOG("Progress: %d/%d chunks done, %d workers alive",
                chunks_done, total_chunks, worker_count);

    } /* end while(running) */

    /* ── Cleanup ── */
    close(epoll_fd);
    close(listen_fd);
    free(file_buffer);
    free(chunks);
    free(results);
    LOG("Coordinator exiting.");
    return 0;
}

