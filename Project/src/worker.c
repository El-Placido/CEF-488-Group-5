/*
 * worker.c - Distributed Data Processing System: Worker Node
 *
 * Responsibilities:
 *   1. Connect to the coordinator and register (send hostname).
 *   2. Receive a chunk of data via MSG_TASK_ASSIGN.
 *   3. Process the chunk according to the requested task type:
 *        TASK_WORD_COUNT  – count whitespace-delimited words
 *        TASK_SUM_NUMBERS – sum all integers (positive & negative)
 *        TASK_LINE_COUNT  – count '\n' characters
 *   4. Send the result back via MSG_RESULT.
 *   5. Send periodic MSG_HEARTBEAT while idle/processing.
 *   6. Cleanly exit on MSG_SHUTDOWN or SIGINT.
 *
 * Usage:
 *   ./worker <coordinator_ip> [port]
 *
 * A single worker can handle exactly ONE chunk at a time.
 * Run multiple worker processes (on the same or different machines)
 * to achieve parallel processing.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "protocol.h"
#include "net_utils.h"

/* ─── Globals ───────────────────────────────────────────────────────────── */
static volatile int running     = 1;
static uint16_t     my_id       = 0;
static int          coord_fd    = -1;
static uint16_t     seq_counter = 0;  /* Incremented per outgoing message   */

/* ─── Signal handler ────────────────────────────────────────────────────── */
static void handle_sigint(int sig)
{
    (void)sig;
    running = 0;
}

/* ─── Processing functions ──────────────────────────────────────────────── */

/*
 * task_word_count()
 *   Counts whitespace-delimited tokens.  Whitespace is any char for which
 *   isspace() returns true (space, tab, newline, carriage return, …).
 */
static int64_t task_word_count(const char *data, size_t len)
{
    int64_t count  = 0;
    int     in_word = 0;

    for (size_t i = 0; i < len; i++) {
        if (isspace((unsigned char)data[i])) {
            in_word = 0;
        } else {
            if (!in_word) {
                count++;
                in_word = 1;
            }
        }
    }
    return count;
}

/*
 * task_sum_numbers()
 *   Finds every decimal integer (optionally preceded by '-') in the data
 *   and sums them all.  Non-numeric characters are ignored.
 */
static int64_t task_sum_numbers(const char *data, size_t len)
{
    int64_t sum = 0;
    size_t  i   = 0;

    while (i < len) {
        /* Skip non-digit characters except '-' */
        if (!isdigit((unsigned char)data[i]) && data[i] != '-') {
            i++;
            continue;
        }
        /* Require '-' to be followed by a digit */
        if (data[i] == '-') {
            if (i + 1 >= len || !isdigit((unsigned char)data[i + 1])) {
                i++;
                continue;
            }
        }
        /* Parse the integer */
        char  *end;
        long long val = strtoll(data + i, &end, 10);
        if (end == data + i) { i++; continue; }  /* Couldn't parse */
        sum += (int64_t)val;
        i    = (size_t)(end - data);
    }
    return sum;
}

/*
 * task_line_count()
 *   Counts the number of newline characters ('\n') in the chunk.
 */
static int64_t task_line_count(const char *data, size_t len)
{
    int64_t count = 0;
    for (size_t i = 0; i < len; i++)
        if (data[i] == '\n') count++;
    return count;
}

/*
 * process_chunk()
 *   Dispatches to the appropriate processing function.
 */
static int64_t process_chunk(TaskType task, const char *data, size_t len)
{
    switch (task) {
        case TASK_WORD_COUNT:  return task_word_count(data, len);
        case TASK_SUM_NUMBERS: return task_sum_numbers(data, len);
        case TASK_LINE_COUNT:  return task_line_count(data, len);
        default:
            LOG("Unknown task type %d – returning 0", (int)task);
            return 0;
    }
}

/* ─── Registration ──────────────────────────────────────────────────────── */
/*
 * register_with_coordinator()
 *   Sends MSG_REGISTER with our hostname, waits for MSG_REGISTER_ACK,
 *   stores the assigned worker ID in my_id.
 *   Returns 0 on success, -1 on failure.
 */
static int register_with_coordinator(void)
{
    RegisterPayload rp;
    memset(&rp, 0, sizeof(rp));
    gethostname(rp.hostname, sizeof(rp.hostname) - 1);

    LOG("Registering as '%s'…", rp.hostname);

    if (send_msg(coord_fd, MSG_REGISTER, 0, seq_counter++,
                 0, 0, &rp, sizeof(rp)) < 0) {
        fprintf(stderr, "register: send failed\n");
        return -1;
    }

    /* Wait for ACK (with timeout already set on socket) */
    PktHeader hdr;
    char      buf[sizeof(RegisterAckPayload) + 16];
    if (recv_msg(coord_fd, &hdr, buf, sizeof(buf)) < 0) {
        fprintf(stderr, "register: no ACK received\n");
        return -1;
    }

    if (hdr.msg_type != MSG_REGISTER_ACK) {
        fprintf(stderr, "register: unexpected msg type %d\n", hdr.msg_type);
        return -1;
    }

    RegisterAckPayload *ack = (RegisterAckPayload *)buf;
    my_id = ntohs(ack->assigned_id);
    LOG("Registered successfully – worker id = %d", my_id);
    return 0;
}

/* ─── Result sender ─────────────────────────────────────────────────────── */
/*
 * send_result_with_retry()
 *   Sends MSG_RESULT and waits for MSG_RESULT_ACK.
 *   Retries up to MAX_RETRIES times on failure.
 *   Returns 0 on ACK received, -1 on exhausted retries.
 */
static int send_result_with_retry(uint16_t chunk_id, int64_t value)
{
    ResultPayload rp;
    rp.chunk_id    = htons(chunk_id);
    /* Use big-endian for int64 portability */
    rp.result_value = (int64_t)htobe64((uint64_t)value);

    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
        if (attempt > 0)
            LOG("  Retrying result send (attempt %d/%d)…", attempt+1, MAX_RETRIES);

        if (send_msg(coord_fd, MSG_RESULT, 0, seq_counter++,
                     my_id, chunk_id, &rp, sizeof(rp)) < 0) {
            LOG("  send_msg failed on attempt %d", attempt+1);
            continue;
        }

        /* Wait for ACK */
        set_socket_timeout(coord_fd, 3);
        PktHeader ack_hdr;
        char      ack_buf[64];
        if (recv_msg(coord_fd, &ack_hdr, ack_buf, sizeof(ack_buf)) < 0) {
            LOG("  No ACK for result (attempt %d)", attempt+1);
            continue;
        }
        if (ack_hdr.msg_type == MSG_RESULT_ACK) {
            LOG("  Result ACK received for chunk %d", chunk_id);
            return 0;  /* Success */
        }
    }

    LOG("ERROR: Could not deliver result for chunk %d after %d attempts",
        chunk_id, MAX_RETRIES);
    return -1;
}

/* ─── Heartbeat thread-like logic ────────────────────────────────────────── */
/*
 * We don't use threads; instead heartbeats are sent at the top of the
 * main loop whenever enough time has elapsed.
 */
static time_t last_heartbeat_sent = 0;

static void maybe_send_heartbeat(void)
{
    time_t now = time(NULL);
    if (now - last_heartbeat_sent >= HEARTBEAT_SECS) {
        send_msg(coord_fd, MSG_HEARTBEAT, 0, seq_counter++,
                 my_id, 0, NULL, 0);

        /* Drain any pending ACK (non-blocking peek) */
        set_socket_timeout(coord_fd, 1);
        PktHeader hdr;
        char      buf[64];
        recv_msg(coord_fd, &hdr, buf, sizeof(buf));
        /* Ignore result – we'll just reset the timer */

        last_heartbeat_sent = now;
    }
}

/* ─── Main ──────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <coordinator_ip> [port]\n", argv[0]);
        return 1;
    }

    const char *coord_ip  = argv[1];
    int         coord_port = (argc >= 3) ? atoi(argv[2]) : COORD_PORT;

    signal(SIGINT,  handle_sigint);
    signal(SIGPIPE, SIG_IGN);

    /* ── Connect to coordinator ── */
    coord_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (coord_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in coord_addr;
    memset(&coord_addr, 0, sizeof(coord_addr));
    coord_addr.sin_family      = AF_INET;
    coord_addr.sin_port        = htons((uint16_t)coord_port);
    if (inet_pton(AF_INET, coord_ip, &coord_addr.sin_addr) <= 0) {
        /* Try hostname resolution */
        struct hostent *he = gethostbyname(coord_ip);
        if (!he) {
            fprintf(stderr, "Cannot resolve coordinator address '%s'\n", coord_ip);
            return 1;
        }
        memcpy(&coord_addr.sin_addr, he->h_addr, (size_t)he->h_length);
    }

    LOG("Connecting to coordinator at %s:%d…", coord_ip, coord_port);
    if (connect(coord_fd, (struct sockaddr *)&coord_addr,
                sizeof(coord_addr)) < 0) {
        perror("connect"); return 1;
    }
    LOG("Connected.");

    set_socket_timeout(coord_fd, 10);

    /* ── Register ── */
    if (register_with_coordinator() < 0) {
        close(coord_fd); return 1;
    }

    last_heartbeat_sent = time(NULL);

    /* ── Main work loop ── */
    while (running) {

        maybe_send_heartbeat();

        /* Wait for a message from coordinator (5-second timeout) */
        set_socket_timeout(coord_fd, 5);
        PktHeader hdr;
        /* Large buffer: header + up to MAX_CHUNK_SIZE bytes of chunk data */
        char *msg_buf = malloc(sizeof(TaskAssignPayload) + MAX_CHUNK_SIZE + 64);
        if (!msg_buf) { perror("malloc"); break; }

        int rc = recv_msg(coord_fd, &hdr, msg_buf,
                          sizeof(TaskAssignPayload) + MAX_CHUNK_SIZE + 64);
        if (rc < 0) {
            free(msg_buf);
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
                /* Timeout – send heartbeat and try again */
                continue;
            }
            /* Connection lost */
            LOG("Connection to coordinator lost.");
            break;
        }

        /* ── Dispatch on message type ── */
        switch (hdr.msg_type) {

        case MSG_TASK_ASSIGN: {
            /*
             * Layout of msg_buf:
             *   [TaskAssignPayload][raw chunk bytes…]
             */
            TaskAssignPayload *tap = (TaskAssignPayload *)msg_buf;
            uint16_t  cid         = ntohs(tap->chunk_id);
            uint32_t  clen        = ntohl(tap->chunk_len);
            TaskType  task        = (TaskType)tap->task_type;
            char     *chunk_data  = msg_buf + sizeof(TaskAssignPayload);

            LOG("Received chunk %d (%u bytes), task=%d", cid, clen, (int)task);

            /* Send task ACK immediately */
            send_msg(coord_fd, MSG_TASK_ACK, task, hdr.seq_num,
                     my_id, cid, NULL, 0);

            /* ── PROCESS ── */
            int64_t result = process_chunk(task, chunk_data, (size_t)clen);
            LOG("Chunk %d processed → result = %lld", cid, (long long)result);

            /* ── SEND RESULT ── */
            if (send_result_with_retry(cid, result) < 0) {
                LOG("Failed to deliver result for chunk %d", cid);
                /* Keep running; coordinator will time out and reassign */
            }
            break;
        }

        case MSG_HEARTBEAT_ACK:
            /* Coordinator acknowledged our heartbeat – nothing to do */
            break;

        case MSG_SHUTDOWN:
            LOG("Received SHUTDOWN from coordinator. Exiting.");
            running = 0;
            break;

        default:
            LOG("Unexpected message type %d", hdr.msg_type);
            break;
        }

        free(msg_buf);
    } /* end while(running) */

    /* ── Clean leave ── */
    if (coord_fd >= 0) {
        send_msg(coord_fd, MSG_WORKER_LEAVE, 0, seq_counter++,
                 my_id, 0, NULL, 0);
        close(coord_fd);
    }

    LOG("Worker %d exiting.", my_id);
    return 0;
}

