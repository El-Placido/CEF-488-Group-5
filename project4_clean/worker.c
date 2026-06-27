/*
 * worker.c
 * ========
 * Project 4: Distributed Data Processing System — Worker
 *
 * RESPONSIBILITIES
 * -----------------
 *   1. Register with the coordinator (MSG_REGISTER) — this is the
 *      DYNAMIC WORKER REGISTRATION feature: a worker can be started
 *      and join the job at any point, before or after other workers,
 *      before or after the coordinator has already started handing
 *      out chunks.
 *   2. Receive a chunk (MSG_TASK), apply the assigned task's MAP
 *      function from the shared task registry, and send back the
 *      result (MSG_RESULT).
 *   3. If no reply arrives from the coordinator within a timeout,
 *      retransmit the last message — this is the UDP-level reliability
 *      layer required because UDP guarantees nothing.
 *   4. Exit cleanly when the coordinator sends MSG_NO_MORE_WORK.
 *   5. Optionally LEAVE gracefully (Ctrl+C) — sends MSG_LEAVE so the
 *      coordinator immediately re-queues any in-flight chunk instead
 *      of waiting for the fault-tolerance timeout. This demonstrates
 *      "workers can leave during processing" without looking like a
 *      crash.
 *
 * USAGE
 * -----
 *   ./worker <coordinator-host> <coordinator-port>
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

#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "protocol.h"
#include "tasks.h"

/* ====================================================================
 * Configuration
 * ==================================================================== */
#define RECV_TIMEOUT_SEC   3    /* how long to wait before retransmitting */
#define MAX_RETRIES        5    /* give up after this many retries        */

static volatile sig_atomic_t g_leaving = 0;
static void sig_handler(int s) { (void)s; g_leaving = 1; }

/* ====================================================================
 * send_register() / send_leave() / send_result() / send_heartbeat()
 * ==================================================================== */
static void send_register(int sock, uint32_t seq)
{
    msg_header hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_type = MSG_REGISTER;
    hdr.seq_num  = seq;
    hdr.chunk_id = CHUNK_ID_NONE;
    header_to_network(&hdr);
    send(sock, &hdr, sizeof(hdr), 0);
}

static void send_leave(int sock, uint32_t seq)
{
    msg_header hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_type = MSG_LEAVE;
    hdr.seq_num  = seq;
    hdr.chunk_id = CHUNK_ID_NONE;
    header_to_network(&hdr);
    send(sock, &hdr, sizeof(hdr), 0);
}

static void send_result(int sock, uint32_t seq, uint32_t chunk_id,
                        uint8_t task_id, int64_t value)
{
    msg_header hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_type = MSG_RESULT;
    hdr.task_id  = task_id;
    hdr.seq_num  = seq;
    hdr.chunk_id = chunk_id;
    header_to_network(&hdr);

    result_payload rp;
    rp.result_value = (int64_t)hton64((uint64_t)value);

    char buf[sizeof(hdr) + sizeof(rp)];
    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), &rp, sizeof(rp));
    send(sock, buf, sizeof(buf), 0);
}

/* ====================================================================
 * recv_with_timeout()
 *
 * Wraps recv() with SO_RCVTIMEO already configured on the socket.
 * Returns the number of bytes received, 0 on timeout, -1 on hard error.
 * ==================================================================== */
static ssize_t recv_with_timeout(int sock, void *buf, size_t buflen)
{
    ssize_t n = recv(sock, buf, buflen, 0);
    if (n == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;  /* timeout */
        if (errno == EINTR) return 0;
        return -1;
    }
    return n;
}

/* ====================================================================
 * main
 * ==================================================================== */
int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <coordinator-host> <coordinator-port>\n",
                argv[0]);
        return EXIT_FAILURE;
    }
    const char *coord_host = argv[1];
    const char *coord_port = argv[2];

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* ── Resolve and "connect" the UDP socket ──
     * connect() on a UDP socket sets the default peer for send()/recv(),
     * letting us use send()/recv() instead of sendto()/recvfrom(), and
     * filters out any stray datagrams from other senders. */
    struct addrinfo hints, *res, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;

    int err = getaddrinfo(coord_host, coord_port, &hints, &res);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return EXIT_FAILURE;
    }

    int sock = -1;
    for (rp = res; rp != NULL; rp = rp->ai_next) {
        sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock == -1) continue;
        if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(sock); sock = -1;
    }
    freeaddrinfo(res);

    if (sock == -1) {
        fprintf(stderr, "[WORKER] Cannot reach coordinator %s:%s\n",
                coord_host, coord_port);
        return EXIT_FAILURE;
    }

    /* Set a receive timeout so recv() doesn't block forever, allowing
     * us to detect a lost packet and retransmit. */
    struct timeval tv = { RECV_TIMEOUT_SEC, 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    fprintf(stdout, "[WORKER] Connecting to coordinator at %s:%s\n",
            coord_host, coord_port);

    /* ── Register (with retransmission) ── */
    uint32_t seq        = 1;
    uint32_t worker_id   = 0;
    uint8_t  my_task_id  = 0;
    int      registered  = 0;

    for (int attempt = 0; attempt < MAX_RETRIES && !registered; attempt++) {
        send_register(sock, seq);
        fprintf(stdout, "[WORKER] Sent MSG_REGISTER (attempt %d)\n", attempt + 1);

        char buf[MAX_DATAGRAM];
        ssize_t n = recv_with_timeout(sock, buf, sizeof(buf));
        if (n <= 0) {
            fprintf(stdout, "[WORKER] No response — retrying registration\n");
            continue;
        }

        msg_header hdr;
        memcpy(&hdr, buf, sizeof(hdr));
        header_to_host(&hdr);

        if (hdr.msg_type == MSG_REGISTER_ACK) {
            register_ack_payload ack;
            memcpy(&ack, buf + sizeof(hdr), sizeof(ack));
            worker_id  = ntohl(ack.worker_id);
            my_task_id = hdr.task_id;
            registered = 1;
            fprintf(stdout,
                    "[WORKER] Registered! worker_id=%u  task=%s\n",
                    worker_id, task_name(my_task_id));
        }
        seq++;
    }

    if (!registered) {
        fprintf(stderr, "[WORKER] Failed to register after %d attempts\n",
                MAX_RETRIES);
        close(sock);
        return EXIT_FAILURE;
    }

    /* ── Main work loop ── */
    int chunks_processed = 0;

    while (!g_leaving) {
        char buf[MAX_DATAGRAM];
        ssize_t n = recv_with_timeout(sock, buf, sizeof(buf));

        if (n == 0) {
            /*
             * Timeout waiting for the coordinator. This can mean:
             *   (a) we are between chunks and the coordinator is busy
             *       with other workers — send a heartbeat so we are
             *       not forgotten, or
             *   (b) our last MSG_RESULT was lost — but since we have
             *       no chunk in flight info stored beyond what we just
             *       sent, the simplest robust behaviour is to send a
             *       heartbeat; the coordinator will hand us new work
             *       on the next message it processes from us.
             */
            fprintf(stdout, "[WORKER] Idle timeout — sending heartbeat\n");
            msg_header hb;
            memset(&hb, 0, sizeof(hb));
            hb.msg_type = MSG_HEARTBEAT;
            hb.seq_num  = seq++;
            hb.chunk_id = CHUNK_ID_NONE;
            header_to_network(&hb);
            send(sock, &hb, sizeof(hb), 0);
            continue;
        }
        if (n < 0) {
            perror("recv");
            break;
        }
        if ((size_t)n < sizeof(msg_header)) continue;

        msg_header hdr;
        memcpy(&hdr, buf, sizeof(hdr));
        header_to_host(&hdr);

        if (hdr.msg_type == MSG_NO_MORE_WORK) {
            fprintf(stdout,
                    "[WORKER] Coordinator says job is complete. "
                    "Processed %d chunk(s). Exiting.\n", chunks_processed);
            break;
        }

        if (hdr.msg_type == MSG_RESULT_ACK) {
            /* Acknowledgement for our previous result — nothing to do,
             * the coordinator's next message (a new MSG_TASK or
             * MSG_NO_MORE_WORK) will arrive separately. */
            continue;
        }

        if (hdr.msg_type != MSG_TASK) {
            fprintf(stderr, "[WORKER] Unexpected msg_type 0x%02x — ignoring\n",
                    hdr.msg_type);
            continue;
        }

        /* ── Process the chunk using the task registry ── */
        const task_descriptor *t = get_task(hdr.task_id);
        if (!t) {
            fprintf(stderr, "[WORKER] Unknown task_id %u — ignoring chunk\n",
                    hdr.task_id);
            continue;
        }

        uint32_t data_len;
        memcpy(&data_len, buf + sizeof(hdr), sizeof(data_len));
        data_len = ntohl(data_len);

        const char *data = buf + sizeof(hdr) + sizeof(data_len);

        fprintf(stdout,
                "[WORKER %u] Task: chunk %u, %u bytes, task=%s\n",
                worker_id, hdr.chunk_id, data_len, t->name);

        int64_t result = t->map(data, data_len);

        fprintf(stdout,
                "[WORKER %u] Chunk %u → result=%lld — sending\n",
                worker_id, hdr.chunk_id, (long long)result);

        send_result(sock, seq++, hdr.chunk_id, hdr.task_id, result);
        chunks_processed++;
    }

    /* ── Graceful leave (Ctrl+C) ── */
    if (g_leaving) {
        fprintf(stdout,
                "[WORKER %u] Leaving gracefully (SIGINT/SIGTERM) after "
                "%d chunk(s)\n", worker_id, chunks_processed);
        send_leave(sock, seq++);
    }

    close(sock);
    return EXIT_SUCCESS;
}
