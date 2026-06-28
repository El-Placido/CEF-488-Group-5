/*
 * worker.c — Distributed Data Processing System
 *
 * Responsibilities:
 *   1. Register with the coordinator on startup.
 *   2. Receive data chunks assigned by the coordinator.
 *   3. Process each chunk according to the task type specified by the coordinator.
 *   4. Return results to the coordinator with reliable delivery.
 *   5. Send heartbeats so the coordinator knows the worker is alive.
 *   6. Handle SHUTDOWN gracefully.
 *
 * Supported task types (chosen by coordinator):
 *   TASK_WORD_COUNT  — count space/newline separated tokens
 *   TASK_SUM_NUMBERS — sum all integers found in the chunk
 *   TASK_LINE_COUNT  — count newline characters
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
#include <signal.h>
#include <time.h>
#include <ctype.h>
#include <arpa/inet.h>
#include <endian.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "protocol.h"
#include "net_util.h"

/* ─── Worker state ─────────────────────────────────────────────────────────── */
static int         g_running    = 1;    /* set to 0 by SIGINT or MSG_SHUTDOWN */
static uint16_t    g_worker_id  = 0;
static task_type_t g_task       = TASK_WORD_COUNT;
static uint16_t    g_seq        = 0;    /* outbound sequence counter          */

/* ─── Signal handler — clean shutdown ─────────────────────────────────────── */
static void on_sigint(int sig)
{
    (void)sig;
    g_running = 0;
}

/* ─── Processing functions ─────────────────────────────────────────────────── */

/*
 * count_words — count whitespace-delimited tokens in buf[0..len-1].
 * Transitions: non-space after space (or start) = +1 word.
 */
static int64_t count_words(const char *buf, uint32_t len)
{
    int64_t count   = 0;
    int     in_word = 0;
    for (uint32_t i = 0; i < len; i++) {
        if (isspace((unsigned char)buf[i])) {
            in_word = 0;
        } else {
            if (!in_word) { count++; in_word = 1; }
        }
    }
    return count;
}

/*
 * sum_numbers — extract ASCII decimal integers (optionally negative)
 * from the chunk and return their sum.
 */
static int64_t sum_numbers(const char *buf, uint32_t len)
{
    int64_t total = 0;
    uint32_t i    = 0;
    while (i < len) {
        /* Skip non-digit / non-minus characters */
        while (i < len && !isdigit((unsigned char)buf[i]) && buf[i] != '-')
            i++;
        if (i >= len) break;

        /* Detect negative sign */
        int neg = 0;
        if (buf[i] == '-') {
            if (i + 1 < len && isdigit((unsigned char)buf[i+1])) {
                neg = 1; i++;
            } else {
                i++; continue;
            }
        }

        /* Parse digits */
        int64_t num = 0;
        while (i < len && isdigit((unsigned char)buf[i])) {
            num = num * 10 + (buf[i] - '0');
            i++;
        }
        total += (neg ? -num : num);
    }
    return total;
}

/*
 * count_lines — count '\n' characters in the chunk.
 */
static int64_t count_lines(const char *buf, uint32_t len)
{
    int64_t count = 0;
    for (uint32_t i = 0; i < len; i++)
        if (buf[i] == '\n') count++;
    return count;
}

/*
 * process_chunk — dispatch to the correct function based on g_task.
 * Fills result_detail with a human-readable summary string.
 */
static int64_t process_chunk(const char *data, uint32_t len,
                              uint32_t chunk_id, char *result_detail)
{
    int64_t value = 0;
    const char *task_name = "";

    switch (g_task) {
    case TASK_WORD_COUNT:
        value     = count_words(data, len);
        task_name = "words";
        break;
    case TASK_SUM_NUMBERS:
        value     = sum_numbers(data, len);
        task_name = "sum";
        break;
    case TASK_LINE_COUNT:
        value     = count_lines(data, len);
        task_name = "lines";
        break;
    default:
        fprintf(stderr, "[worker %d] unknown task %d\n", g_worker_id, g_task);
    }

    snprintf(result_detail, MAX_RESULT_SIZE,
             "chunk=%u %s=%lld bytes=%u",
             chunk_id, task_name, (long long)value, len);

    printf("[worker %d] Processed chunk %u — %s\n",
           g_worker_id, chunk_id, result_detail);
    return value;
}

/* ─── Register with coordinator, retry until success ─────────────────────── */
static int do_register(int sock, struct sockaddr_in *coord_addr,
                        uint16_t my_listen_port)
{
    payload_register_t reg;
    reg.listen_port = htons(my_listen_port);

    uint8_t out[sizeof(msg_header_t) + sizeof(reg)];
    int len = build_message(out, sizeof(out), MSG_REGISTER, g_seq++, 0,
                            &reg, sizeof(reg));

    uint8_t ack_buf[sizeof(msg_header_t) + sizeof(payload_register_ack_t) + 16];
    struct sockaddr_in from;

    printf("[worker] Registering with coordinator at %s:%d...\n",
           inet_ntoa(coord_addr->sin_addr), COORDINATOR_PORT);

    int rcvd = udp_send_reliable(sock, out, len, coord_addr,
                                 MSG_REGISTER_ACK, ack_buf, sizeof(ack_buf), &from);
    if (rcvd < 0) {
        fprintf(stderr, "[worker] Registration failed.\n");
        return -1;
    }

    msg_header_t hdr;
    const uint8_t *payload = parse_header(ack_buf, (size_t)rcvd, &hdr);
    if (!payload || hdr.payload_len < sizeof(payload_register_ack_t)) {
        fprintf(stderr, "[worker] Bad REGISTER_ACK.\n");
        return -1;
    }

    const payload_register_ack_t *rack = (const payload_register_ack_t *)payload;
    g_worker_id = ntohs(rack->assigned_id);
    g_task      = (task_type_t)rack->task_type;

    const char *task_names[] = { "", "WORD_COUNT", "SUM_NUMBERS", "LINE_COUNT" };
    printf("[worker] Registered as ID=%d, task=%s\n",
           g_worker_id, task_names[g_task]);
    return 0;
}

/* ─── Send a heartbeat (fire-and-forget) ─────────────────────────────────── */
static void send_heartbeat(int sock, struct sockaddr_in *coord_addr)
{
    uint8_t out[sizeof(msg_header_t)];
    int len = build_message(out, sizeof(out), MSG_HEARTBEAT,
                            g_seq++, g_worker_id, NULL, 0);
    udp_send_simple(sock, out, len, coord_addr);
}

/* ─── Send result to coordinator with reliable delivery ─────────────────── */
static void send_result(int sock, struct sockaddr_in *coord_addr,
                         uint32_t chunk_id, int64_t value,
                         const char *detail)
{
    payload_result_t res;
    memset(&res, 0, sizeof(res));
    res.chunk_id = htonl(chunk_id);
    res.value    = (int64_t)htobe64((uint64_t)value);
    memset(res.detail, 0, sizeof(res.detail));
    memcpy(res.detail, detail, strnlen(detail, sizeof(res.detail) - 1));

    uint8_t out[sizeof(msg_header_t) + sizeof(res)];
    int len = build_message(out, sizeof(out), MSG_RESULT,
                            g_seq++, g_worker_id, &res, sizeof(res));

    uint8_t ack_buf[sizeof(msg_header_t) + sizeof(payload_result_ack_t) + 8];
    struct sockaddr_in from;

    int rcvd = udp_send_reliable(sock, out, len, coord_addr,
                                 MSG_RESULT_ACK, ack_buf, sizeof(ack_buf), &from);
    if (rcvd < 0)
        fprintf(stderr, "[worker %d] WARNING: result for chunk %u not ACKed\n",
                g_worker_id, chunk_id);
    else
        printf("[worker %d] Result for chunk %u acknowledged by coordinator.\n",
               g_worker_id, chunk_id);
}

/* ═══════════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr,
            "Usage: %s <coordinator_ip> <my_listen_port>\n", argv[0]);
        return 1;
    }

    signal(SIGINT,  on_sigint);
    signal(SIGTERM, on_sigint);

    const char *coord_ip   = argv[1];
    uint16_t my_port       = (uint16_t)atoi(argv[2]);

    /* Build coordinator address struct */
    struct sockaddr_in coord_addr;
    memset(&coord_addr, 0, sizeof(coord_addr));
    coord_addr.sin_family = AF_INET;
    coord_addr.sin_port   = htons(COORDINATOR_PORT);
    if (inet_pton(AF_INET, coord_ip, &coord_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid coordinator IP: %s\n", coord_ip);
        return 1;
    }

    /* Bind our own UDP socket so the coordinator can reach us */
    int sock = create_udp_socket(my_port);
    if (sock < 0) return 1;
    printf("[worker] Listening on UDP port %d\n", my_port);

    /* Register with the coordinator */
    if (do_register(sock, &coord_addr, my_port) < 0) {
        close(sock);
        return 1;
    }

    /* Set socket receive timeout for the main loop */
    struct timeval tv = { .tv_sec = HEARTBEAT_INTERVAL, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    time_t last_heartbeat = now_sec();

    /* ── Main receive loop ────────────────────────────────────────────── */
    uint8_t buf[sizeof(msg_header_t) + sizeof(payload_chunk_t) + 16];

    while (g_running) {

        /* Send heartbeat if interval has elapsed */
        if ((now_sec() - last_heartbeat) >= HEARTBEAT_INTERVAL) {
            send_heartbeat(sock, &coord_addr);
            last_heartbeat = now_sec();
        }

        struct sockaddr_in from;
        socklen_t from_len = sizeof(from);
        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                             (struct sockaddr *)&from, &from_len);

        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* Timeout: send heartbeat and loop */
                send_heartbeat(sock, &coord_addr);
                last_heartbeat = now_sec();
                continue;
            }
            perror("[worker] recvfrom");
            break;
        }

        msg_header_t hdr;
        const uint8_t *payload = parse_header(buf, (size_t)n, &hdr);
        if (!payload) {
            fprintf(stderr, "[worker %d] malformed packet\n", g_worker_id);
            continue;
        }

        switch ((msg_type_t)hdr.type) {

        case MSG_CHUNK_ASSIGN: {
            /* Validate payload length */
            if (hdr.payload_len < sizeof(payload_chunk_t)) {
                fprintf(stderr, "[worker %d] CHUNK_ASSIGN too short\n", g_worker_id);
                break;
            }
            const payload_chunk_t *pc = (const payload_chunk_t *)payload;
            uint32_t chunk_id = ntohl(pc->chunk_id);
            uint32_t data_len = ntohl(pc->data_len);

            printf("[worker %d] Received chunk %u (%u bytes)\n",
                   g_worker_id, chunk_id, data_len);

            /* ACK the chunk receipt immediately */
            payload_chunk_ack_t ca;
            ca.chunk_id = htonl(chunk_id);
            uint8_t out[sizeof(msg_header_t) + sizeof(ca)];
            int len = build_message(out, sizeof(out), MSG_CHUNK_ACK,
                                    g_seq++, g_worker_id, &ca, sizeof(ca));
            udp_send_simple(sock, out, len, &from);

            /* Process the chunk */
            char detail[MAX_RESULT_SIZE];
            int64_t value = process_chunk(pc->data, data_len, chunk_id, detail);

            /* Send result back with reliable delivery */
            send_result(sock, &coord_addr, chunk_id, value, detail);
            break;
        }

        case MSG_HEARTBEAT_ACK:
            /* Coordinator acknowledged our heartbeat — nothing to do */
            break;

        case MSG_SHUTDOWN:
            printf("[worker %d] Received SHUTDOWN from coordinator. Exiting.\n",
                   g_worker_id);
            g_running = 0;
            break;

        case MSG_DONE:
            printf("[worker %d] Coordinator says processing is DONE.\n",
                   g_worker_id);
            g_running = 0;
            break;

        default:
            fprintf(stderr, "[worker %d] unknown msg type %d\n",
                    g_worker_id, hdr.type);
        }
    }

    printf("[worker %d] Shutting down.\n", g_worker_id);
    close(sock);
    return 0;
}
