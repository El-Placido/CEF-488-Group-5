/*
 * kv_client.c  —  Lab 3 Task 2/3: Key-Value Client (TCP + UDP)
 *
 * Usage:
 *   ./kv_client <host> <port> set <key> <value>   # TCP SET
 *   ./kv_client <host> <port> get <key>           # TCP GET
 *   ./kv_client <host> <port> del <key>           # TCP DEL
 *   ./kv_client -u <host> <port> set <key> <value>  # UDP SET
 *   ./kv_client -u <host> <port> get <key>           # UDP GET
 *
 * TCP mode:
 *   • Connects, sends one request, reads the response, prints result.
 *
 * UDP mode:
 *   • Sends the request as a single datagram.
 *   • Waits for a response with a timeout (SO_RCVTIMEO).
 *   • If timeout: retransmit with exponential backoff (initial 200 ms,
 *     doubled each retry, up to 3 retries total).
 *   • If still no response: prints error and exits with failure.
 *
 * Build:
 *   gcc -Wall -Wextra -Werror -o kv_client kv_client.c
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "kv_protocol.h"

/* ====================================================================
 * UDP retransmission parameters
 * ==================================================================== */
#define UDP_INITIAL_TIMEOUT_MS  200    /* first timeout: 200 milliseconds */
#define UDP_MAX_RETRIES         3      /* try 3 times before giving up    */

/* ====================================================================
 * Helper: write all bytes to a socket, handling partial writes
 *
 * TCP's send() may transfer fewer bytes than requested on a non-blocking
 * socket or when the send buffer is full.  This wrapper retries until
 * all bytes are sent (TLPI §61.1).
 * ==================================================================== */
static int send_all(int sock, const void *buf, size_t len)
{
    const char *ptr = (const char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, ptr + sent, len - sent, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

/* ====================================================================
 * Helper: read exactly `len` bytes from a TCP socket
 *
 * TCP is a byte stream — recv() may return fewer bytes than requested.
 * We loop until we have exactly the requested count (TLPI §61.1).
 * ==================================================================== */
static int recv_all(int sock, void *buf, size_t len)
{
    char *ptr = (char *)buf;
    size_t total = 0;
    while (total < len) {
        ssize_t n = recv(sock, ptr + total, len - total, 0);
        if (n == 0) return -1;   /* connection closed unexpectedly     */
        if (n == -1) {
            if (errno == EINTR) continue;
            return -1;
        }
        total += (size_t)n;
    }
    return 0;
}

/* ====================================================================
 * Print a human-readable response status
 * ==================================================================== */
static void print_response(const struct kv_response *hdr, const char *val)
{
    uint32_t status = ntohl(hdr->status);
    switch (status) {
    case STATUS_SUCCESS:
        if (val && val[0] != '\0')
            printf("[OK] value = \"%s\"\n", val);
        else
            printf("[OK]\n");
        break;
    case STATUS_NOT_FOUND:
        printf("[NOT FOUND] Key does not exist\n");
        break;
    case STATUS_ERROR:
        printf("[ERROR] Server reported an error\n");
        break;
    case STATUS_BAD_REQ:
        printf("[BAD REQUEST] Malformed request\n");
        break;
    default:
        printf("[UNKNOWN STATUS %u]\n", status);
        break;
    }
}

/* ====================================================================
 * TCP operation — connect, send request, receive response, print
 * ==================================================================== */
static int do_tcp(const char *host, const char *port,
                  const uint8_t *req_buf, int req_len)
{
    /* ── Resolve server address (supports IPv4 and IPv6) ── */
    struct addrinfo hints, *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int err = getaddrinfo(host, port, &hints, &result);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return -1;
    }

    int sock = -1;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock == -1) continue;
        if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0)
            break;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(result);

    if (sock == -1) {
        fprintf(stderr, "Cannot connect to %s:%s\n", host, port);
        return -1;
    }

    /* Disable Nagle for lower latency */
    int nodelay = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    /* ── Send the request ── */
    if (send_all(sock, req_buf, (size_t)req_len) == -1) {
        perror("send");
        close(sock);
        return -1;
    }

    /* ── Receive the fixed-size response header ── */
    struct kv_response resp_hdr;
    if (recv_all(sock, &resp_hdr, sizeof(resp_hdr)) == -1) {
        fprintf(stderr, "Failed to receive response header\n");
        close(sock);
        return -1;
    }

    uint32_t total_len = ntohl(resp_hdr.length);
    uint32_t pay_len   = total_len - (uint32_t)sizeof(resp_hdr);

    /* ── Receive optional payload (value string for GET) ── */
    char val_buf[KV_MAX_VAL_LEN] = {0};
    if (pay_len > 0 && pay_len < KV_MAX_VAL_LEN) {
        if (recv_all(sock, val_buf, pay_len) == -1) {
            fprintf(stderr, "Failed to receive response payload\n");
            close(sock);
            return -1;
        }
    }

    print_response(&resp_hdr, val_buf);
    close(sock);
    return 0;
}

/* ====================================================================
 * UDP operation — send datagram, wait with timeout, retransmit
 *
 * Exponential backoff algorithm (TLPI §61.12):
 *   • Start with INITIAL_TIMEOUT_MS.
 *   • If no response within the timeout, resend and double the timeout.
 *   • Give up after UDP_MAX_RETRIES attempts.
 * ==================================================================== */
static int do_udp(const char *host, const char *port,
                  const uint8_t *req_buf, int req_len)
{
    struct addrinfo hints, *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;

    int err = getaddrinfo(host, port, &hints, &result);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return -1;
    }

    int sock = -1;
    struct addrinfo *chosen = NULL;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock == -1) continue;
        chosen = rp;
        break;
    }

    if (sock == -1) {
        freeaddrinfo(result);
        fprintf(stderr, "Cannot create UDP socket\n");
        return -1;
    }

    int timeout_ms = UDP_INITIAL_TIMEOUT_MS;
    int success    = 0;

    for (int attempt = 1; attempt <= UDP_MAX_RETRIES; attempt++) {
        /* ── Send the datagram ── */
        ssize_t sent = sendto(sock, req_buf, (size_t)req_len, 0,
                              chosen->ai_addr, chosen->ai_addrlen);
        if (sent == -1) {
            perror("sendto");
            break;
        }
        printf("[UDP] Attempt %d: sent %zd bytes (timeout %d ms)\n",
               attempt, sent, timeout_ms);

        /*
         * Set receive timeout with SO_RCVTIMEO.
         * If recvfrom() doesn't return within this time, it returns -1
         * with errno == EAGAIN / EWOULDBLOCK (TLPI §61.12).
         */
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        /* ── Wait for response ── */
        uint8_t resp_buf[KV_MAX_PACKET_LEN];
        ssize_t n = recvfrom(sock, resp_buf, sizeof(resp_buf), 0,
                             NULL, NULL);

        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK ||
                errno == EINPROGRESS) {
                printf("[UDP] Timeout on attempt %d — retrying...\n",
                       attempt);
                /*
                 * Exponential backoff: double the timeout for the next
                 * attempt.  This reduces network congestion when the
                 * server is slow or packets are being lost.
                 */
                timeout_ms *= 2;
                continue;
            }
            perror("recvfrom");
            break;
        }

        /* ── Parse and print response ── */
        if (n >= (ssize_t)sizeof(struct kv_response)) {
            struct kv_response hdr;
            memcpy(&hdr, resp_buf, sizeof(hdr));
            uint32_t pay_len = ntohl(hdr.length) - (uint32_t)sizeof(hdr);

            char val[KV_MAX_VAL_LEN] = {0};
            if (pay_len > 0 && pay_len < KV_MAX_VAL_LEN)
                memcpy(val, resp_buf + sizeof(hdr), pay_len);

            print_response(&hdr, val);
            success = 1;
        }
        break;
    }

    freeaddrinfo(result);
    close(sock);

    if (!success)
        fprintf(stderr, "[UDP] No response after %d attempts.\n",
                UDP_MAX_RETRIES);
    return success ? 0 : -1;
}

/* ====================================================================
 * main — parse arguments and dispatch
 * ==================================================================== */
int main(int argc, char *argv[])
{
    int use_udp = 0;
    int argi    = 1;

    /* Check for -u flag (UDP mode) */
    if (argc > 1 && strcmp(argv[1], "-u") == 0) {
        use_udp = 1;
        argi    = 2;
    }

    if (argc - argi < 3) {
        fprintf(stderr,
            "Usage:\n"
            "  %s [-u] <host> <port> set <key> <value>\n"
            "  %s [-u] <host> <port> get <key>\n"
            "  %s [-u] <host> <port> del <key>\n"
            "  -u  use UDP instead of TCP\n",
            argv[0], argv[0], argv[0]);
        return EXIT_FAILURE;
    }

    const char *host = argv[argi++];
    const char *port = argv[argi++];
    const char *op   = argv[argi++];

    /* ── Build the request packet ── */
    uint8_t req_buf[KV_MAX_PACKET_LEN];
    int     req_len = 0;

    if (strcmp(op, "set") == 0) {
        if (argc - argi < 2) {
            fprintf(stderr, "set requires <key> <value>\n");
            return EXIT_FAILURE;
        }
        req_len = build_set_request(req_buf, argv[argi], argv[argi + 1]);

    } else if (strcmp(op, "get") == 0) {
        if (argc - argi < 1) {
            fprintf(stderr, "get requires <key>\n");
            return EXIT_FAILURE;
        }
        req_len = build_get_request(req_buf, argv[argi]);

    } else if (strcmp(op, "del") == 0) {
        if (argc - argi < 1) {
            fprintf(stderr, "del requires <key>\n");
            return EXIT_FAILURE;
        }
        req_len = build_del_request(req_buf, argv[argi]);

    } else {
        fprintf(stderr, "Unknown operation: %s\n", op);
        return EXIT_FAILURE;
    }

    if (req_len <= 0) {
        fprintf(stderr, "Key or value too long\n");
        return EXIT_FAILURE;
    }

    /* ── Dispatch to TCP or UDP handler ── */
    int ret;
    if (use_udp)
        ret = do_udp(host, port, req_buf, req_len);
    else
        ret = do_tcp(host, port, req_buf, req_len);

    return (ret == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
