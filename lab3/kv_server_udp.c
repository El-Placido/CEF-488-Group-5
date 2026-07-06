/*
 * kv_server_udp.c  —  Lab 3 Task 3: UDP Key-Value Server
 *
 * UDP differs fundamentally from TCP:
 *   • Connectionless — no accept(), no per-client state.
 *   • Each recvfrom() delivers exactly one datagram (complete message).
 *   • Unreliable — datagrams can be lost or reordered.
 *   • The server must sendto() back to the sender's address.
 *
 * Because each datagram is self-contained (our packets are ≤ 648 bytes,
 * well within one UDP datagram), we do not need a receive buffer for
 * partial packets.
 *
 * Build:
 *   gcc -Wall -Wextra -Werror -o kv_server_udp \
 *       kv_server_udp.c kv_store.c
 *
 * Run:
 *   ./kv_server_udp 12346
 *
 * NOTE: Both TCP and UDP servers share the same hash table logic but
 *       are separate processes.  Run them on different ports.
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "kv_protocol.h"
#include "kv_store.h"

/* ====================================================================
 * Globals
 * ==================================================================== */
static volatile sig_atomic_t running = 1;
static struct kv_store        g_store;

/* ====================================================================
 * Signal handler
 * ==================================================================== */
static void handle_signal(int signo)
{
    (void)signo;
    running = 0;
}

/* ====================================================================
 * Process one complete UDP datagram
 *
 * `buf`  — the raw bytes of the received datagram
 * `len`  — number of bytes received
 * `sock` — the UDP socket to send the response on
 * `peer` — sender's address (from recvfrom)
 * `plen` — length of peer address structure
 * ==================================================================== */
static void process_datagram(int sock,
                              const uint8_t *buf, ssize_t len,
                              const struct sockaddr *peer, socklen_t plen)
{
    if (len < (ssize_t)sizeof(struct kv_request)) {
        /* Too short to be a valid request — send error and return */
        uint8_t resp[sizeof(struct kv_response)];
        int n = build_response(resp, STATUS_BAD_REQ, NULL);
        sendto(sock, resp, (size_t)n, 0, peer, plen);
        return;
    }

    /* ── Parse header ── */
    struct kv_request hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    uint32_t opcode  = ntohl(hdr.opcode);
    uint32_t pkt_len = ntohl(hdr.length);

    /* Basic sanity check on declared length vs actual bytes received */
    if ((ssize_t)pkt_len != len) {
        uint8_t resp[sizeof(struct kv_response)];
        int n = build_response(resp, STATUS_BAD_REQ, NULL);
        sendto(sock, resp, (size_t)n, 0, peer, plen);
        return;
    }

    const char *payload = (const char *)(buf + sizeof(hdr));
    uint32_t    pay_len = pkt_len - (uint32_t)sizeof(hdr);

    uint8_t resp_buf[KV_MAX_PACKET_LEN];
    int     resp_len = 0;

    switch (opcode) {

    case OP_SET: {
        const char *key = payload;
        size_t key_slen = strnlen(key, pay_len);
        if (key_slen >= pay_len) {
            resp_len = build_response(resp_buf, STATUS_BAD_REQ, NULL);
            break;
        }
        const char *val = key + key_slen + 1;
        if (kv_store_set(&g_store, key, val) == 0) {
            fprintf(stdout, "[UDP SET] %s = %s\n", key, val);
            resp_len = build_response(resp_buf, STATUS_SUCCESS, NULL);
        } else {
            resp_len = build_response(resp_buf, STATUS_ERROR, NULL);
        }
        break;
    }

    case OP_GET: {
        const char *key = payload;
        const char *val = kv_store_get(&g_store, key);
        if (val != NULL) {
            fprintf(stdout, "[UDP GET] %s → %s\n", key, val);
            resp_len = build_response(resp_buf, STATUS_SUCCESS, val);
        } else {
            fprintf(stdout, "[UDP GET] %s → NOT FOUND\n", key);
            resp_len = build_response(resp_buf, STATUS_NOT_FOUND, NULL);
        }
        break;
    }

    case OP_DEL: {
        const char *key = payload;
        if (kv_store_delete(&g_store, key) == 0) {
            fprintf(stdout, "[UDP DEL] %s deleted\n", key);
            resp_len = build_response(resp_buf, STATUS_SUCCESS, NULL);
        } else {
            fprintf(stdout, "[UDP DEL] %s NOT FOUND\n", key);
            resp_len = build_response(resp_buf, STATUS_NOT_FOUND, NULL);
        }
        break;
    }

    default:
        resp_len = build_response(resp_buf, STATUS_BAD_REQ, NULL);
        break;
    }

    if (resp_len > 0)
        sendto(sock, resp_buf, (size_t)resp_len, 0, peer, plen);
}

/* ====================================================================
 * main
 * ==================================================================== */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        return EXIT_FAILURE;
    }
    const char *port = argv[1];

    /* ── Signal handling ── */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    kv_store_init(&g_store);

    /* ── getaddrinfo for UDP (SOCK_DGRAM) ── */
    struct addrinfo hints, *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;   /* UDP */
    hints.ai_flags    = AI_PASSIVE;

    int err = getaddrinfo(NULL, port, &hints, &result);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return EXIT_FAILURE;
    }

    int sock = -1;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock == -1) continue;

        int optval = 1;
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

        if (rp->ai_family == AF_INET6) {
            int v6only = 0;
            setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY,
                       &v6only, sizeof(v6only));
        }

        if (bind(sock, rp->ai_addr, rp->ai_addrlen) == 0)
            break;

        close(sock);
        sock = -1;
    }
    freeaddrinfo(result);

    if (sock == -1) {
        fprintf(stderr, "Could not bind UDP socket on port %s\n", port);
        return EXIT_FAILURE;
    }

    fprintf(stdout, "[INFO] UDP server listening on port %s\n", port);

    /* ── Main receive loop ── */
    uint8_t buf[KV_MAX_PACKET_LEN];

    while (running) {
        struct sockaddr_storage peer_addr;
        socklen_t               peer_len = sizeof(peer_addr);

        /*
         * recvfrom() receives one complete datagram.
         * If the datagram is larger than buf, the excess is silently
         * discarded by the kernel — that is why we limit packet size.
         */
        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                             (struct sockaddr *)&peer_addr, &peer_len);

        if (n == -1) {
            if (errno == EINTR) continue;   /* interrupted by signal   */
            perror("recvfrom");
            break;
        }

        process_datagram(sock, buf, n,
                         (struct sockaddr *)&peer_addr, peer_len);
    }

    close(sock);
    kv_store_destroy(&g_store);
    fprintf(stdout, "[INFO] UDP server stopped.\n");
    return EXIT_SUCCESS;
}
