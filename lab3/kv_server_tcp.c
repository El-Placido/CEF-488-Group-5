/*
 * kv_server_tcp.c  —  Lab 3 Task 2: TCP Key-Value Server
 *
 * Architecture overview:
 *   • Uses getaddrinfo(AI_PASSIVE) to bind to the given port on all
 *     interfaces, supporting both IPv4 and IPv6 transparently.
 *   • A single epoll instance in edge-triggered mode (EPOLLET)
 *     monitors the listening socket and all connected client sockets.
 *   • Each connected client has a per-client receive buffer so we can
 *     handle partial reads across multiple epoll events.
 *   • Signal handling: SIGTERM / SIGINT set a flag that breaks the
 *     main loop and triggers cleanup.
 *
 * Concurrency model: single-threaded, event-driven.
 *   No threads, no forks.  One epoll loop handles thousands of
 *   simultaneous connections with very low overhead (TLPI §63.4).
 *
 * Build:
 *   gcc -Wall -Wextra -Werror -o kv_server_tcp \
 *       kv_server_tcp.c kv_store.c
 *
 * Run:
 *   ./kv_server_tcp 12345
 *   ./kv_server_tcp 12345 ::1        # bind to IPv6 loopback only
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>

#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>          /* getaddrinfo(), freeaddrinfo()            */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>    /* TCP_NODELAY                              */

#include "kv_protocol.h"
#include "kv_store.h"

/* ====================================================================
 * Configuration constants
 * ==================================================================== */
#define LISTEN_BACKLOG     128      /* listen() queue length            */
#define MAX_EVENTS         64       /* epoll_wait() batch size          */
#define CLIENT_BUF_SIZE    2048     /* per-client receive buffer size   */
#define MAX_CLIENTS        1024     /* maximum simultaneous connections */

/* ====================================================================
 * Per-client state
 *
 * Because we use edge-triggered epoll we may receive partial packets.
 * We buffer incoming bytes here until a complete packet is assembled.
 * ==================================================================== */
struct client_state {
    int      fd;                        /* socket file descriptor       */
    uint8_t  buf[CLIENT_BUF_SIZE];      /* receive buffer               */
    int      buf_len;                   /* bytes currently in buffer    */
    int      in_use;                    /* 1 = slot occupied            */
};

/* ====================================================================
 * Globals
 * ==================================================================== */
static volatile sig_atomic_t running = 1;
static struct kv_store        g_store;
static struct client_state    g_clients[MAX_CLIENTS];

/* ====================================================================
 * Signal handler
 * ==================================================================== */
static void handle_signal(int signo)
{
    (void)signo;
    running = 0;
}

/* ====================================================================
 * Helper: set a file descriptor to non-blocking mode (TLPI §5.9)
 * ==================================================================== */
static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ====================================================================
 * Helper: find or allocate a client slot by fd
 * ==================================================================== */
static struct client_state *get_client(int fd)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (g_clients[i].in_use && g_clients[i].fd == fd)
            return &g_clients[i];
    return NULL;
}

static struct client_state *alloc_client(int fd)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!g_clients[i].in_use) {
            g_clients[i].fd      = fd;
            g_clients[i].buf_len = 0;
            g_clients[i].in_use  = 1;
            return &g_clients[i];
        }
    }
    return NULL;   /* table full */
}

static void free_client(struct client_state *c)
{
    memset(c, 0, sizeof(*c));   /* zeroes in_use too */
}

/* ====================================================================
 * Process one complete request packet from a client
 * ==================================================================== */
static void process_request(int client_fd,
                             const uint8_t *pkt, uint32_t pkt_len)
{
    if (pkt_len < (uint32_t)sizeof(struct kv_request)) {
        /* Malformed — send error response */
        uint8_t resp[sizeof(struct kv_response)];
        int n = build_response(resp, STATUS_BAD_REQ, NULL);
        send(client_fd, resp, (size_t)n, MSG_NOSIGNAL);
        return;
    }

    /* ── Parse header ── */
    struct kv_request hdr;
    memcpy(&hdr, pkt, sizeof(hdr));
    uint32_t opcode = ntohl(hdr.opcode);

    /* Payload starts immediately after the fixed header */
    const char *payload = (const char *)(pkt + sizeof(hdr));
    uint32_t    pay_len = pkt_len - (uint32_t)sizeof(hdr);

    uint8_t resp_buf[KV_MAX_PACKET_LEN];
    int     resp_len = 0;

    switch (opcode) {

    /* ── SET: payload = key\0 value\0 ── */
    case OP_SET: {
        /* Find the null terminator of the key */
        const char *key = payload;
        size_t key_slen = strnlen(key, pay_len);
        if (key_slen >= pay_len) {
            /* No null terminator found — bad request */
            resp_len = build_response(resp_buf, STATUS_BAD_REQ, NULL);
            break;
        }
        const char *val = key + key_slen + 1;
        /* Store the key-value pair */
        if (kv_store_set(&g_store, key, val) == 0) {
            fprintf(stdout, "[SET] key=%s value=%s\n", key, val);
            resp_len = build_response(resp_buf, STATUS_SUCCESS, NULL);
        } else {
            resp_len = build_response(resp_buf, STATUS_ERROR, NULL);
        }
        break;
    }

    /* ── GET: payload = key\0 ── */
    case OP_GET: {
        const char *key = payload;
        const char *val = kv_store_get(&g_store, key);
        if (val != NULL) {
            fprintf(stdout, "[GET] key=%s → value=%s\n", key, val);
            resp_len = build_response(resp_buf, STATUS_SUCCESS, val);
        } else {
            fprintf(stdout, "[GET] key=%s → NOT FOUND\n", key);
            resp_len = build_response(resp_buf, STATUS_NOT_FOUND, NULL);
        }
        break;
    }

    /* ── DEL: payload = key\0 ── */
    case OP_DEL: {
        const char *key = payload;
        if (kv_store_delete(&g_store, key) == 0) {
            fprintf(stdout, "[DEL] key=%s → deleted\n", key);
            resp_len = build_response(resp_buf, STATUS_SUCCESS, NULL);
        } else {
            fprintf(stdout, "[DEL] key=%s → NOT FOUND\n", key);
            resp_len = build_response(resp_buf, STATUS_NOT_FOUND, NULL);
        }
        break;
    }

    default:
        resp_len = build_response(resp_buf, STATUS_BAD_REQ, NULL);
        break;
    }

    if (resp_len > 0)
        send(client_fd, resp_buf, (size_t)resp_len, MSG_NOSIGNAL);
}

/* ====================================================================
 * Handle data available on a connected client socket
 *
 * Because we use EPOLLET (edge-triggered), we MUST read until EAGAIN
 * or EWOULDBLOCK to ensure we do not miss data (TLPI §63.4.5).
 * ==================================================================== */
static void handle_client_data(int epfd, struct client_state *c)
{
    for (;;) {
        /* Read as many bytes as fit in the remaining buffer space */
        ssize_t n = recv(c->fd,
                         c->buf + c->buf_len,
                         CLIENT_BUF_SIZE - c->buf_len,
                         0);

        if (n == 0) {
            /* Client closed connection (EOF) */
            fprintf(stdout, "[INFO] Client fd=%d disconnected\n", c->fd);
            epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
            close(c->fd);
            free_client(c);
            return;
        }

        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;   /* all data consumed — stop reading           */
            perror("recv");
            epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
            close(c->fd);
            free_client(c);
            return;
        }

        c->buf_len += (int)n;

        /*
         * Process all complete packets in the buffer.
         * A packet is complete when buf_len >= 4 (enough to read the
         * `length` field) AND buf_len >= the value of that `length` field.
         */
        while (c->buf_len >= (int)sizeof(struct kv_request)) {
            /* Peek at the length field (still in network byte order) */
            uint32_t pkt_len_net;
            memcpy(&pkt_len_net, c->buf, sizeof(uint32_t));
            uint32_t pkt_len = ntohl(pkt_len_net);

            if (pkt_len < (uint32_t)sizeof(struct kv_request) ||
                pkt_len > KV_MAX_PACKET_LEN) {
                /* Bogus length — drop this client */
                fprintf(stderr, "[WARN] fd=%d sent bogus length %u\n",
                        c->fd, pkt_len);
                epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
                close(c->fd);
                free_client(c);
                return;
            }

            if (c->buf_len < (int)pkt_len)
                break;   /* incomplete packet — wait for more data     */

            /* We have a full packet: process it */
            process_request(c->fd, c->buf, pkt_len);

            /* Shift remaining bytes to the front of the buffer */
            int remaining = c->buf_len - (int)pkt_len;
            if (remaining > 0)
                memmove(c->buf, c->buf + pkt_len, (size_t)remaining);
            c->buf_len = remaining;
        }
    }
}

/* ====================================================================
 * main
 * ==================================================================== */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port> [bind-address]\n", argv[0]);
        return EXIT_FAILURE;
    }
    const char *port    = argv[1];
    const char *bindaddr = (argc >= 3) ? argv[2] : NULL;

    /* ── Signal handling ── */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    /* ── Initialise the key-value store ── */
    kv_store_init(&g_store);
    memset(g_clients, 0, sizeof(g_clients));

    /* ── Step 1: getaddrinfo — protocol-independent address lookup ──
     *
     * AI_PASSIVE: return address suitable for bind().
     * If bindaddr is NULL, getaddrinfo fills in INADDR_ANY / in6addr_any.
     * This transparently handles both IPv4 and IPv6 (TLPI §59.10).
     */
    struct addrinfo hints, *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;     /* accept IPv4 or IPv6          */
    hints.ai_socktype = SOCK_STREAM;   /* TCP                          */
    hints.ai_flags    = AI_PASSIVE;    /* for bind()                   */

    int err = getaddrinfo(bindaddr, port, &hints, &result);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return EXIT_FAILURE;
    }

    /* ── Step 2: Create, configure, and bind the listening socket ── */
    int listen_fd = -1;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        listen_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (listen_fd == -1) continue;

        /*
         * SO_REUSEADDR: lets us re-bind to the port while it is in
         * TIME_WAIT (e.g. immediately after restarting the server).
         * Without this, bind() would fail with EADDRINUSE (TLPI §61.10).
         */
        int optval = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR,
                   &optval, sizeof(optval));

        /*
         * IPV6_V6ONLY = 0 on an IPv6 socket: accept both IPv4 and IPv6
         * connections (IPv4 addresses appear as IPv4-mapped IPv6 addresses).
         * This gives us a dual-stack server from a single socket.
         */
        if (rp->ai_family == AF_INET6) {
            int v6only = 0;
            setsockopt(listen_fd, IPPROTO_IPV6, IPV6_V6ONLY,
                       &v6only, sizeof(v6only));
        }

        if (bind(listen_fd, rp->ai_addr, rp->ai_addrlen) == 0)
            break;   /* successfully bound */

        close(listen_fd);
        listen_fd = -1;
    }
    freeaddrinfo(result);

    if (listen_fd == -1) {
        fprintf(stderr, "Could not bind to port %s\n", port);
        return EXIT_FAILURE;
    }

    set_nonblocking(listen_fd);
    listen(listen_fd, LISTEN_BACKLOG);
    fprintf(stdout, "[INFO] TCP server listening on port %s\n", port);

    /* ── Step 3: Create epoll instance ── */
    int epfd = epoll_create1(0);
    if (epfd == -1) { perror("epoll_create1"); return EXIT_FAILURE; }

    /*
     * Add the listening socket to epoll with EPOLLIN | EPOLLET.
     * EPOLLET = edge-triggered: we get notified only when the state
     * changes from "no data" to "data available".  We must then call
     * accept() in a loop until EAGAIN (TLPI §63.4.5).
     */
    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        perror("epoll_ctl(listen_fd)");
        return EXIT_FAILURE;
    }

    struct epoll_event events[MAX_EVENTS];

    /* ── Step 4: Main event loop ── */
    while (running) {
        int nready = epoll_wait(epfd, events, MAX_EVENTS, 1000);

        if (nready == -1) {
            if (errno == EINTR) continue;   /* interrupted by signal   */
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nready; i++) {
            int fd = events[i].data.fd;

            /* ── New connection on the listening socket ── */
            if (fd == listen_fd) {
                /*
                 * Edge-triggered: accept() in a loop until EAGAIN.
                 * Each call may accept a different client.
                 */
                for (;;) {
                    struct sockaddr_storage peer_addr;
                    socklen_t peer_len = sizeof(peer_addr);
                    int conn_fd = accept(listen_fd,
                                        (struct sockaddr *)&peer_addr,
                                        &peer_len);
                    if (conn_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;   /* no more pending connections     */
                        perror("accept");
                        break;
                    }

                    /* Set new connection to non-blocking */
                    set_nonblocking(conn_fd);

                    /*
                     * TCP_NODELAY: disables Nagle's algorithm.
                     * Nagle's algorithm coalesces small packets to reduce
                     * overhead, but adds latency for request-response
                     * workloads.  We disable it for lower latency.
                     * (TLPI §61.3)
                     */
                    int nodelay = 1;
                    setsockopt(conn_fd, IPPROTO_TCP, TCP_NODELAY,
                               &nodelay, sizeof(nodelay));

                    /* Allocate per-client state */
                    struct client_state *c = alloc_client(conn_fd);
                    if (c == NULL) {
                        fprintf(stderr, "[WARN] client table full\n");
                        close(conn_fd);
                        continue;
                    }

                    /* Add to epoll */
                    ev.events  = EPOLLIN | EPOLLET;
                    ev.data.fd = conn_fd;
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, conn_fd, &ev) == -1) {
                        perror("epoll_ctl(conn_fd)");
                        close(conn_fd);
                        free_client(c);
                    } else {
                        /* Log the new connection */
                        char host[NI_MAXHOST], svc[NI_MAXSERV];
                        getnameinfo((struct sockaddr *)&peer_addr, peer_len,
                                    host, sizeof(host), svc, sizeof(svc),
                                    NI_NUMERICHOST | NI_NUMERICSERV);
                        fprintf(stdout,
                                "[INFO] New client fd=%d from %s:%s\n",
                                conn_fd, host, svc);
                    }
                }
                continue;
            }

            /* ── Data or error on an existing client socket ── */
            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                /* Client disconnected or error */
                struct client_state *c = get_client(fd);
                if (c) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                    free_client(c);
                }
                continue;
            }

            if (events[i].events & EPOLLIN) {
                struct client_state *c = get_client(fd);
                if (c) handle_client_data(epfd, c);
            }
        }
    }

    /* ── Cleanup ── */
    fprintf(stdout, "\n[INFO] Server shutting down...\n");
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (g_clients[i].in_use) close(g_clients[i].fd);
    close(listen_fd);
    close(epfd);
    kv_store_destroy(&g_store);
    fprintf(stdout, "[INFO] Done.\n");
    return EXIT_SUCCESS;
}
