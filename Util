#ifndef NET_UTIL_H
#define NET_UTIL_H

#include <stdint.h>
#include <stddef.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "protocol.h"

/* ─── Send a UDP datagram with simple retransmission ───────────────────────
 *
 *  sock        : already-bound/connected UDP socket fd
 *  buf         : complete message bytes (header + payload)
 *  len         : byte count
 *  dest        : destination address
 *  ack_type    : msg_type_t we expect back as acknowledgement
 *  ack_buf     : caller buffer to receive the ACK packet
 *  ack_buf_len : size of ack_buf
 *  from        : filled with the address of the ACK sender
 *
 *  Returns total bytes of the ACK packet, or -1 on permanent failure.
 */
static inline int udp_send_reliable(int sock,
                                    const uint8_t *buf, int len,
                                    const struct sockaddr_in *dest,
                                    msg_type_t ack_type,
                                    uint8_t *ack_buf, size_t ack_buf_len,
                                    struct sockaddr_in *from)
{
    struct timeval tv;
    tv.tv_sec  = RETRANSMIT_TIMEOUT;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    for (int attempt = 0; attempt < MAX_RETRANSMITS; attempt++) {

        /* Send the datagram */
        ssize_t sent = sendto(sock, buf, (size_t)len, 0,
                              (const struct sockaddr *)dest, sizeof(*dest));
        if (sent < 0) {
            perror("[net_util] sendto failed");
            return -1;
        }

        /* Wait for an ACK */
        socklen_t from_len = sizeof(*from);
        ssize_t rcvd = recvfrom(sock, ack_buf, ack_buf_len, 0,
                                (struct sockaddr *)from, &from_len);
        if (rcvd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                fprintf(stderr, "[net_util] timeout on attempt %d, retransmitting...\n",
                        attempt + 1);
                continue; /* retry */
            }
            perror("[net_util] recvfrom failed");
            return -1;
        }

        /* Validate the ACK type */
        msg_header_t hdr;
        if (parse_header(ack_buf, (size_t)rcvd, &hdr) == NULL) {
            fprintf(stderr, "[net_util] received malformed ACK, retrying\n");
            continue;
        }
        if ((msg_type_t)hdr.type == ack_type) {
            /* Restore blocking mode */
            tv.tv_sec = tv.tv_usec = 0;
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            return (int)rcvd;
        }
        fprintf(stderr, "[net_util] unexpected message type %d (want %d)\n",
                hdr.type, (int)ack_type);
    }

    fprintf(stderr, "[net_util] gave up after %d retransmits\n", MAX_RETRANSMITS);
    return -1;
}

/* ─── Send a fire-and-forget UDP datagram (no ACK expected) ────────────── */
static inline int udp_send_simple(int sock,
                                  const uint8_t *buf, int len,
                                  const struct sockaddr_in *dest)
{
    ssize_t sent = sendto(sock, buf, (size_t)len, 0,
                          (const struct sockaddr *)dest, sizeof(*dest));
    if (sent < 0) { perror("[net_util] sendto simple"); return -1; }
    return (int)sent;
}

/* ─── Create and bind a UDP socket to the given port ───────────────────── */
static inline int create_udp_socket(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    return fd;
}

/* ─── Timestamp helper: seconds since epoch ────────────────────────────── */
static inline time_t now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec;
}

#endif /* NET_UTIL_H */
