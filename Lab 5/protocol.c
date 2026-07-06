// protocol.c
#include "protocol.h"
#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

/* -- Reliable I/O ----------------------------------------- */

ssize_t send_all(int fd, const void *buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t s = send(fd, (const char*)buf + sent, n - sent, MSG_NOSIGNAL);
        if (s < 0) return -1;
        sent += (size_t)s;
    }
    return (ssize_t)sent;
}

ssize_t recv_all(int fd, void *buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = recv(fd, (char*)buf + got, n - got, 0);
        if (r == 0) return (ssize_t)got;  /* clean EOF  */
        if (r < 0)  return -1;
        got += (size_t)r;
    }
    return (ssize_t)got;
}

/* -- Double ? uint64 (IEEE 754 bit-cast) ----------------- */
uint64_t double_to_net(double d) {
    uint64_t u;
    memcpy(&u, &d, 8);
    /* htobe64 converts host ? big-endian 64-bit */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    /* manual byte-swap if htobe64 unavailable */
    u = ((u & 0x00000000000000FFULL) << 56) |
        ((u & 0x000000000000FF00ULL) << 40) |
        ((u & 0x0000000000FF0000ULL) << 24) |
        ((u & 0x00000000FF000000ULL) <<  8) |
        ((u & 0x000000FF00000000ULL) >>  8) |
        ((u & 0x0000FF0000000000ULL) >> 24) |
        ((u & 0x00FF000000000000ULL) >> 40) |
        ((u & 0xFF00000000000000ULL) >> 56);
#endif
    return u;
}

double net_to_double(uint64_t u) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    u = ((u & 0x00000000000000FFULL) << 56) |
        ((u & 0x000000000000FF00ULL) << 40) |
        ((u & 0x0000000000FF0000ULL) << 24) |
        ((u & 0x00000000FF000000ULL) <<  8) |
        ((u & 0x000000FF00000000ULL) >>  8) |
        ((u & 0x0000FF0000000000ULL) >> 24) |
        ((u & 0x00FF000000000000ULL) >> 40) |
        ((u & 0xFF00000000000000ULL) >> 56);
#endif
    double d;
    memcpy(&d, &u, 8);
    return d;
}

/* -- OP_TASK ---------------------------------------------- */
int send_task(int sock,
              uint32_t width, uint32_t height,
              uint32_t start_row, uint32_t num_rows,
              uint32_t max_iter,
              double center_x, double center_y, double zoom) {

    msg_header_t hdr = {
        .opcode      = htonl(OP_TASK),
        .payload_len = htonl(sizeof(task_payload_t))
    };
    task_payload_t body = {
        .width         = htonl(width),
        .height        = htonl(height),
        .start_row     = htonl(start_row),
        .num_rows      = htonl(num_rows),
        .max_iter      = htonl(max_iter),
        .center_x_bits = double_to_net(center_x),
        .center_y_bits = double_to_net(center_y),
        .zoom_bits     = double_to_net(zoom)
    };

    if (send_all(sock, &hdr,  sizeof hdr)  < 0) return -1;
    if (send_all(sock, &body, sizeof body) < 0) return -1;
    return 0;
}

int recv_task(int sock, task_payload_t *out) {
    msg_header_t hdr;
    if (recv_all(sock, &hdr, sizeof hdr) != sizeof hdr) return -1;
    if (ntohl(hdr.opcode) == OP_TERMINATE) return 1; /* sentinel */
    if (ntohl(hdr.opcode) != OP_TASK)     return -1;

    if (recv_all(sock, out, sizeof *out) != (ssize_t)sizeof *out) return -1;

    /* Convert from network byte order */
    out->width     = ntohl(out->width);
    out->height    = ntohl(out->height);
    out->start_row = ntohl(out->start_row);
    out->num_rows  = ntohl(out->num_rows);
    out->max_iter  = ntohl(out->max_iter);
    out->center_x_bits = net_to_double(out->center_x_bits); /* reuse field as double below */
    /* NOTE: caller should cast out->center_x_bits with memcpy to double */
    return 0;
}

/* -- OP_RESULT -------------------------------------------- */
int send_result(int sock,
                uint32_t start_row, uint32_t num_rows,
                uint32_t width, const unsigned char *pixels) {

    uint32_t pixel_bytes = num_rows * width;

    msg_header_t hdr = {
        .opcode      = htonl(OP_RESULT),
        .payload_len = htonl(sizeof(result_payload_t) + pixel_bytes)
    };
    result_payload_t body = {
        .start_row = htonl(start_row),
        .num_rows  = htonl(num_rows),
        .width     = htonl(width)
    };

    if (send_all(sock, &hdr,   sizeof hdr)   < 0) return -1;
    if (send_all(sock, &body,  sizeof body)  < 0) return -1;
    if (send_all(sock, pixels, pixel_bytes)  < 0) return -1;
    return 0;
}

int recv_result(int sock, result_payload_t *hdr_out, unsigned char **pixels_out) {
    msg_header_t hdr;
    if (recv_all(sock, &hdr, sizeof hdr) != sizeof hdr) return -1;
    if (ntohl(hdr.opcode) != OP_RESULT) return -1;

    result_payload_t body;
    if (recv_all(sock, &body, sizeof body) != (ssize_t)sizeof body) return -1;

    body.start_row = ntohl(body.start_row);
    body.num_rows  = ntohl(body.num_rows);
    body.width     = ntohl(body.width);
    *hdr_out = body;

    uint32_t pixel_bytes = body.num_rows * body.width;
    *pixels_out = malloc(pixel_bytes);
    if (!*pixels_out) return -1;

    if (recv_all(sock, *pixels_out, pixel_bytes) != (ssize_t)pixel_bytes) {
        free(*pixels_out); *pixels_out = NULL;
        return -1;
    }
    return 0;
}

/* -- OP_TERMINATE ----------------------------------------- */
int send_terminate(int sock) {
    msg_header_t hdr = {
        .opcode      = htonl(OP_TERMINATE),
        .payload_len = 0
    };
    return (send_all(sock, &hdr, sizeof hdr) < 0) ? -1 : 0;
}

/* -- OP_REQUEST (dynamic load balancing) ------------------ */
int send_request(int sock) {
    msg_header_t hdr = {
        .opcode      = htonl(OP_REQUEST),
        .payload_len = 0
    };
    return (send_all(sock, &hdr, sizeof hdr) < 0) ? -1 : 0;
}
