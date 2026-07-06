/*
 * net_utils.c - Implementation of TCP helper functions declared in net_utils.h
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "net_utils.h"
#include "protocol.h"

/* ─── send_all ──────────────────────────────────────────────────────────── */
/*
 * Loops until all `len` bytes are written, handling EINTR and short writes.
 */
int send_all(int fd, const void *buf, size_t len)
{
    const char *ptr = (const char *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t n = send(fd, ptr, remaining, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;  /* interrupted – retry */
            perror("send_all: send");
            return -1;
        }
        if (n == 0) {
            /* Peer closed connection */
            return -1;
        }
        ptr       += n;
        remaining -= (size_t)n;
    }
    return 0;
}

/* ─── recv_all ──────────────────────────────────────────────────────────── */
/*
 * Loops until all `len` bytes are read, handling EINTR.
 */
int recv_all(int fd, void *buf, size_t len)
{
    char  *ptr       = (char *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t n = recv(fd, ptr, remaining, 0);
        if (n < 0) {
            if (errno == EINTR) continue;  /* interrupted – retry */
            /* Don't print error on EAGAIN; caller handles non-blocking fds */
            return -1;
        }
        if (n == 0) {
            /* Peer closed connection cleanly */
            return -1;
        }
        ptr       += n;
        remaining -= (size_t)n;
    }
    return 0;
}

/* ─── send_msg ──────────────────────────────────────────────────────────── */
/*
 * Builds a PktHeader, converts multi-byte fields to network byte order,
 * then sends header + payload in one logical write sequence.
 */
int send_msg(int fd, MsgType type, TaskType task, uint16_t seq,
             uint16_t worker_id, uint16_t chunk_id,
             const void *payload, uint32_t payload_len)
{
    PktHeader hdr;
    memset(&hdr, 0, sizeof(hdr));

    /* Fill header fields (convert to network byte order) */
    hdr.magic       = htonl(MAGIC_NUMBER);
    hdr.msg_type    = (uint8_t)type;
    hdr.task_type   = (uint8_t)task;
    hdr.seq_num     = htons(seq);
    hdr.worker_id   = htons(worker_id);
    hdr.chunk_id    = htons(chunk_id);
    hdr.payload_len = htonl(payload_len);

    /* Send the fixed-size header first */
    if (send_all(fd, &hdr, sizeof(hdr)) < 0) {
        fprintf(stderr, "send_msg: failed to send header\n");
        return -1;
    }

    /* Send optional payload */
    if (payload != NULL && payload_len > 0) {
        if (send_all(fd, payload, payload_len) < 0) {
            fprintf(stderr, "send_msg: failed to send payload\n");
            return -1;
        }
    }
    return 0;
}

/* ─── recv_msg ──────────────────────────────────────────────────────────── */
/*
 * Reads one complete message:
 *   1. Read the fixed-size PktHeader.
 *   2. Validate the magic number.
 *   3. Convert fields back to host byte order.
 *   4. Read payload_len bytes into payload_buf.
 */
int recv_msg(int fd, PktHeader *out_hdr, void *payload_buf, size_t buf_size)
{
    PktHeader hdr;

    /* Step 1: receive raw header bytes */
    if (recv_all(fd, &hdr, sizeof(hdr)) < 0) {
        return -1;  /* connection closed or error */
    }

    /* Step 2: validate magic */
    if (ntohl(hdr.magic) != MAGIC_NUMBER) {
        fprintf(stderr, "recv_msg: bad magic 0x%08X\n", ntohl(hdr.magic));
        return -1;
    }

    /* Step 3: decode multi-byte fields */
    out_hdr->magic       = MAGIC_NUMBER;
    out_hdr->msg_type    = hdr.msg_type;
    out_hdr->task_type   = hdr.task_type;
    out_hdr->seq_num     = ntohs(hdr.seq_num);
    out_hdr->worker_id   = ntohs(hdr.worker_id);
    out_hdr->chunk_id    = ntohs(hdr.chunk_id);
    out_hdr->payload_len = ntohl(hdr.payload_len);

    /* Step 4: receive payload if present */
    uint32_t plen = out_hdr->payload_len;
    if (plen > 0) {
        if (plen > buf_size) {
            fprintf(stderr, "recv_msg: payload %u exceeds buffer %zu\n",
                    plen, buf_size);
            return -1;
        }
        if (payload_buf == NULL) {
            fprintf(stderr, "recv_msg: payload_buf is NULL but plen=%u\n", plen);
            return -1;
        }
        if (recv_all(fd, payload_buf, plen) < 0) {
            return -1;
        }
    }
    return 0;
}

/* ─── set_nonblocking ───────────────────────────────────────────────────── */
int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        perror("set_nonblocking: fcntl F_GETFL");
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("set_nonblocking: fcntl F_SETFL");
        return -1;
    }
    return 0;
}

/* ─── set_socket_timeout ────────────────────────────────────────────────── */
void set_socket_timeout(int fd, int secs)
{
    struct timeval tv;
    tv.tv_sec  = secs;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

