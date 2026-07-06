/*
 * net_utils.h - Low-level TCP send/receive helpers
 * Provides reliable full-message read/write over a TCP stream,
 * plus a small logging macro used throughout the project.
 */

#ifndef NET_UTILS_H
#define NET_UTILS_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include "protocol.h"

/* ─── Logging macro ─────────────────────────────────────────────────────── */
/*
 * LOG(fmt, ...) prints a timestamped line to stdout.
 * Usage: LOG("connected worker %d", id);
 */
/* Use __VA_OPT__ when available (C23/GNU C); fall back to ## trick.        */
#define LOG(fmt, ...) \
    do { \
        time_t _t = time(NULL); \
        struct tm *_tm = localtime(&_t); \
        char _buf[20]; \
        strftime(_buf, sizeof(_buf), "%H:%M:%S", _tm); \
        printf("[%s] " fmt "\n", _buf, ##__VA_ARGS__); \
        fflush(stdout); \
    } while (0)

/* ─── Function prototypes ───────────────────────────────────────────────── */

/*
 * send_all() - write exactly `len` bytes to fd, retrying on EINTR/short write.
 * Returns 0 on success, -1 on error.
 */
int send_all(int fd, const void *buf, size_t len);

/*
 * recv_all() - read exactly `len` bytes from fd, blocking until done.
 * Returns 0 on success, -1 on peer close or error.
 */
int recv_all(int fd, void *buf, size_t len);

/*
 * send_msg() - serialize and send a PktHeader + optional payload.
 * `payload` may be NULL when payload_len == 0.
 * Returns 0 on success, -1 on error.
 */
int send_msg(int fd, MsgType type, TaskType task, uint16_t seq,
             uint16_t worker_id, uint16_t chunk_id,
             const void *payload, uint32_t payload_len);

/*
 * recv_msg() - receive one complete message (header + payload).
 * `out_hdr`     : filled with the decoded header.
 * `payload_buf` : caller-supplied buffer of at least `buf_size` bytes.
 * Returns 0 on success, -1 on error/closed connection.
 */
int recv_msg(int fd, PktHeader *out_hdr, void *payload_buf, size_t buf_size);

/*
 * set_nonblocking() - put fd into O_NONBLOCK mode.
 * Returns 0 on success, -1 on error.
 */
int set_nonblocking(int fd);

/*
 * set_socket_timeout() - set SO_RCVTIMEO and SO_SNDTIMEO on fd.
 * `secs` : whole seconds for the timeout.
 */
void set_socket_timeout(int fd, int secs);

#endif /* NET_UTILS_H */

