// protocol.h
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <sys/types.h>

/* -- Opcodes ----------------------------------------------- */
#define OP_TASK        1   /* master ? worker: here is a row range    */
#define OP_RESULT      2   /* worker ? master: here are pixels        */
#define OP_TERMINATE   3   /* master ? worker: you may exit           */
#define OP_REQUEST     4   /* worker ? master: give me work (dynamic) */

/* -- Wire structures (all integers in network byte order) -- */

/*
 * Fixed message header sent before every message body.
 * The receiver reads this 8-byte header first, then reads
 * exactly 'payload_len' more bytes for the body.
 */
typedef struct {
    uint32_t opcode;       /* OP_* constant         */
    uint32_t payload_len;  /* bytes that follow      */
} msg_header_t;            /* 8 bytes on the wire    */

/*
 * Payload for OP_TASK  (master ? worker).
 * Describes a horizontal band of the image to compute.
 */
typedef struct {
    uint32_t width;        /* image width  (pixels)  */
    uint32_t height;       /* image height (pixels)  */
    uint32_t start_row;    /* first row to compute   */
    uint32_t num_rows;     /* how many rows          */
    uint32_t max_iter;     /* max Mandelbrot iters   */
    uint64_t center_x_bits;/* double center_x as IEEE754 bits (htobe64) */
    uint64_t center_y_bits;
    uint64_t zoom_bits;
} task_payload_t;          /* fixed size; easy to recv_all             */

/*
 * Payload for OP_RESULT (worker ? master).
 * Followed immediately by start_row * width bytes of pixel data.
 */
typedef struct {
    uint32_t start_row;
    uint32_t num_rows;
    uint32_t width;
    /* pixel data follows: num_rows * width bytes */
} result_payload_t;

/* -- Reliable I/O helpers --------------------------------- */
ssize_t send_all(int fd, const void *buf, size_t n);
ssize_t recv_all(int fd, void       *buf, size_t n);

/* -- High-level send/recv --------------------------------- */
int send_task(int sock, uint32_t width, uint32_t height,
              uint32_t start_row, uint32_t num_rows,
              uint32_t max_iter,
              double center_x, double center_y, double zoom);

int recv_task(int sock, task_payload_t *out);

int send_result(int sock, uint32_t start_row, uint32_t num_rows,
                uint32_t width, const unsigned char *pixels);

int recv_result(int sock, result_payload_t *hdr, unsigned char **pixels);

int send_terminate(int sock);
int send_request(int sock);   /* dynamic: worker asks for work */

uint64_t double_to_net(double d);
double net_to_double(uint64_t u);

#endif
