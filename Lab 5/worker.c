// worker.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>

#include "protocol.h"
#include "mandelbrot.h"

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <master_host> <port>\n", argv[0]);
        return 1;
    }
    const char *host = argv[1];
    const char *port = argv[2];

    /* -- Resolve master address (protocol-agnostic) ------- */
    struct addrinfo hints = {
        .ai_family   = AF_UNSPEC,    /* IPv4 or IPv6  */
        .ai_socktype = SOCK_STREAM
    };
    struct addrinfo *res;
    int err = getaddrinfo(host, port, &hints, &res);
    if (err) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return 1;
    }

    /* Try each returned address until one connects */
    int sock = -1;
    for (struct addrinfo *rp = res; rp; rp = rp->ai_next) {
        sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock < 0) continue;
        if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(sock); sock = -1;
    }
    freeaddrinfo(res);

    if (sock < 0) {
        fprintf(stderr, "[worker] could not connect to %s:%s\n", host, port);
        return 1;
    }
    printf("[worker] connected to master %s:%s\n", host, port);

    /* -- Main work loop ----------------------------------- */
    for (;;) {
        /* Read the next message header from master */
        msg_header_t hdr;
        ssize_t r = recv_all(sock, &hdr, sizeof hdr);
        if (r <= 0) {
            printf("[worker] master closed connection, exiting\n");
            break;
        }

        uint32_t op = ntohl(hdr.opcode);

        if (op == OP_TERMINATE) {
            printf("[worker] received TERMINATE, exiting cleanly\n");
            break;
        }

        if (op != OP_TASK) {
            fprintf(stderr, "[worker] unexpected opcode %u\n", op);
            break;
        }

        /* -- Receive task payload ------------------------- */
        task_payload_t task;
        if (recv_all(sock, &task, sizeof task) != (ssize_t)sizeof task) {
            perror("recv task payload"); break;
        }

        /* Deserialize integers */
        uint32_t width     = ntohl(task.width);
        uint32_t height    = ntohl(task.height);
        uint32_t start_row = ntohl(task.start_row);
        uint32_t num_rows  = ntohl(task.num_rows);
        uint32_t max_iter  = ntohl(task.max_iter);

        /* Deserialize doubles (bit-cast from network-order uint64) */
        double center_x = net_to_double(task.center_x_bits);
        double center_y = net_to_double(task.center_y_bits);
        double zoom     = net_to_double(task.zoom_bits);

        printf("[worker] computing rows %u–%u of %ux%u image\n",
               start_row, start_row + num_rows - 1, width, height);

        /* -- Compute the assigned rows -------------------- */
        unsigned char *pixels = malloc(num_rows * width);
        if (!pixels) { perror("malloc"); break; }

        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        for (uint32_t i = 0; i < num_rows; i++)
            compute_row(pixels + i * width,
                        (int)(start_row + i),
                        (int)width, (int)height,
                        center_x, center_y, zoom, (int)max_iter);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        double elapsed = (t1.tv_sec - t0.tv_sec) +
                         (t1.tv_nsec - t0.tv_nsec) / 1e9;
        printf("[worker] computed %u rows in %.3fs\n", num_rows, elapsed);

        /* -- Send result back to master ------------------- */
        if (send_result(sock, start_row, num_rows, width, pixels) < 0) {
            perror("send_result"); free(pixels); break;
        }
        free(pixels);
        printf("[worker] sent result for rows %u–%u\n",
               start_row, start_row + num_rows - 1);
    }

    close(sock);
    return 0;
}
