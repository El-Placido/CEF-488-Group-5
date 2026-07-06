// master.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <time.h>

#include "protocol.h"
#include "mandelbrot.h"

#define MAX_WORKERS   3
#define TASK_TIMEOUT  10   /* seconds before declaring worker dead */

/* -- Per-worker state ------------------------------------- */
typedef enum {
    WS_IDLE,       /* connected, waiting for a task     */
    WS_BUSY,       /* computing a row range             */
    WS_DEAD        /* timed out / disconnected          */
} worker_state_t;

typedef struct {
    int            sock;
    worker_state_t state;
    uint32_t       start_row;   /* rows this worker is currently computing */
    uint32_t       num_rows;
    time_t         sent_at;     /* when task was sent (for timeout checks) */
} worker_t;

/* -- Chunk queue (dynamic load balancing) ----------------- */
typedef struct chunk {
    uint32_t start_row;
    uint32_t num_rows;
    struct chunk *next;
} chunk_t;

static chunk_t *queue_head = NULL;
static chunk_t *queue_tail = NULL;

static void enqueue(uint32_t start, uint32_t num) {
    chunk_t *c = malloc(sizeof *c);
    c->start_row = start; c->num_rows = num; c->next = NULL;
    if (queue_tail) queue_tail->next = c; else queue_head = c;
    queue_tail = c;
}

static int dequeue(uint32_t *start, uint32_t *num) {
    if (!queue_head) return 0;
    chunk_t *c = queue_head;
    queue_head = c->next;
    if (!queue_head) queue_tail = NULL;
    *start = c->start_row; *num = c->num_rows;
    free(c);
    return 1;
}

/* -- Globals ---------------------------------------------- */
static volatile int g_running = 1;

static void handle_sigint(int sig) { (void)sig; g_running = 0; }

/* -- Assign next chunk to a worker ----------------------- */
static int assign_work(worker_t *w,
                       uint32_t W, uint32_t H,
                       uint32_t max_iter,
                       double cx, double cy, double zoom) {
    uint32_t sr, nr;
    if (!dequeue(&sr, &nr)) return 0; /* no work left */

    if (send_task(w->sock, W, H, sr, nr, max_iter, cx, cy, zoom) < 0) {
        perror("send_task"); return -1;
    }
    w->state     = WS_BUSY;
    w->start_row = sr;
    w->num_rows  = nr;
    w->sent_at   = time(NULL);
    printf("[master] assigned rows %u–%u to worker fd=%d\n",
           sr, sr + nr - 1, w->sock);
    return 1;
}

/* -- Main ------------------------------------------------- */
int main(int argc, char *argv[]) {
    if (argc < 6) {
        fprintf(stderr,
            "Usage: %s <port> <width> <height> <max_iter> <chunk_rows>\n"
            "  e.g: %s 9000 800 600 256 20\n", argv[0], argv[0]);
        return 1;
    }
    int      port       = atoi(argv[1]);
    uint32_t W          = (uint32_t)atoi(argv[2]);
    uint32_t H          = (uint32_t)atoi(argv[3]);
    uint32_t max_iter   = (uint32_t)atoi(argv[4]);
    uint32_t chunk_rows = (uint32_t)atoi(argv[5]);

    double center_x = -0.5, center_y = 0.0, zoom = 4.0 / H;

    /* -- Image buffer ------------------------------------- */
    unsigned char *image = calloc(W * H, 1);
    if (!image) { perror("calloc"); return 1; }

    /* -- Build the initial work queue -------------------- */
    for (uint32_t row = 0; row < H; row += chunk_rows)
        enqueue(row, (row + chunk_rows <= H) ? chunk_rows : H - row);

    uint32_t total_rows_received = 0;

    /* -- Signal handling ---------------------------------- */
    struct sigaction sa = { .sa_handler = handle_sigint };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);  /* don't crash on broken pipe */

    /* -- Listening socket --------------------------------- */
    int lsock = socket(AF_INET6, SOCK_STREAM, 0);
    if (lsock < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);

    /* IPV6_V6ONLY = 0 ? dual-stack: accept IPv4 and IPv6 */
    int v6only = 0;
    setsockopt(lsock, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof v6only);

    struct sockaddr_in6 addr = {
        .sin6_family = AF_INET6,
        .sin6_port   = htons((uint16_t)port),
        .sin6_addr   = in6addr_any
    };
    if (bind(lsock, (struct sockaddr*)&addr, sizeof addr) < 0) {
        perror("bind"); return 1;
    }
    listen(lsock, 10);
    printf("[master] listening on port %d, image %ux%u, %u rows/chunk\n",
           port, W, H, chunk_rows);
    printf("[master] waiting 10 seconds for workers to connect...\n");
    sleep(10);
    printf("[master] starting work distribution\n");
    
    printf("[master] starting work distribution\n");
 	struct timespec t_start, t_end;
	clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* -- epoll setup -------------------------------------- */
    int epfd = epoll_create1(0);
    struct epoll_event ev = { .events = EPOLLIN, .data.fd = lsock };
    epoll_ctl(epfd, EPOLL_CTL_ADD, lsock, &ev);

    /* -- Worker table ------------------------------------- */
    worker_t workers[MAX_WORKERS];
    memset(workers, 0, sizeof workers);
    for (int i = 0; i < MAX_WORKERS; i++) workers[i].sock = -1;
    int num_connected = 0;

    /* -- Event loop --------------------------------------- */
    struct epoll_event events[16];

    while (g_running && total_rows_received < H) {
        /* Use a 1-second timeout so we can check timeouts each second */
        int n = epoll_wait(epfd, events, 16, 1000);

        /* -- Timeout check for busy workers ---------------- */
        time_t now = time(NULL);
        for (int i = 0; i < MAX_WORKERS; i++) {
            worker_t *w = &workers[i];
            if (w->state == WS_BUSY &&
                (now - w->sent_at) > TASK_TIMEOUT) {

                fprintf(stderr,
                    "[master] TIMEOUT worker fd=%d (rows %u–%u), "
                    "reassigning\n",
                    w->sock, w->start_row, w->start_row + w->num_rows - 1);

                /* Re-enqueue the chunk at the front */
                enqueue(w->start_row, w->num_rows);

                epoll_ctl(epfd, EPOLL_CTL_DEL, w->sock, NULL);
                close(w->sock);
                w->sock  = -1;
                w->state = WS_DEAD;
                num_connected--;
            }
        }

        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait"); break;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            /* -- New worker connection -------------------- */
            if (fd == lsock) {
                struct sockaddr_in6 caddr;
                socklen_t clen = sizeof caddr;
                int csock = accept(lsock,
                                   (struct sockaddr*)&caddr, &clen);
                if (csock < 0) { perror("accept"); continue; }

                if (num_connected >= MAX_WORKERS) {
                    fprintf(stderr, "[master] too many workers, rejecting\n");
                    close(csock);
                    continue;
                }

                /* Find a free slot */
                int slot = -1;
                for (int j = 0; j < MAX_WORKERS; j++)
                    if (workers[j].sock < 0) { slot = j; break; }

                workers[slot].sock  = csock;
                workers[slot].state = WS_IDLE;
                num_connected++;

                struct epoll_event wev = {
                    .events   = EPOLLIN,
                    .data.fd  = csock
                };
                epoll_ctl(epfd, EPOLL_CTL_ADD, csock, &wev);
                printf("[master] worker %d connected (fd=%d)\n", slot, csock);

                /* Immediately assign work if available */
                assign_work(&workers[slot],
                            W, H, max_iter, center_x, center_y, zoom);
                continue;
            }

            /* -- Data from a worker ----------------------- */
            /* Find which worker this fd belongs to */
            int slot = -1;
            for (int j = 0; j < MAX_WORKERS; j++)
                if (workers[j].sock == fd) { slot = j; break; }
            if (slot < 0) continue;

            worker_t *w = &workers[slot];

            result_payload_t rhdr;
            unsigned char    *pixels = NULL;

            if (recv_result(fd, &rhdr, &pixels) < 0) {
                /* Worker disconnected or error */
                fprintf(stderr, "[master] worker fd=%d disconnected\n", fd);
                if (w->state == WS_BUSY)
                    enqueue(w->start_row, w->num_rows); /* reassign */
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                close(fd);
                w->sock  = -1;
                w->state = WS_DEAD;
                num_connected--;
                continue;
            }

            /* Copy pixels into the master image buffer */
            memcpy(image + rhdr.start_row * W,
                   pixels,
                   rhdr.num_rows * W);
            free(pixels);

            total_rows_received += rhdr.num_rows;
            printf("[master] received rows %u–%u (%u/%u total)\n",
                   rhdr.start_row,
                   rhdr.start_row + rhdr.num_rows - 1,
                   total_rows_received, H);

            w->state = WS_IDLE;

            /* Give this worker the next chunk, or terminate it */
            int res = assign_work(w, W, H, max_iter, center_x, center_y, zoom);
            if (res == 0) {
                /* No more work — send terminate */
                send_terminate(w->sock);
                epoll_ctl(epfd, EPOLL_CTL_DEL, w->sock, NULL);
                close(w->sock);
                w->sock  = -1;
                w->state = WS_IDLE;
                num_connected--;
                printf("[master] worker slot %d terminated\n", slot);
            }
        }
    }

	clock_gettime(CLOCK_MONOTONIC, &t_end);
	double elapsed = (t_end.tv_sec - t_start.tv_sec) +
                 (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
	printf("[master] computation completed in %.3f seconds\n", elapsed);

    /* -- Write output image ------------------------------- */
    if (total_rows_received == H) {
        write_ppm("output.ppm", image, W, H);
        printf("[master] done! Wrote output.ppm\n");
    } else {
        fprintf(stderr, "[master] incomplete: only %u/%u rows received\n",
                total_rows_received, H);
    }

    free(image);
    close(lsock);
    close(epfd);
    return 0;
}
