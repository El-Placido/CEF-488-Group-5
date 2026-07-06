#include "mandelbrot.h"
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

int main(void) {
    int width = 3840, height = 2160, max_iter = 1000;
    double cx = -0.5, cy = 0.0, zoom = 0.004;

    unsigned char *pixels = malloc(width * height);
    if (!pixels) { perror("malloc"); return 1; }

    /* Start timing here — after malloc, before computation */
    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    for (int row = 0; row < height; row++)
        compute_row(pixels + row * width,
                    row, width, height,
                    cx, cy, zoom, max_iter);

    /* Stop timing here — after computation, before file write */
    clock_gettime(CLOCK_MONOTONIC, &t_end);

    double elapsed = (t_end.tv_sec  - t_start.tv_sec) +
                     (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    printf("Single machine computation time: %.3f seconds\n", elapsed);

    write_ppm("local_test.ppm", pixels, width, height);
    free(pixels);
    return 0;
}
