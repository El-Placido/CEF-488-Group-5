// mandelbrot.c
#include "mandelbrot.h"
#include <stdio.h>
#include <stdlib.h>

void compute_row(unsigned char *pixels,
                 int row, int width, int height,
                 double center_x, double center_y,
                 double zoom, int max_iter) {

    for (int col = 0; col < width; col++) {
        /* Map pixel (col, row) to complex plane coordinates.
         * zoom represents the number of units per pixel.
         * The center of the image maps to (center_x, center_y).
         */
        double cx = center_x + (col - width  / 2.0) * zoom;
        double cy = center_y - (row - height / 2.0) * zoom;

        /* Mandelbrot iteration: z_{n+1} = z_n^2 + c, z_0 = 0 */
        double zx = 0.0, zy = 0.0;
        int iter = 0;

        while (iter < max_iter && (zx*zx + zy*zy) <= 4.0) {
            double tmp = zx*zx - zy*zy + cx;  /* Re(z^2 + c) */
            zy = 2.0 * zx * zy + cy;          /* Im(z^2 + c) */
            zx = tmp;
            iter++;
        }

        /* Map iteration count to grayscale 0–255.
         * Points in the set (hit max_iter) ? 0 (black).
         * Points that escaped quickly ? brighter.
         */
        pixels[col] = (iter == max_iter)
                      ? 0
                      : (unsigned char)(255 * iter / max_iter);
    }
}

void write_ppm(const char *filename,
               const unsigned char *pixels,
               int width, int height) {

    FILE *f = fopen(filename, "wb");
    if (!f) { perror("fopen"); return; }

    /* PPM P6 header: binary RGB.
     * We have grayscale so R=G=B for each pixel. */
    fprintf(f, "P6\n%d %d\n255\n", width, height);

    for (int i = 0; i < width * height; i++) {
        unsigned char rgb[3] = {pixels[i], pixels[i], pixels[i]};
        fwrite(rgb, 1, 3, f);
    }
    fclose(f);
    printf("Written: %s (%dx%d)\n", filename, width, height);
}
