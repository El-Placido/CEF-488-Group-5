// mandelbrot.h
#ifndef MANDELBROT_H
#define MANDELBROT_H

#include <stdint.h>

/* Compute one row of the Mandelbrot set.
 *
 * pixels    : output array of 'width' bytes (0–255 grayscale)
 * row       : which row (y-coordinate in image space)
 * width     : image width in pixels
 * height    : image height in pixels (needed for y mapping)
 * center_x  : real part of the center of the view
 * center_y  : imaginary part of the center of the view
 * zoom      : scale factor (smaller = more zoomed in)
 * max_iter  : maximum iterations before declaring "in set"
 */
void compute_row(unsigned char *pixels,
                 int row, int width, int height,
                 double center_x, double center_y,
                 double zoom, int max_iter);

/* Write a PPM (Portable Pixmap) file from a grayscale pixel buffer.
 * pixels: row-major array of width*height bytes.
 */
void write_ppm(const char *filename,
               const unsigned char *pixels,
               int width, int height);

#endif
