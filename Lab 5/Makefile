CC = gcc
CFLAGS = -Wall -Wextra -Werror -O2 -std=c11 -D_POSIX_C_SOURCE=200809L

all: master worker test_local

master: master.o protocol.o mandelbrot.o
	$(CC) $(CFLAGS) -o master master.o protocol.o mandelbrot.o

worker: worker.o protocol.o mandelbrot.o
	$(CC) $(CFLAGS) -o worker worker.o protocol.o mandelbrot.o

test_local: test_local.o mandelbrot.o
	$(CC) $(CFLAGS) -o test_local test_local.o mandelbrot.o

master.o: master.c protocol.h mandelbrot.h
	$(CC) $(CFLAGS) -c master.c

worker.o: worker.c protocol.h mandelbrot.h
	$(CC) $(CFLAGS) -c worker.c

test_local.o: test_local.c mandelbrot.h
	$(CC) $(CFLAGS) -c test_local.c

protocol.o: protocol.c protocol.h
	$(CC) $(CFLAGS) -c protocol.c

mandelbrot.o: mandelbrot.c mandelbrot.h
	$(CC) $(CFLAGS) -c mandelbrot.c

clean:
	rm -f *.o master worker test_local output.ppm local_test.ppm

.PHONY: all clean
