CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -g -Iinclude
LDFLAGS =

SRC_COMMON = src/udp_utils.c src/epoll_utils.c

all: coordinator worker client

coordinator: $(SRC_COMMON) src/coordinator.c
	$(CC) $(CFLAGS) -o coordinator $(SRC_COMMON) src/coordinator.c $(LDFLAGS)

worker: $(SRC_COMMON) src/worker.c
	$(CC) $(CFLAGS) -o worker $(SRC_COMMON) src/worker.c $(LDFLAGS)

client: $(SRC_COMMON) src/client.c
	$(CC) $(CFLAGS) -o client $(SRC_COMMON) src/client.c $(LDFLAGS)

clean:
	rm -f coordinator worker client

.PHONY: all clean
