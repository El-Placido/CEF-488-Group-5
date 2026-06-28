# Distributed Data Processing System Makefile

CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -g -O2 -Iinclude
LDFLAGS =

COORD_SRC = coordinator.c
WORK_SRC  = worker.c
COORD_BIN = coordinator
WORK_BIN  = worker

.PHONY: all clean coordinator worker

all: coordinator worker

coordinator: $(COORD_SRC) protocol.h net_util.h
	$(CC) $(CFLAGS) $(COORD_SRC) -o $(COORD_BIN) $(LDFLAGS)
	@echo "Built: $(COORD_BIN)"

worker: $(WORK_SRC) protocol.h net_util.h
	$(CC) $(CFLAGS) $(WORK_SRC) -o $(WORK_BIN) $(LDFLAGS)
	@echo "Built: $(WORK_BIN)"

clean:
	rm -f $(COORD_BIN) $(WORK_BIN)
	@echo "Clean done."
