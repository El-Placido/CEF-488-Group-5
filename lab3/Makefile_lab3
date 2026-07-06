# Makefile — Lab 3: Advanced Socket Programming & Protocol Design
#
# Targets:
#   all            — build everything
#   server_tcp     — build only the TCP server
#   server_udp     — build only the UDP server
#   client         — build only the client
#   clean          — remove all compiled files
#
# Compiler flags:
#   -std=c11        — C11 standard
#   -Wall -Wextra   — enable most warnings
#   -Werror         — treat warnings as errors
#   -g              — include debug symbols (for gdb)
#   -D_POSIX_C_SOURCE=200809L  — POSIX 2008 API (sigaction, etc.)
#   -D_DEFAULT_SOURCE          — BSD/glibc extensions (flock, etc.)

CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Werror -g \
           -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE

# Source files shared between server components
STORE_SRC = kv_store.c
STORE_OBJ = kv_store.o

.PHONY: all clean server_tcp server_udp client

all: server_tcp server_udp client

# ── TCP Server ────────────────────────────────────────────────────────
server_tcp: kv_server_tcp.o $(STORE_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

kv_server_tcp.o: kv_server_tcp.c kv_protocol.h kv_store.h
	$(CC) $(CFLAGS) -c -o $@ $<

# ── UDP Server ────────────────────────────────────────────────────────
server_udp: kv_server_udp.o $(STORE_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

kv_server_udp.o: kv_server_udp.c kv_protocol.h kv_store.h
	$(CC) $(CFLAGS) -c -o $@ $<

# ── Client ────────────────────────────────────────────────────────────
client: kv_client.o
	$(CC) $(CFLAGS) -o $@ $^

kv_client.o: kv_client.c kv_protocol.h
	$(CC) $(CFLAGS) -c -o $@ $<

# ── Shared object ─────────────────────────────────────────────────────
$(STORE_OBJ): $(STORE_SRC) kv_store.h
	$(CC) $(CFLAGS) -c -o $@ $<

# ── Clean ─────────────────────────────────────────────────────────────
clean:
	rm -f *.o server_tcp server_udp client
