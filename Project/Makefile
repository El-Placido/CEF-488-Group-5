# Makefile - Distributed Data Processing System
# Targets: all, coordinator, worker, clean, test_data

CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=gnu11 -I./include -D_GNU_SOURCE -g -O2
LDFLAGS =

COORD_SRCS  = src/coordinator.c src/net_utils.c
WORKER_SRCS = src/worker.c      src/net_utils.c

COORD_BIN  = coordinator
WORKER_BIN = worker

.PHONY: all clean test_data

all: $(COORD_BIN) $(WORKER_BIN)
	@echo ""
	@echo "Build complete. Binaries: ./$(COORD_BIN)  ./$(WORKER_BIN)"

$(COORD_BIN): $(COORD_SRCS) include/protocol.h include/net_utils.h
	$(CC) $(CFLAGS) -o $@ $(COORD_SRCS) $(LDFLAGS)
	@echo "  LD  $@"

$(WORKER_BIN): $(WORKER_SRCS) include/protocol.h include/net_utils.h
	$(CC) $(CFLAGS) -o $@ $(WORKER_SRCS) $(LDFLAGS)
	@echo "  LD  $@"

test_data:
	@mkdir -p data
	@python3 -c "\
import random; \
words = ['hello','world','foo','bar','baz','distributed','computing','socket','epoll','chunk','worker','coordinator','linux','network']; \
lines = [' '.join(random.choices(words, k=random.randint(5,15))) for _ in range(500)]; \
open('data/sample.txt','w').write('\n'.join(lines)+'\n')"
	@python3 -c "\
import random; \
nums = [str(random.randint(-1000,1000)) for _ in range(2000)]; \
lines = [' '.join(nums[i:i+10]) for i in range(0,len(nums),10)]; \
open('data/numbers.txt','w').write('\n'.join(lines)+'\n')"
	@echo "Test data ready:"
	@wc -w data/sample.txt
	@wc -l data/sample.txt

clean:
	rm -f $(COORD_BIN) $(WORKER_BIN)
	@echo "Cleaned."

