# =============================================================================
# Makefile — Project 4: Distributed Data Processing System
# CEF488 Group Project
# =============================================================================

CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Werror -g -O2 -D_POSIX_C_SOURCE=200809L
LDLIBS  =

.PHONY: all clean data test

all: coordinator worker

coordinator: coordinator.o tasks.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

worker: worker.o tasks.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

coordinator.o: coordinator.c protocol.h tasks.h
	$(CC) $(CFLAGS) -c -o $@ $<

worker.o: worker.c protocol.h tasks.h
	$(CC) $(CFLAGS) -c -o $@ $<

tasks.o: tasks.c tasks.h protocol.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Generate a sample dataset for testing: 5000 lines of text + numbers
data:
	mkdir -p data
	python3 -c "\
import random; \
words = ['the','quick','brown','fox','jumps','over','lazy','dog','distributed','systems','network','programming','cef488']; \
lines = []; \
[lines.append(' '.join(random.choice(words) for _ in range(random.randint(5,12)))) for _ in range(5000)]; \
open('data/sample.txt','w').write('\n'.join(lines) + '\n')"
	python3 -c "\
import random; \
nums = [str(random.randint(-1000,1000)) for _ in range(5000)]; \
open('data/numbers.txt','w').write(' '.join(nums) + '\n')"
	@echo "Generated data/sample.txt and data/numbers.txt"
	@wc -w data/sample.txt
	@wc -l data/sample.txt

# Self-contained single-machine test: coordinator + 3 workers in background
test: all data
	@echo ""
	@echo "=== Reference count (ground truth) ==="
	@wc -w data/sample.txt
	@echo ""
	@echo "=== Running distributed word count with 3 workers ==="
	./coordinator -f data/sample.txt -p 9100 -t wordcount -c 256 &
	CPID=$$!; \
	sleep 1; \
	./worker 127.0.0.1 9100 & \
	./worker 127.0.0.1 9100 & \
	./worker 127.0.0.1 9100 & \
	wait $$CPID

clean:
	rm -f coordinator worker *.o
	@echo "Cleaned."

distclean: clean
	rm -rf data
