CC = gcc
CFLAGS = -Wall -Wextra -Werror -std=c11 -pedantic

TARGET = lab1_daemon
OBJS = lab1_daemon.o

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

lab1_daemon.o: lab1_daemon.c lab1_common.h
	$(CC) $(CFLAGS) -c lab1_daemon.c

clean:
	rm -f $(TARGET) $(OBJS)