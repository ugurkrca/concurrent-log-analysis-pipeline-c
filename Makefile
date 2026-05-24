CC=gcc
CFLAGS=-std=gnu11 -Wall -Wextra -Werror -Wno-deprecated-declarations -pthread
TARGET=analyzer
SRCS=main.c reader.c dispatcher.c analyzer.c aggregator.c shm.c watchdog.c

all:
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS)

clean:
	rm -f $(TARGET) *.o
