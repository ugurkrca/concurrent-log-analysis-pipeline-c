#include "watchdog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>

static long elapsed_seconds_since(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long elapsed = now.tv_sec - start->tv_sec;
    if (now.tv_nsec < start->tv_nsec) elapsed--;
    return elapsed < 0 ? 0 : elapsed;
}

static int count_alive_children(const watchdog_args_t *args) {
    int alive = 0;
    for (int i = 0; i < args->num_children; i++) {
        if (args->child_pids[i] > 0 && (kill(args->child_pids[i], 0) == 0 || errno == EPERM)) alive++;
    }
    return alive;
}

static void print_snapshot(const watchdog_args_t *args, const int *counts, const struct timespec *start) {
    fprintf(stderr, "[WATCHDOG] Progress at T+%lds:", elapsed_seconds_since(start));
    for (int i = 0; i < args->num_readers; i++) fprintf(stderr, " Reader %d=%d", i, counts[i]);
    fprintf(stderr, " children_alive=%d\n", count_alive_children(args));
    fflush(stderr);
}

static void read_heartbeat_lines(const watchdog_args_t *args, int *counts, int index) {
    char buffer[4096];
    ssize_t n = read(args->pipe_read_fds[index], buffer, sizeof(buffer) - 1);
    if (n <= 0) return;
    buffer[n] = '\0';
    char *save = NULL;
    char *line = strtok_r(buffer, "\n", &save);
    while (line) {
        int id = -1;
        int lines = -1;
        char filename[256];
        if (sscanf(line, "[R%d] %d lines processed", &id, &lines) == 2 && id >= 0 && id < args->num_readers) {
            counts[id] = lines;
        } else if (sscanf(line, "[R%d:%255[^]]] %d lines processed", &id, filename, &lines) == 3 && id >= 0 && id < args->num_readers) {
            counts[id] = lines;
        }
        line = strtok_r(NULL, "\n", &save);
    }
}

void* watchdog_thread_func(void *arg) {
    watchdog_args_t *args = arg;
    int *counts = calloc((size_t)args->num_readers, sizeof(int));
    if (!counts) return NULL;

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    long last_snapshot = -1;
    int printed_any = 0;

    while (!*args->shutdown_flag) {
        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;
        for (int i = 0; i < args->num_readers; i++) {
            if (args->pipe_read_fds[i] >= 0) {
                FD_SET(args->pipe_read_fds[i], &rfds);
                if (args->pipe_read_fds[i] > maxfd) maxfd = args->pipe_read_fds[i];
            }
        }

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        int ret = maxfd >= 0 ? select(maxfd + 1, &rfds, NULL, NULL, &tv) : 0;

        if (ret > 0) {
            for (int i = 0; i < args->num_readers; i++) {
                if (args->pipe_read_fds[i] >= 0 && FD_ISSET(args->pipe_read_fds[i], &rfds)) read_heartbeat_lines(args, counts, i);
            }
        }

        long elapsed = elapsed_seconds_since(&start);
        if (elapsed > 0 && elapsed % 3 == 0 && elapsed != last_snapshot) {
            print_snapshot(args, counts, &start);
            last_snapshot = elapsed;
            printed_any = 1;
        }
    }

    for (int i = 0; i < args->num_readers; i++) {
        if (args->pipe_read_fds[i] >= 0) read_heartbeat_lines(args, counts, i);
    }
    if (!printed_any) print_snapshot(args, counts, &start);

    free(counts);
    return NULL;
}
