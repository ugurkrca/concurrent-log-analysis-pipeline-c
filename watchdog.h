#ifndef WATCHDOG_H
#define WATCHDOG_H

#include "types.h"
#include <signal.h>

typedef struct {
    int *pipe_read_fds;
    int num_readers;
    pid_t *child_pids;
    int num_children;
    volatile sig_atomic_t *shutdown_flag;
} watchdog_args_t;

void* watchdog_thread_func(void *arg);

#endif
