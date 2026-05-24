#ifndef DISPATCHER_H
#define DISPATCHER_H

#include "types.h"

typedef struct {
    shared_regions_t *shm;
    const char *filter_file;
    int timeout_sec;
} dispatcher_args_t;

void dispatcher_process(dispatcher_args_t *args);

#endif
