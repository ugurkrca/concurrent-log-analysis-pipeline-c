#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include "types.h"

typedef struct {
    shared_regions_t *shm;
    const char *output_file;
    const char *binary_file;
    int num_keywords;
    char **keywords;
    int timeout_sec;
    int num_worker_threads;
    int num_files;
} aggregator_args_t;

void aggregator_process(aggregator_args_t *args);

#endif
