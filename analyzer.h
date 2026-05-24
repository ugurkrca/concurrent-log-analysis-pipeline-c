#ifndef ANALYZER_H
#define ANALYZER_H

#include "types.h"

typedef struct {
    int level_idx;  
    const char *level_name;
    int num_keywords;
    char **keywords;
    int num_worker_threads;
    shared_regions_t *shm;
} analyzer_args_t;

void analyzer_process(analyzer_args_t *args);

#endif
