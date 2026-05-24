#ifndef READER_H
#define READER_H

#include "types.h"

typedef struct {
    int reader_id;
    const char *filename;
    int num_reader_threads;
    shared_regions_t *shm;
    int *reader_pipes;  
    int num_files;
} reader_args_t;

void reader_process(reader_args_t *args);

#endif
