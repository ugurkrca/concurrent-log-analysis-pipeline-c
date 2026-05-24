#ifndef SHM_H
#define SHM_H

#include "types.h"

shared_regions_t* initialize_shared_memory(int capacity_a, int capacity_b, int capacity_d);
void cleanup_shared_memory(shared_regions_t *shm);

#endif
