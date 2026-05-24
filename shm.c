#include "shm.h"
#include <sys/mman.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

shared_regions_t* initialize_shared_memory(int capacity_a, int capacity_b, int capacity_d) {
    shared_regions_t *shm = mmap(NULL, sizeof(shared_regions_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shm == MAP_FAILED) {
        perror("mmap shm structure failed");
        return NULL;
    }
    memset(shm, 0, sizeof(shared_regions_t));
    shm->queue_a = mmap(NULL, capacity_a * sizeof(log_entry_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shm->queue_a == MAP_FAILED) {
        perror("mmap queue_a failed");
        munmap(shm, sizeof(shared_regions_t));
        return NULL;
    }
    memset(shm->queue_a, 0, capacity_a * sizeof(log_entry_t));
    for (int i = 0; i < 4; i++) {
        shm->level_buffers[i].queue_b = mmap(NULL, capacity_b * sizeof(log_entry_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        if (shm->level_buffers[i].queue_b == MAP_FAILED) {
            perror("mmap queue_b failed");
            return NULL;
        }
        memset(shm->level_buffers[i].queue_b, 0, capacity_b * sizeof(log_entry_t));
    }
    shm->queue_d = mmap(NULL, capacity_d * sizeof(log_entry_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shm->queue_d == MAP_FAILED) {
        perror("mmap queue_d failed");
        return NULL;
    }
    memset(shm->queue_d, 0, capacity_d * sizeof(log_entry_t));
    pthread_mutexattr_t mattr;
    pthread_condattr_t cattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
    pthread_condattr_init(&cattr);
    pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&shm->input_mutex, &mattr);
    pthread_cond_init(&shm->not_full_a, &cattr);
    pthread_cond_init(&shm->not_empty_a, &cattr);
    shm->capacity_a = capacity_a;
    for (int i = 0; i < 4; i++) {
        pthread_mutex_init(&shm->level_buffers[i].level_mutex, &mattr);
        pthread_cond_init(&shm->level_buffers[i].not_full_b, &cattr);
        pthread_cond_init(&shm->level_buffers[i].not_empty_b, &cattr);
        shm->level_buffers[i].capacity_b = capacity_b;
    }
    pthread_mutex_init(&shm->result_mutex, &mattr);
    pthread_cond_init(&shm->result_ready_cond, &cattr);
    for (int i = 0; i < 4; i++) {
        sem_init(&shm->level_sem[i], 1, 0);
        pthread_mutex_init(&shm->level_results[i].sources_mutex, &mattr);
    }
    pthread_mutex_init(&shm->priority_mutex, &mattr);
    pthread_cond_init(&shm->not_full_d, &cattr);
    pthread_cond_init(&shm->not_empty_d, &cattr);
    shm->capacity_d = capacity_d;
    pthread_mutexattr_destroy(&mattr);
    pthread_condattr_destroy(&cattr);
    return shm;
}

void cleanup_shared_memory(shared_regions_t *shm) {
    if (!shm) return;
    pthread_mutex_destroy(&shm->input_mutex);
    pthread_cond_destroy(&shm->not_full_a);
    pthread_cond_destroy(&shm->not_empty_a);
    for (int i = 0; i < 4; i++) {
        pthread_mutex_destroy(&shm->level_buffers[i].level_mutex);
        pthread_cond_destroy(&shm->level_buffers[i].not_full_b);
        pthread_cond_destroy(&shm->level_buffers[i].not_empty_b);
        sem_destroy(&shm->level_sem[i]);
        pthread_mutex_destroy(&shm->level_results[i].sources_mutex);
    }
    pthread_mutex_destroy(&shm->result_mutex);
    pthread_cond_destroy(&shm->result_ready_cond);
    pthread_mutex_destroy(&shm->priority_mutex);
    pthread_cond_destroy(&shm->not_full_d);
    pthread_cond_destroy(&shm->not_empty_d);
    munmap(shm->queue_a, shm->capacity_a * sizeof(log_entry_t));
    for (int i = 0; i < 4; i++) munmap(shm->level_buffers[i].queue_b, shm->level_buffers[i].capacity_b * sizeof(log_entry_t));
    munmap(shm->queue_d, shm->capacity_d * sizeof(log_entry_t));
    munmap(shm, sizeof(shared_regions_t));
}
