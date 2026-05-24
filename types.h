#ifndef TYPES_H
#define TYPES_H

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>

#define MAX_KEYWORDS 8
#define MAX_WORKERS 64
#define MAX_READERS 16
#define MAX_SOURCE_LEN 64
#define MAX_MESSAGE_LEN 1024
#define MAX_FILENAME_LEN 256

typedef struct {
    char timestamp[20];
    char level[8];
    char source[MAX_SOURCE_LEN];
    char message[MAX_MESSAGE_LEN];
    int is_eof;
} log_entry_t;

typedef struct {
    char source[MAX_SOURCE_LEN];
    long hit_count;
} source_stat_t;

typedef struct {
    char level[8];
    long total_entries;
    double total_weighted_score;
    double per_keyword_score[MAX_KEYWORDS];
    double per_thread_score[MAX_WORKERS];
    char top_source[3][MAX_SOURCE_LEN];
    long top_source_hits[3];
    int ready;
    source_stat_t sources[100];
    int num_sources;
    pthread_mutex_t sources_mutex;
} level_result_t;

typedef struct {
    pthread_mutex_t input_mutex;
    pthread_cond_t not_full_a;
    pthread_cond_t not_empty_a;
    log_entry_t *queue_a;
    int head_a;
    int tail_a;
    int count_a;
    int capacity_a;
    int eof_count_per_level[4];
    int total_readers;
    struct {
        pthread_mutex_t level_mutex;
        pthread_cond_t not_full_b;
        pthread_cond_t not_empty_b;
        log_entry_t *queue_b;
        int head_b;
        int tail_b;
        int count_b;
        int capacity_b;
        int eof_posted;
    } level_buffers[4];
    pthread_mutex_t result_mutex;
    pthread_cond_t result_ready_cond;
    level_result_t level_results[4];
    sem_t level_sem[4];
    double high_priority_score;
    pthread_mutex_t priority_mutex;
    pthread_cond_t not_full_d;
    pthread_cond_t not_empty_d;
    log_entry_t *queue_d;
    int head_d;
    int tail_d;
    int count_d;
    int capacity_d;
    int dispatcher_done;
} shared_regions_t;

#endif
