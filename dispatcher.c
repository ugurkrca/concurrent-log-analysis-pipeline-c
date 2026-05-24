#include "dispatcher.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>

#define MAX_PRIORITY_SOURCES 1000

static char priority_sources[MAX_PRIORITY_SOURCES][MAX_SOURCE_LEN];
static int num_priority_sources = 0;

static int level_index(const char *level) {
    if (strcmp(level, "ERROR") == 0) return 0;
    if (strcmp(level, "WARN") == 0) return 1;
    if (strcmp(level, "INFO") == 0) return 2;
    if (strcmp(level, "DEBUG") == 0) return 3;
    return -1;
}

static int load_priority_filter(const char *filter_file) {
    FILE *f = fopen(filter_file, "r");
    if (!f) {
        fprintf(stderr, "Failed to open filter file: %s\n", filter_file);
        return 0;
    }
    char line[MAX_SOURCE_LEN + 8];
    num_priority_sources = 0;
    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r' || line[len - 1] == ' ' || line[len - 1] == '\t')) line[--len] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '\0') continue;
        if (num_priority_sources < MAX_PRIORITY_SOURCES) {
            strncpy(priority_sources[num_priority_sources], p, MAX_SOURCE_LEN - 1);
            priority_sources[num_priority_sources][MAX_SOURCE_LEN - 1] = '\0';
            num_priority_sources++;
        }
    }
    fclose(f);
    return 1;
}

static int is_priority_source(const char *source) {
    for (int i = 0; i < num_priority_sources; i++) if (strcmp(source, priority_sources[i]) == 0) return 1;
    return 0;
}

static int all_eof_seen(int eof_count[4], int readers) {
    for (int i = 0; i < 4; i++) if (eof_count[i] < readers) return 0;
    return 1;
}

static void push_level_buffer(shared_regions_t *shm, int level_idx, const log_entry_t *entry) {
    pthread_mutex_lock(&shm->level_buffers[level_idx].level_mutex);
    while (shm->level_buffers[level_idx].count_b >= shm->level_buffers[level_idx].capacity_b) pthread_cond_wait(&shm->level_buffers[level_idx].not_full_b, &shm->level_buffers[level_idx].level_mutex);
    shm->level_buffers[level_idx].queue_b[shm->level_buffers[level_idx].tail_b] = *entry;
    shm->level_buffers[level_idx].tail_b = (shm->level_buffers[level_idx].tail_b + 1) % shm->level_buffers[level_idx].capacity_b;
    shm->level_buffers[level_idx].count_b++;
    if (entry->is_eof) shm->level_buffers[level_idx].eof_posted = 1;
    pthread_cond_broadcast(&shm->level_buffers[level_idx].not_empty_b);
    pthread_mutex_unlock(&shm->level_buffers[level_idx].level_mutex);
}

static void push_priority_buffer(shared_regions_t *shm, const log_entry_t *entry) {
    pthread_mutex_lock(&shm->priority_mutex);
    while (shm->count_d >= shm->capacity_d) pthread_cond_wait(&shm->not_full_d, &shm->priority_mutex);
    shm->queue_d[shm->tail_d] = *entry;
    shm->tail_d = (shm->tail_d + 1) % shm->capacity_d;
    shm->count_d++;
    pthread_cond_signal(&shm->not_empty_d);
    pthread_mutex_unlock(&shm->priority_mutex);
}

void dispatcher_process(dispatcher_args_t *args) {
    pid_t pid = getpid();
    printf("[PID:%d] Dispatcher started.\n", pid);
    shared_regions_t *shm = args->shm;
    if (!load_priority_filter(args->filter_file)) {
        fprintf(stderr, "[PID:%d] Failed to load priority filter\n", pid);
        exit(1);
    }
    int eof_count_total[4] = {0, 0, 0, 0};
    int eof_forwarded[4] = {0, 0, 0, 0};
    while (1) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += args->timeout_sec;
        pthread_mutex_lock(&shm->input_mutex);
        int ret = 0;
        while (shm->count_a == 0 && !all_eof_seen(eof_count_total, shm->total_readers)) {
            ret = pthread_cond_timedwait(&shm->not_empty_a, &shm->input_mutex, &ts);
            if (ret == ETIMEDOUT) break;
        }
        if (shm->count_a == 0) {
            int done = all_eof_seen(eof_count_total, shm->total_readers);
            pthread_mutex_unlock(&shm->input_mutex);
            if (done || ret == ETIMEDOUT) {
                if (done) break;
                continue;
            }
            continue;
        }
        log_entry_t entry = shm->queue_a[shm->head_a];
        shm->head_a = (shm->head_a + 1) % shm->capacity_a;
        shm->count_a--;
        pthread_cond_signal(&shm->not_full_a);
        pthread_mutex_unlock(&shm->input_mutex);
        int idx = level_index(entry.level);
        if (idx < 0) continue;
        if (entry.is_eof) {
            eof_count_total[idx]++;
            if (eof_count_total[idx] == shm->total_readers && !eof_forwarded[idx]) {
                eof_forwarded[idx] = 1;
                log_entry_t eof_entry;
                memset(&eof_entry, 0, sizeof(eof_entry));
                eof_entry.is_eof = 1;
                strncpy(eof_entry.level, entry.level, sizeof(eof_entry.level) - 1);
                push_level_buffer(shm, idx, &eof_entry);
            }
            continue;
        }
        push_level_buffer(shm, idx, &entry);
        int priority = is_priority_source(entry.source);
        if (priority) push_priority_buffer(shm, &entry);
        printf("[PID:%d] Routed entry to %s buffer. High-priority: %s (source: %s)\n", pid, entry.level, priority ? "YES" : "NO", entry.source);
    }
    pthread_mutex_lock(&shm->priority_mutex);
    shm->dispatcher_done = 1;
    pthread_cond_broadcast(&shm->not_empty_d);
    pthread_mutex_unlock(&shm->priority_mutex);
    printf("[PID:%d] All EOF markers forwarded to Region B. Exiting.\n", pid);
    exit(0);
}
