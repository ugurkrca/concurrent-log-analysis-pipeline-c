#include "aggregator.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t num_levels;
    uint32_t num_keywords;
    double total_weighted;
    double high_priority_weighted;
} checkpoint_header_t;

typedef struct {
    shared_regions_t *shm;
    int num_keywords;
    char **keywords;
    double score;
} priority_drain_args_t;

static int count_keyword_occurrences(const char *text, const char *keyword) {
    int count = 0;
    size_t klen = strlen(keyword);
    if (klen == 0) return 0;
    for (const char *p = text; *p; p++) if (strncmp(p, keyword, klen) == 0) count++;
    return count;
}

static int get_level_weight(const char *level) {
    if (strcmp(level, "ERROR") == 0) return 4;
    if (strcmp(level, "WARN") == 0) return 3;
    if (strcmp(level, "INFO") == 0) return 2;
    if (strcmp(level, "DEBUG") == 0) return 1;
    return 0;
}

static void* drain_priority_region(void *arg) {
    priority_drain_args_t *dargs = arg;
    shared_regions_t *shm = dargs->shm;
    double score = 0.0;
    pthread_mutex_lock(&shm->priority_mutex);
    while (!shm->dispatcher_done || shm->count_d > 0) {
        while (shm->count_d == 0 && !shm->dispatcher_done) pthread_cond_wait(&shm->not_empty_d, &shm->priority_mutex);
        if (shm->count_d == 0 && shm->dispatcher_done) break;
        log_entry_t entry = shm->queue_d[shm->head_d];
        shm->head_d = (shm->head_d + 1) % shm->capacity_d;
        shm->count_d--;
        pthread_cond_signal(&shm->not_full_d);
        pthread_mutex_unlock(&shm->priority_mutex);
        int weight = get_level_weight(entry.level);
        for (int i = 0; i < dargs->num_keywords; i++) score += (double)count_keyword_occurrences(entry.message, dargs->keywords[i]) * weight;
        pthread_mutex_lock(&shm->priority_mutex);
    }
    pthread_mutex_unlock(&shm->priority_mutex);
    dargs->score = score;
    return NULL;
}

static int all_results_ready(shared_regions_t *shm) {
    for (int i = 0; i < 4; i++) if (!shm->level_results[i].ready) return 0;
    return 1;
}

void aggregator_process(aggregator_args_t *args) {
    pid_t pid = getpid();
    printf("[PID:%d] Aggregator started. Waiting for 4 levels...\n", pid);
    shared_regions_t *shm = args->shm;
    priority_drain_args_t drain_args;
    memset(&drain_args, 0, sizeof(drain_args));
    drain_args.shm = shm;
    drain_args.num_keywords = args->num_keywords;
    drain_args.keywords = args->keywords;
    pthread_t drain_thread;
    pthread_create(&drain_thread, NULL, drain_priority_region, &drain_args);
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += args->timeout_sec;
    pthread_mutex_lock(&shm->result_mutex);
    while (!all_results_ready(shm)) {
        int ret = pthread_cond_timedwait(&shm->result_ready_cond, &shm->result_mutex, &deadline);
        if (ret == ETIMEDOUT && !all_results_ready(shm)) {
            fprintf(stderr, "[PID:%d] Timeout waiting for analyzer results\n", pid);
            pthread_mutex_unlock(&shm->result_mutex);
            pthread_join(drain_thread, NULL);
            exit(1);
        }
    }
    pthread_mutex_unlock(&shm->result_mutex);
    for (int i = 0; i < 4; i++) printf("[PID:%d] %s result received.\n", pid, shm->level_results[i].level);
    pthread_join(drain_thread, NULL);
    double high_priority_score = drain_args.score;
    pthread_mutex_lock(&shm->result_mutex);
    shm->high_priority_score = high_priority_score;
    pthread_mutex_unlock(&shm->result_mutex);
    printf("[PID:%d] All results received. Writing output files...\n", pid);
    double total_weighted_score = 0.0;
    int order[4] = {0, 1, 2, 3};
    for (int i = 0; i < 4; i++) total_weighted_score += shm->level_results[i].total_weighted_score;
    for (int i = 0; i < 3; i++) {
        for (int j = i + 1; j < 4; j++) {
            if (shm->level_results[order[j]].total_weighted_score > shm->level_results[order[i]].total_weighted_score) {
                int tmp = order[i];
                order[i] = order[j];
                order[j] = tmp;
            }
        }
    }
    FILE *out = fopen(args->output_file, "w");
    if (!out) {
        fprintf(stderr, "[PID:%d] Failed to open output file\n", pid);
        exit(1);
    }
    fprintf(out, "KEYWORD_LIST: ");
    for (int i = 0; i < args->num_keywords; i++) fprintf(out, "%s%s", i ? "," : "", args->keywords[i]);
    fprintf(out, "\nFILES: %d\n", args->num_files);
    fprintf(out, "TOTAL_WEIGHTED_SCORE: %.1f\n", total_weighted_score);
    fprintf(out, "HIGH_PRIORITY_SCORE: %.1f\n", high_priority_score);
    fprintf(out, "# Levels sorted by total_weighted_score DESC\n");
    fprintf(out, "%-5s  %7s  %14s", "LEVEL", "ENTRIES", "WEIGHTED_SCORE");
    for (int i = 0; i < args->num_keywords; i++) fprintf(out, "  %7s", args->keywords[i]);
    fprintf(out, "\n");
    for (int k = 0; k < 4; k++) {
        int i = order[k];
        fprintf(out, "%-5s  %7ld  %14.1f", shm->level_results[i].level, shm->level_results[i].total_entries, shm->level_results[i].total_weighted_score);
        for (int j = 0; j < args->num_keywords; j++) fprintf(out, "  %7.1f", shm->level_results[i].per_keyword_score[j]);
        fprintf(out, "\n");
    }
    fprintf(out, "# Top-3 sources per level\n");
    for (int i = 0; i < 4; i++) {
        fprintf(out, "%s", shm->level_results[i].level);
        for (int j = 0; j < 3; j++) if (shm->level_results[i].top_source[j][0]) fprintf(out, "  %s:%ld", shm->level_results[i].top_source[j], shm->level_results[i].top_source_hits[j]);
        fprintf(out, "\n");
    }
    fprintf(out, "# Per-thread contributions (weighted score)\n");
    for (int i = 0; i < 4; i++) {
        fprintf(out, "%s", shm->level_results[i].level);
        for (int j = 0; j < args->num_worker_threads; j++) fprintf(out, "  thread_%d:%.1f", j, shm->level_results[i].per_thread_score[j]);
        fprintf(out, "\n");
    }
    fclose(out);
    char tmp_file[512];
    snprintf(tmp_file, sizeof(tmp_file), "%s.tmp", args->binary_file);
    FILE *bin = fopen(tmp_file, "wb");
    if (!bin) {
        fprintf(stderr, "[PID:%d] Failed to open binary output file\n", pid);
        exit(1);
    }
    checkpoint_header_t header;
    header.magic = 0xC5E3440B;
    header.version = 1;
    header.num_levels = 4;
    header.num_keywords = (uint32_t)args->num_keywords;
    header.total_weighted = total_weighted_score;
    header.high_priority_weighted = high_priority_score;
    if (fwrite(&header, sizeof(header), 1, bin) != 1) {
        fprintf(stderr, "[PID:%d] Failed to write binary header\n", pid);
        fclose(bin);
        exit(1);
    }
    for (int i = 0; i < 4; i++) {
        if (fwrite(&shm->level_results[i], sizeof(level_result_t), 1, bin) != 1) {
            fprintf(stderr, "[PID:%d] Failed to write binary level result\n", pid);
            fclose(bin);
            exit(1);
        }
    }
    if (fclose(bin) != 0) {
        fprintf(stderr, "[PID:%d] Failed to close binary output file\n", pid);
        exit(1);
    }
    if (rename(tmp_file, args->binary_file) != 0) {
        fprintf(stderr, "[PID:%d] Failed to rename binary file\n", pid);
        exit(1);
    }
    printf("[PID:%d] Output files written: %s, %s\n", pid, args->output_file, args->binary_file);
    printf("[PID:%d] Aggregator exiting.\n", pid);
    exit(0);
}
