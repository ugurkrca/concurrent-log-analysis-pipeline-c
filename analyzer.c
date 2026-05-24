#include "analyzer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/types.h>
#include <limits.h>

#ifndef INT_MAX
#include <limits.h>
#endif

typedef struct {
    char source[MAX_SOURCE_LEN];
    long hit_count;
} local_source_stat_t;

typedef struct {
    int count;
    int trip;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} simple_barrier_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int flush_count;
    int num_workers;
    pid_t min_tid;
    int reporter_worker_id;
} analyzer_control_t;

typedef struct {
    double keyword_scores[MAX_KEYWORDS];
    local_source_stat_t sources[100];
    int num_sources;
    int worker_id;
    int level_idx;
    int num_keywords;
    shared_regions_t *shm;
    double thread_score;
    long total_entries;
} worker_tls_t;

typedef struct {
    analyzer_args_t *args;
    int worker_id;
    pid_t tid;
    simple_barrier_t *barrier;
    analyzer_control_t *control;
} analyzer_worker_args_t;

static pthread_key_t tls_key;

static pid_t current_tid(void) {
#ifdef __linux__
    return (pid_t)syscall(SYS_gettid);
#else
    return getpid();
#endif
}

static void simple_barrier_init(simple_barrier_t *b, int count) {
    b->count = 0;
    b->trip = count;
    pthread_mutex_init(&b->mutex, NULL);
    pthread_cond_init(&b->cond, NULL);
}

static void simple_barrier_wait(simple_barrier_t *b) {
    pthread_mutex_lock(&b->mutex);
    b->count++;
    if (b->count == b->trip) {
        pthread_cond_broadcast(&b->cond);
    } else {
        while (b->count < b->trip) pthread_cond_wait(&b->cond, &b->mutex);
    }
    pthread_mutex_unlock(&b->mutex);
}

static void simple_barrier_destroy(simple_barrier_t *b) {
    pthread_mutex_destroy(&b->mutex);
    pthread_cond_destroy(&b->cond);
}

static void analyzer_control_init(analyzer_control_t *c, int num_workers) {
    pthread_mutex_init(&c->mutex, NULL);
    pthread_cond_init(&c->cond, NULL);
    c->flush_count = 0;
    c->num_workers = num_workers;
    c->min_tid = INT_MAX;
    c->reporter_worker_id = -1;
}

static void analyzer_control_destroy(analyzer_control_t *c) {
    pthread_mutex_destroy(&c->mutex);
    pthread_cond_destroy(&c->cond);
}

static int count_keyword_occurrences(const char *text, const char *keyword) {
    int count = 0;
    size_t klen = strlen(keyword);
    if (klen == 0) return 0;
    for (const char *p = text; *p; p++) {
        if (strncmp(p, keyword, klen) == 0) count++;
    }
    return count;
}

static int get_level_weight(int level_idx) {
    if (level_idx == 0) return 4;
    if (level_idx == 1) return 3;
    if (level_idx == 2) return 2;
    if (level_idx == 3) return 1;
    return 0;
}

static void tls_destructor(void *data) {
    free(data);
}

static void add_source_hits(worker_tls_t *tls, const char *source, long hits) {
    if (hits <= 0) return;
    for (int i = 0; i < tls->num_sources; i++) {
        if (strcmp(tls->sources[i].source, source) == 0) {
            tls->sources[i].hit_count += hits;
            return;
        }
    }
    if (tls->num_sources < 100) {
        int idx = tls->num_sources++;
        strncpy(tls->sources[idx].source, source, MAX_SOURCE_LEN - 1);
        tls->sources[idx].source[MAX_SOURCE_LEN - 1] = '\0';
        tls->sources[idx].hit_count = hits;
    }
}

static void flush_tls_to_region_c(worker_tls_t *tls) {
    shared_regions_t *shm = tls->shm;
    int level_idx = tls->level_idx;
    pthread_mutex_lock(&shm->result_mutex);
    for (int i = 0; i < tls->num_keywords; i++) {
        shm->level_results[level_idx].per_keyword_score[i] += tls->keyword_scores[i];
    }
    shm->level_results[level_idx].total_entries += tls->total_entries;
    if (tls->worker_id >= 0 && tls->worker_id < MAX_WORKERS) {
        shm->level_results[level_idx].per_thread_score[tls->worker_id] += tls->thread_score;
    }
    pthread_mutex_lock(&shm->level_results[level_idx].sources_mutex);
    for (int i = 0; i < tls->num_sources; i++) {
        int found = -1;
        for (int j = 0; j < shm->level_results[level_idx].num_sources; j++) {
            if (strcmp(shm->level_results[level_idx].sources[j].source, tls->sources[i].source) == 0) {
                found = j;
                break;
            }
        }
        if (found >= 0) {
            shm->level_results[level_idx].sources[found].hit_count += tls->sources[i].hit_count;
        } else if (shm->level_results[level_idx].num_sources < 100) {
            int idx = shm->level_results[level_idx].num_sources++;
            strncpy(shm->level_results[level_idx].sources[idx].source, tls->sources[i].source, MAX_SOURCE_LEN - 1);
            shm->level_results[level_idx].sources[idx].source[MAX_SOURCE_LEN - 1] = '\0';
            shm->level_results[level_idx].sources[idx].hit_count = tls->sources[i].hit_count;
        }
    }
    pthread_mutex_unlock(&shm->level_results[level_idx].sources_mutex);
    pthread_mutex_unlock(&shm->result_mutex);
}

static void write_level_summary(analyzer_args_t *args, pid_t tid) {
    shared_regions_t *shm = args->shm;
    int level_idx = args->level_idx;
    pthread_mutex_lock(&shm->result_mutex);
    strncpy(shm->level_results[level_idx].level, args->level_name, sizeof(shm->level_results[level_idx].level) - 1);
    shm->level_results[level_idx].level[sizeof(shm->level_results[level_idx].level) - 1] = '\0';
    double total_score = 0.0;
    for (int i = 0; i < args->num_keywords; i++) total_score += shm->level_results[level_idx].per_keyword_score[i];
    shm->level_results[level_idx].total_weighted_score = total_score;
    pthread_mutex_lock(&shm->level_results[level_idx].sources_mutex);
    for (int t = 0; t < 3; t++) {
        long best_hits = -1;
        int best_idx = -1;
        for (int i = 0; i < shm->level_results[level_idx].num_sources; i++) {
            int used = 0;
            for (int j = 0; j < t; j++) {
                if (strcmp(shm->level_results[level_idx].top_source[j], shm->level_results[level_idx].sources[i].source) == 0) used = 1;
            }
            if (!used && shm->level_results[level_idx].sources[i].hit_count > best_hits) {
                best_hits = shm->level_results[level_idx].sources[i].hit_count;
                best_idx = i;
            }
        }
        if (best_idx >= 0) {
            strncpy(shm->level_results[level_idx].top_source[t], shm->level_results[level_idx].sources[best_idx].source, MAX_SOURCE_LEN - 1);
            shm->level_results[level_idx].top_source[t][MAX_SOURCE_LEN - 1] = '\0';
            shm->level_results[level_idx].top_source_hits[t] = shm->level_results[level_idx].sources[best_idx].hit_count;
        }
    }
    pthread_mutex_unlock(&shm->level_results[level_idx].sources_mutex);
    shm->level_results[level_idx].ready = 1;
    printf("[PID:%d][TID:%d] ** Reporting thread (lowest TID). Level: %s **\n", getpid(), tid, args->level_name);
    printf("[PID:%d][TID:%d] Total entries: %ld | Total weighted score: %.1f\n", getpid(), tid, shm->level_results[level_idx].total_entries, shm->level_results[level_idx].total_weighted_score);
    pthread_cond_broadcast(&shm->result_ready_cond);
    sem_post(&shm->level_sem[level_idx]);
    pthread_mutex_unlock(&shm->result_mutex);
}

static void* analyzer_worker_func(void *arg) {
    analyzer_worker_args_t *wargs = arg;
    analyzer_args_t *args = wargs->args;
    shared_regions_t *shm = args->shm;
    int level_idx = args->level_idx;
    pid_t tid = current_tid();
    wargs->tid = tid;
    pthread_mutex_lock(&wargs->control->mutex);
    if (tid < wargs->control->min_tid ||
        (tid == wargs->control->min_tid &&
         (wargs->control->reporter_worker_id < 0 || wargs->worker_id < wargs->control->reporter_worker_id))) {
        wargs->control->min_tid = tid;
        wargs->control->reporter_worker_id = wargs->worker_id;
    }
    pthread_mutex_unlock(&wargs->control->mutex);
    printf("[PID:%d][TID:%d] Worker %d started.\n", getpid(), tid, wargs->worker_id);
    worker_tls_t *tls = calloc(1, sizeof(worker_tls_t));
    if (!tls) pthread_exit(NULL);
    tls->level_idx = level_idx;
    tls->num_keywords = args->num_keywords;
    tls->shm = shm;
    tls->worker_id = wargs->worker_id;
    pthread_setspecific(tls_key, tls);
    int weight = get_level_weight(level_idx);
    while (1) {
        pthread_mutex_lock(&shm->level_buffers[level_idx].level_mutex);
        while (shm->level_buffers[level_idx].count_b == 0 && !shm->level_buffers[level_idx].eof_posted) {
            pthread_cond_wait(&shm->level_buffers[level_idx].not_empty_b, &shm->level_buffers[level_idx].level_mutex);
        }
        if (shm->level_buffers[level_idx].count_b == 0 && shm->level_buffers[level_idx].eof_posted) {
            pthread_mutex_unlock(&shm->level_buffers[level_idx].level_mutex);
            break;
        }
        log_entry_t entry = shm->level_buffers[level_idx].queue_b[shm->level_buffers[level_idx].head_b];
        shm->level_buffers[level_idx].head_b = (shm->level_buffers[level_idx].head_b + 1) % shm->level_buffers[level_idx].capacity_b;
        shm->level_buffers[level_idx].count_b--;
        pthread_cond_signal(&shm->level_buffers[level_idx].not_full_b);
        pthread_mutex_unlock(&shm->level_buffers[level_idx].level_mutex);
        if (entry.is_eof) break;
        tls->total_entries++;
        long source_hits = 0;
        for (int i = 0; i < args->num_keywords; i++) {
            int count = count_keyword_occurrences(entry.message, args->keywords[i]);
            double score = (double)count * weight;
            tls->keyword_scores[i] += score;
            tls->thread_score += score;
            source_hits += count;
        }
        add_source_hits(tls, entry.source, source_hits);
    }
    printf("[PID:%d][TID:%d] Worker %d done. Entries: %ld, Weighted score: %.1f\n", getpid(), tid, wargs->worker_id, tls->total_entries, tls->thread_score);
    simple_barrier_wait(wargs->barrier);
    flush_tls_to_region_c(tls);
    pthread_mutex_lock(&wargs->control->mutex);
    wargs->control->flush_count++;
    pthread_cond_broadcast(&wargs->control->cond);
    int is_reporter = (tid == wargs->control->min_tid && wargs->worker_id == wargs->control->reporter_worker_id);
    while (is_reporter && wargs->control->flush_count < wargs->control->num_workers) {
        pthread_cond_wait(&wargs->control->cond, &wargs->control->mutex);
    }
    pthread_mutex_unlock(&wargs->control->mutex);
    if (is_reporter) write_level_summary(args, tid);
    return NULL;
}

void analyzer_process(analyzer_args_t *args) {
    pid_t pid = getpid();
    printf("[PID:%d] Analyzer %s started. Workers: %d\n", pid, args->level_name, args->num_worker_threads);
    pthread_key_create(&tls_key, tls_destructor);
    simple_barrier_t barrier;
    analyzer_control_t control;
    simple_barrier_init(&barrier, args->num_worker_threads);
    analyzer_control_init(&control, args->num_worker_threads);
    pthread_t *threads = calloc((size_t)args->num_worker_threads, sizeof(pthread_t));
    analyzer_worker_args_t *wargs = calloc((size_t)args->num_worker_threads, sizeof(analyzer_worker_args_t));
    if (!threads || !wargs) exit(1);
    for (int i = 0; i < args->num_worker_threads; i++) {
        wargs[i].args = args;
        wargs[i].worker_id = i;
        wargs[i].tid = INT_MAX;
        wargs[i].barrier = &barrier;
        wargs[i].control = &control;
        pthread_create(&threads[i], NULL, analyzer_worker_func, &wargs[i]);
    }
    for (int i = 0; i < args->num_worker_threads; i++) pthread_join(threads[i], NULL);
    simple_barrier_destroy(&barrier);
    analyzer_control_destroy(&control);
    pthread_key_delete(tls_key);
    free(threads);
    free(wargs);
    printf("[PID:%d] Analyzer %s exiting.\n", pid, args->level_name);
    exit(0);
}
