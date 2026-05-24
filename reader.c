#include "reader.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <ctype.h>
#include <sys/syscall.h>

#define INTERNAL_BUFFER_CAPACITY 1000

typedef struct {
    log_entry_t entries[INTERNAL_BUFFER_CAPACITY];
    int head;
    int tail;
    int count;
    int active_readers;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} internal_buffer_t;

typedef struct {
    int reader_id;
    const char *filename;
    int thread_id;
    off_t start;
    off_t end;
    internal_buffer_t *ibuf;
    int pipe_fd;
    int lines_processed;
    int malformed_lines;
} reader_thread_args_t;

typedef struct {
    int reader_id;
    shared_regions_t *shm;
    internal_buffer_t *ibuf;
    int dispatched[4];
} parser_args_t;

static pid_t current_tid(void) {
#ifdef __linux__
    return (pid_t)syscall(SYS_gettid);
#else
    return getpid();
#endif
}

static int level_index(const char *level) {
    if (strcmp(level, "ERROR") == 0) return 0;
    if (strcmp(level, "WARN") == 0) return 1;
    if (strcmp(level, "INFO") == 0) return 2;
    if (strcmp(level, "DEBUG") == 0) return 3;
    return -1;
}

static int parse_log_entry(const char *line, log_entry_t *entry) {
    const char *p = line;
    memset(entry, 0, sizeof(*entry));
    if (*p != '[') return 0;
    p++;
    const char *ts = p;
    while (*p && *p != ']') p++;
    if (*p != ']' || p - ts != 19) return 0;
    memcpy(entry->timestamp, ts, 19);
    entry->timestamp[19] = '\0';
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '[') return 0;
    p++;
    const char *lv = p;
    while (*p && *p != ']') p++;
    if (*p != ']') return 0;
    size_t lvlen = (size_t)(p - lv);
    if (lvlen == 0 || lvlen >= sizeof(entry->level)) return 0;
    memcpy(entry->level, lv, lvlen);
    entry->level[lvlen] = '\0';
    if (level_index(entry->level) < 0) return 0;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '[') return 0;
    p++;
    const char *src = p;
    while (*p && *p != ']') {
        if (!isalnum((unsigned char)*p) && *p != '_' && *p != '-' && *p != '.') return 0;
        p++;
    }
    if (*p != ']') return 0;
    size_t srclen = (size_t)(p - src);
    if (srclen == 0 || srclen >= MAX_SOURCE_LEN) return 0;
    memcpy(entry->source, src, srclen);
    entry->source[srclen] = '\0';
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    size_t msglen = strlen(p);
    while (msglen > 0 && (p[msglen - 1] == '\n' || p[msglen - 1] == '\r')) msglen--;
    if (msglen >= MAX_MESSAGE_LEN) msglen = MAX_MESSAGE_LEN - 1;
    memcpy(entry->message, p, msglen);
    entry->message[msglen] = '\0';
    entry->is_eof = 0;
    return 1;
}

static void push_region_a(shared_regions_t *shm, const log_entry_t *entry) {
    pthread_mutex_lock(&shm->input_mutex);
    while (shm->count_a >= shm->capacity_a) pthread_cond_wait(&shm->not_full_a, &shm->input_mutex);
    shm->queue_a[shm->tail_a] = *entry;
    shm->tail_a = (shm->tail_a + 1) % shm->capacity_a;
    shm->count_a++;
    if (entry->is_eof) {
        int idx = level_index(entry->level);
        if (idx >= 0) shm->eof_count_per_level[idx]++;
    }
    pthread_cond_signal(&shm->not_empty_a);
    pthread_mutex_unlock(&shm->input_mutex);
}

static void* parser_thread_func(void *arg) {
    parser_args_t *pargs = arg;
    internal_buffer_t *ibuf = pargs->ibuf;
    while (1) {
        pthread_mutex_lock(&ibuf->mutex);
        while (ibuf->count == 0 && ibuf->active_readers > 0) pthread_cond_wait(&ibuf->not_empty, &ibuf->mutex);
        if (ibuf->count == 0 && ibuf->active_readers == 0) {
            pthread_mutex_unlock(&ibuf->mutex);
            break;
        }
        log_entry_t entry = ibuf->entries[ibuf->head];
        ibuf->head = (ibuf->head + 1) % INTERNAL_BUFFER_CAPACITY;
        ibuf->count--;
        pthread_cond_signal(&ibuf->not_full);
        pthread_mutex_unlock(&ibuf->mutex);
        int idx = level_index(entry.level);
        if (idx >= 0) {
            pargs->dispatched[idx]++;
            push_region_a(pargs->shm, &entry);
        }
    }
    const char *levels[4] = {"ERROR", "WARN", "INFO", "DEBUG"};
    for (int i = 0; i < 4; i++) {
        log_entry_t eof_entry;
        memset(&eof_entry, 0, sizeof(eof_entry));
        eof_entry.is_eof = 1;
        strncpy(eof_entry.level, levels[i], sizeof(eof_entry.level) - 1);
        push_region_a(pargs->shm, &eof_entry);
    }
    return NULL;
}

static void* reader_thread_func(void *arg) {
    reader_thread_args_t *args = arg;
    pid_t tid = current_tid();
    printf("[PID:%d][TID:%d] Reader thread %d: range [%lld, %lld) bytes\n", getpid(), tid, args->thread_id, (long long)args->start, (long long)args->end);
    FILE *f = fopen(args->filename, "r");
    if (!f) {
        fprintf(stderr, "[PID:%d] Failed to open file in thread: %s\n", getpid(), args->filename);
        pthread_mutex_lock(&args->ibuf->mutex);
        args->ibuf->active_readers--;
        pthread_cond_broadcast(&args->ibuf->not_empty);
        pthread_mutex_unlock(&args->ibuf->mutex);
        return NULL;
    }
    fseeko(f, args->start, SEEK_SET);
    char line[MAX_MESSAGE_LEN + 512];
    if (args->start > 0) {
        if (!fgets(line, sizeof(line), f)) {
            fclose(f);
            pthread_mutex_lock(&args->ibuf->mutex);
            args->ibuf->active_readers--;
            pthread_cond_broadcast(&args->ibuf->not_empty);
            pthread_mutex_unlock(&args->ibuf->mutex);
            printf("[PID:%d][TID:%d] Reader thread %d: finished, lines_read=0, malformed=0\n", getpid(), tid, args->thread_id);
            return NULL;
        }
    }
    int lines_read = 0;
    int malformed = 0;
    while (ftello(f) < args->end && fgets(line, sizeof(line), f)) {
        char *q = line;
        while (*q && isspace((unsigned char)*q)) q++;
        if (*q == '\0') continue;
        log_entry_t entry;
        if (!parse_log_entry(line, &entry)) {
            malformed++;
        } else {
            pthread_mutex_lock(&args->ibuf->mutex);
            while (args->ibuf->count >= INTERNAL_BUFFER_CAPACITY) pthread_cond_wait(&args->ibuf->not_full, &args->ibuf->mutex);
            args->ibuf->entries[args->ibuf->tail] = entry;
            args->ibuf->tail = (args->ibuf->tail + 1) % INTERNAL_BUFFER_CAPACITY;
            args->ibuf->count++;
            pthread_cond_signal(&args->ibuf->not_empty);
            pthread_mutex_unlock(&args->ibuf->mutex);
        }
        lines_read++;
        if (lines_read % 50 == 0) {
            char heartbeat[128];
            int n = snprintf(heartbeat, sizeof(heartbeat), "[R%d] %d lines processed\n", args->reader_id, lines_read);
            if (n > 0) write(args->pipe_fd, heartbeat, (size_t)n);
        }
    }
    char heartbeat[128];
    int n = snprintf(heartbeat, sizeof(heartbeat), "[R%d] %d lines processed\n", args->reader_id, lines_read);
    if (n > 0) write(args->pipe_fd, heartbeat, (size_t)n);
    fclose(f);
    args->lines_processed = lines_read;
    args->malformed_lines = malformed;
    pthread_mutex_lock(&args->ibuf->mutex);
    args->ibuf->active_readers--;
    pthread_cond_broadcast(&args->ibuf->not_empty);
    pthread_mutex_unlock(&args->ibuf->mutex);
    printf("[PID:%d][TID:%d] Reader thread %d: finished, lines_read=%d, malformed=%d\n", getpid(), tid, args->thread_id, lines_read, malformed);
    return NULL;
}

void reader_process(reader_args_t *args) {
    pid_t pid = getpid();
    printf("[PID:%d] Reader %d started. File: %s, Threads: %d\n", pid, args->reader_id, args->filename, args->num_reader_threads);
    FILE *file = fopen(args->filename, "r");
    if (!file) {
        fprintf(stderr, "[PID:%d] Failed to open file: %s\n", pid, args->filename);
        exit(1);
    }
    fseeko(file, 0, SEEK_END);
    off_t file_size = ftello(file);
    fclose(file);
    internal_buffer_t ibuf;
    memset(&ibuf, 0, sizeof(ibuf));
    ibuf.active_readers = args->num_reader_threads;
    pthread_mutex_init(&ibuf.mutex, NULL);
    pthread_cond_init(&ibuf.not_full, NULL);
    pthread_cond_init(&ibuf.not_empty, NULL);
    pthread_t parser_thread;
    parser_args_t pargs;
    memset(&pargs, 0, sizeof(pargs));
    pargs.reader_id = args->reader_id;
    pargs.shm = args->shm;
    pargs.ibuf = &ibuf;
    pthread_create(&parser_thread, NULL, parser_thread_func, &pargs);
    pthread_t *threads = calloc((size_t)args->num_reader_threads, sizeof(pthread_t));
    reader_thread_args_t *thread_args = calloc((size_t)args->num_reader_threads, sizeof(reader_thread_args_t));
    off_t chunk = args->num_reader_threads > 0 ? file_size / args->num_reader_threads : file_size;
    for (int i = 0; i < args->num_reader_threads; i++) {
        thread_args[i].reader_id = args->reader_id;
        thread_args[i].filename = args->filename;
        thread_args[i].thread_id = i;
        thread_args[i].start = i * chunk;
        thread_args[i].end = (i == args->num_reader_threads - 1) ? file_size : (i + 1) * chunk;
        thread_args[i].ibuf = &ibuf;
        thread_args[i].pipe_fd = args->reader_pipes[args->reader_id];
        pthread_create(&threads[i], NULL, reader_thread_func, &thread_args[i]);
    }
    for (int i = 0; i < args->num_reader_threads; i++) pthread_join(threads[i], NULL);
    pthread_join(parser_thread, NULL);
    printf("[PID:%d] Parser thread: dispatched E:%d W:%d I:%d D:%d -> Region A\n", pid, pargs.dispatched[0], pargs.dispatched[1], pargs.dispatched[2], pargs.dispatched[3]);
    int total_lines = 0;
    for (int i = 0; i < args->num_reader_threads; i++) total_lines += thread_args[i].lines_processed;
    char heartbeat[128];
    int n = snprintf(heartbeat, sizeof(heartbeat), "[R%d] %d lines processed\n", args->reader_id, total_lines);
    if (n > 0) write(args->reader_pipes[args->reader_id], heartbeat, (size_t)n);
    free(threads);
    free(thread_args);
    pthread_mutex_destroy(&ibuf.mutex);
    pthread_cond_destroy(&ibuf.not_full);
    pthread_cond_destroy(&ibuf.not_empty);
    printf("[PID:%d] Reader %d exiting.\n", pid, args->reader_id);
    exit(0);
}
