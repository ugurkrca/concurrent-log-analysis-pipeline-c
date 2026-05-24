#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <signal.h>
#include <getopt.h>
#include <errno.h>
#include "types.h"
#include "shm.h"
#include "reader.h"
#include "dispatcher.h"
#include "analyzer.h"
#include "aggregator.h"
#include "watchdog.h"

#define MAX_FILES 16

volatile sig_atomic_t shutdown_watchdog = 0;
volatile sig_atomic_t sigint_received = 0;

typedef struct {
    char filename[MAX_FILENAME_LEN];
} config_file_t;

static void sigint_handler(int sig) {
    (void)sig;
    sigint_received = 1;
}

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s -c <config> -f <filter> -k <keywords> -t <reader_threads> -w <workers> -a <cap_a> -b <cap_b> -d <cap_d> -T <timeout> -o <output> -O <binary>\n", prog);
}

static int file_readable(const char *path) {
    return path && access(path, R_OK) == 0;
}

static int read_config_file(const char *config_path, config_file_t *files, int max_files) {
    FILE *f = fopen(config_path, "r");
    if (!f) {
        fprintf(stderr, "Failed to open config file: %s\n", config_path);
        return 0;
    }
    int count = 0;
    char line[MAX_FILENAME_LEN + 8];
    while (fgets(line, sizeof(line), f) && count < max_files) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r' || line[len - 1] == ' ' || line[len - 1] == '\t')) line[--len] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '\0') continue;
        strncpy(files[count].filename, p, MAX_FILENAME_LEN - 1);
        files[count].filename[MAX_FILENAME_LEN - 1] = '\0';
        if (!file_readable(files[count].filename)) fprintf(stderr, "Warning: log file may not be readable now: %s\n", files[count].filename);
        count++;
    }
    fclose(f);
    return count;
}

static int parse_keywords(const char *keyword_str, char **keywords) {
    char *copy = strdup(keyword_str);
    if (!copy) return 0;
    int count = 0;
    char *save = NULL;
    char *tok = strtok_r(copy, ",", &save);
    while (tok) {
        if (*tok == '\0' || strchr(tok, ' ') || strchr(tok, '\t') || count >= MAX_KEYWORDS) {
            free(copy);
            return 0;
        }
        keywords[count] = strdup(tok);
        if (!keywords[count]) {
            free(copy);
            return 0;
        }
        count++;
        tok = strtok_r(NULL, ",", &save);
    }
    free(copy);
    return count;
}

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    pid_t pid = getpid();
    const char *config_file = NULL;
    const char *filter_file = NULL;
    const char *keyword_str = NULL;
    int num_reader_threads = 0;
    int num_worker_threads = 0;
    int capacity_a = 0;
    int capacity_b = 0;
    int capacity_d = 0;
    int timeout_sec = 10;
    const char *output_file = NULL;
    const char *binary_file = NULL;
    int opt;
    while ((opt = getopt(argc, argv, "c:f:k:t:w:a:b:d:T:o:O:")) != -1) {
        switch (opt) {
            case 'c': config_file = optarg; break;
            case 'f': filter_file = optarg; break;
            case 'k': keyword_str = optarg; break;
            case 't': num_reader_threads = atoi(optarg); break;
            case 'w': num_worker_threads = atoi(optarg); break;
            case 'a': capacity_a = atoi(optarg); break;
            case 'b': capacity_b = atoi(optarg); break;
            case 'd': capacity_d = atoi(optarg); break;
            case 'T': timeout_sec = atoi(optarg); break;
            case 'o': output_file = optarg; break;
            case 'O': binary_file = optarg; break;
            default: usage(argv[0]); return 1;
        }
    }
    if (!config_file || !filter_file || !keyword_str || !output_file || !binary_file) {
        fprintf(stderr, "Missing required arguments\n");
        usage(argv[0]);
        return 1;
    }
    if (!file_readable(config_file) || !file_readable(filter_file)) {
        fprintf(stderr, "Config or filter file is not readable\n");
        return 1;
    }
    if (num_reader_threads < 1 || num_worker_threads < 1 || num_worker_threads > MAX_WORKERS || capacity_a < 4 || capacity_b < 4 || capacity_d < 2 || timeout_sec < 1 || output_file[0] == '\0' || binary_file[0] == '\0') {
        fprintf(stderr, "Invalid numeric parameter\n");
        return 1;
    }
    char *keywords[MAX_KEYWORDS] = {0};
    int num_keywords = parse_keywords(keyword_str, keywords);
    if (num_keywords < 1 || num_keywords > MAX_KEYWORDS) {
        fprintf(stderr, "Invalid keyword list\n");
        return 1;
    }
    config_file_t config_files[MAX_FILES];
    int num_files = read_config_file(config_file, config_files, MAX_FILES);
    if (num_files < 1) {
        fprintf(stderr, "No files in config\n");
        return 1;
    }
    printf("[PID:%d] Parent started. Files: %d, Keywords: %s\n", pid, num_files, keyword_str);
    shared_regions_t *shm = initialize_shared_memory(capacity_a, capacity_b, capacity_d);
    if (!shm) {
        fprintf(stderr, "Failed to initialize shared memory\n");
        return 1;
    }
    shm->total_readers = num_files;
    printf("[PID:%d] Shared memory initialized (A:%d B:%dx4 D:%d).\n", pid, capacity_a, capacity_b, capacity_d);
    int reader_pipes[MAX_FILES][2];
    for (int i = 0; i < num_files; i++) {
        if (pipe(reader_pipes[i]) == -1) {
            perror("pipe");
            return 1;
        }
    }
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT, &sa, NULL);
    pid_t reader_pids[MAX_FILES];
    for (int i = 0; i < num_files; i++) {
        printf("[PID:%d] Forking Reader %d -> %s\n", pid, i, config_files[i].filename);
        pid_t child = fork();
        if (child == 0) {
            close(reader_pipes[i][0]);
            reader_args_t rargs;
            rargs.reader_id = i;
            rargs.filename = config_files[i].filename;
            rargs.num_reader_threads = num_reader_threads;
            rargs.shm = shm;
            rargs.reader_pipes = malloc((size_t)num_files * sizeof(int));
            rargs.num_files = num_files;
            for (int j = 0; j < num_files; j++) rargs.reader_pipes[j] = (j == i) ? reader_pipes[i][1] : -1;
            reader_process(&rargs);
            exit(0);
        }
        if (child < 0) {
            perror("fork");
            return 1;
        }
        reader_pids[i] = child;
    }
    printf("[PID:%d] Forking Dispatcher\n", pid);
    pid_t dispatcher_pid = fork();
    if (dispatcher_pid == 0) {
        dispatcher_args_t dargs;
        dargs.shm = shm;
        dargs.filter_file = filter_file;
        dargs.timeout_sec = timeout_sec;
        dispatcher_process(&dargs);
        exit(0);
    }
    if (dispatcher_pid < 0) {
        perror("fork");
        return 1;
    }
    const char *level_names[4] = {"ERROR", "WARN", "INFO", "DEBUG"};
    pid_t analyzer_pids[4];
    for (int i = 0; i < 4; i++) {
        printf("[PID:%d] Forking Analyzer %s (index %d)\n", pid, level_names[i], i);
        pid_t child = fork();
        if (child == 0) {
            analyzer_args_t aargs;
            aargs.level_idx = i;
            aargs.level_name = level_names[i];
            aargs.num_keywords = num_keywords;
            aargs.keywords = keywords;
            aargs.num_worker_threads = num_worker_threads;
            aargs.shm = shm;
            analyzer_process(&aargs);
            exit(0);
        }
        if (child < 0) {
            perror("fork");
            return 1;
        }
        analyzer_pids[i] = child;
    }
    printf("[PID:%d] Forking Aggregator\n", pid);
    pid_t aggregator_pid = fork();
    if (aggregator_pid == 0) {
        aggregator_args_t gargs;
        gargs.shm = shm;
        gargs.output_file = output_file;
        gargs.binary_file = binary_file;
        gargs.num_keywords = num_keywords;
        gargs.keywords = keywords;
        gargs.timeout_sec = timeout_sec;
        gargs.num_worker_threads = num_worker_threads;
        gargs.num_files = num_files;
        aggregator_process(&gargs);
        exit(0);
    }
    if (aggregator_pid < 0) {
        perror("fork");
        return 1;
    }
    int reader_pipe_read_fds[MAX_FILES];
    for (int i = 0; i < num_files; i++) {
        close(reader_pipes[i][1]);
        reader_pipe_read_fds[i] = reader_pipes[i][0];
    }
    pid_t child_pids[MAX_FILES + 6];
    int child_count = 0;
    for (int i = 0; i < num_files; i++) child_pids[child_count++] = reader_pids[i];
    child_pids[child_count++] = dispatcher_pid;
    for (int i = 0; i < 4; i++) child_pids[child_count++] = analyzer_pids[i];
    child_pids[child_count++] = aggregator_pid;
    watchdog_args_t wargs;
    wargs.pipe_read_fds = reader_pipe_read_fds;
    wargs.num_readers = num_files;
    wargs.child_pids = child_pids;
    wargs.num_children = child_count;
    wargs.shutdown_flag = &shutdown_watchdog;
    pthread_t watchdog_tid;
    printf("[PID:%d] Watchdog thread started.\n", pid);
    pthread_create(&watchdog_tid, NULL, watchdog_thread_func, &wargs);
    int exited = 0;
    while (exited < child_count) {
        if (sigint_received) {
            for (int i = 0; i < child_count; i++) if (child_pids[i] > 0) kill(child_pids[i], SIGTERM);
        }
        int status;
        pid_t child = waitpid(-1, &status, 0);
        if (child > 0) exited++;
        else if (errno != EINTR) break;
    }
    shutdown_watchdog = 1;
    pthread_join(watchdog_tid, NULL);
    for (int i = 0; i < num_files; i++) close(reader_pipe_read_fds[i]);
    printf("==================================================\n");
    printf("SYSTEM SUMMARY\n");
    printf("Keywords : ");
    for (int i = 0; i < num_keywords; i++) printf("%s%s", i ? ", " : "", keywords[i]);
    printf("\nLog files : %d\n", num_files);
    long total_entries = 0;
    double total_weighted = 0.0;
    for (int i = 0; i < 4; i++) {
        total_entries += shm->level_results[i].total_entries;
        total_weighted += shm->level_results[i].total_weighted_score;
    }
    printf("Total entries : %ld\n", total_entries);
    printf("Total weighted : %.1f\n", total_weighted);
    printf("High-priority : %.1f (source filter: %s)\n", shm->high_priority_score, filter_file);
    for (int i = 0; i < 4; i++) printf(" %s : %ld entries, score: %.1f\n", shm->level_results[i].level, shm->level_results[i].total_entries, shm->level_results[i].total_weighted_score);
    printf("==================================================\n");
    printf("Program terminated successfully.\n");
    cleanup_shared_memory(shm);
    for (int i = 0; i < num_keywords; i++) free(keywords[i]);
    return 0;
}
