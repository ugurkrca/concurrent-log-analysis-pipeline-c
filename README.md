# Concurrent Log Analysis Pipeline in C

A POSIX C system programming project that implements a concurrent multi-process and multi-threaded log analysis pipeline. The program reads multiple log files, routes log entries by severity level, performs weighted keyword analysis, detects high-priority sources, and generates both human-readable and binary checkpoint outputs.

## Features

- Multi-process architecture using `fork()`
- POSIX threads inside reader and analyzer processes
- Shared memory regions created with `mmap(MAP_SHARED | MAP_ANONYMOUS)`
- Process-shared mutexes and condition variables
- POSIX semaphores for analyzer result synchronization
- Reader heartbeat monitoring through pipes
- Watchdog thread using `select()`
- Dispatcher stage for routing log entries
- Per-level analyzer processes for `ERROR`, `WARN`, `INFO`, and `DEBUG`
- Thread-local per-worker keyword accumulation
- Weighted keyword scoring by severity level
- High-priority source detection
- Human-readable output file
- Binary checkpoint file written through atomic temporary-file rename

## Architecture

The system consists of the following components:

```text
Parent Process
│
├── Reader Process 0
│   ├── Reader Threads
│   └── Parser Thread
│
├── Reader Process 1
│   ├── Reader Threads
│   └── Parser Thread
│
├── Dispatcher Process
│
├── Analyzer Process: ERROR
│   └── Worker Threads
│
├── Analyzer Process: WARN
│   └── Worker Threads
│
├── Analyzer Process: INFO
│   └── Worker Threads
│
├── Analyzer Process: DEBUG
│   └── Worker Threads
│
├── Aggregator Process
│
└── Watchdog Thread
