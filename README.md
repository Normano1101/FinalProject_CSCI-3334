# Concurrent Task Dispatcher

Norman Madrid
UTRGV Computer Science — Spring 2026

---

## Overview

This project simulates a concurrent task scheduling system written in Rust. The goal is to model how an operating-system-style dispatcher manages incoming work, places tasks into queues, assigns tasks to worker threads, and collects performance metrics.

The system supports both CPU-bound and IO-bound tasks and compares scheduling behavior under two different policies: FIFO and Optimized (Shortest Job First with IO preference).

This project was built for a systems/concurrency final project.

---

## Build Instructions

Clone the repository:

```bash
git clone <your_repo_link>
cd finalproject
```

Build:

```bash
cargo build
```

Run:

```bash
cargo run --release
```

To save experiment output to a file:

```bash
cargo run --release > experiment_output.txt
```

---

## Example Output

```text
== FIFO simulation ==
1000 tasks, 70% IO / 30% CPU, 8 workers, cap 100%

— results —
total runtime        : 24942 ms
makespan             : 24942 ms
tasks completed      : 1000  (IO=709, CPU=291)
avg wait time        : 8793.51 ms
avg turnaround time  : 8991.75 ms
max wait time        : 21685 ms (task #998)
avg CPU usage        : 99.95 %
avg workers active   : 8.00 / 8
monitor samples      : 2234
monitor csv          : monitor_log_fifo.csv

== Optimized simulation ==
1000 tasks, 70% IO / 30% CPU, 8 workers, cap 100%

— results —
total runtime        : 25042 ms
makespan             : 25042 ms
tasks completed      : 1000  (IO=709, CPU=291)
avg wait time        : 7583.69 ms
avg wait (IO only)   : 4418.96 ms
avg wait (CPU only)  : 15294.32 ms
avg turnaround time  : 7782.01 ms
max wait time        : 23903 ms (task #44)
avg CPU usage        : 99.95 %
avg workers active   : 8.00 / 8
monitor samples      : 2219
monitor csv          : monitor_log.csv
```

---

## Features

### Task Model

Each task contains:

- Task ID
- Arrival offset (staggered arrival time)
- Task type (`CPU` or `IO`)
- Simulated execution duration
- Creation timestamp

### Workload Generation

Tasks are generated using a fixed random seed for reproducible results. Arrivals are staggered so tasks do not all enter the system at the same time.

### Concurrent Architecture

The program uses four components running concurrently:

- **Generator thread** — produces tasks and feeds them into the queues over time
- **Worker pool** — 8 worker threads that pull and execute tasks
- **Monitor thread** — samples active worker count every 10ms and writes to CSV
- **Metrics collector** — records wait time and turnaround time per task

Concurrency primitives used:

- `Arc` — shared ownership across threads
- `Mutex` — mutual exclusion for shared queues and counters
- `Condvar` — blocks workers efficiently until work is available
- `thread` — spawns all concurrent components

### Queue Design

The dispatcher uses a dual queue system:

- **CPU Queue** — holds CPU-bound tasks
- **IO Queue** — holds IO-bound tasks

This allows scheduling decisions to be made based on task type.

---

## Scheduling Policies

### FIFO

Tasks are processed in arrival order. IO tasks are checked first, then CPU tasks.

**Advantages:** Simple, predictable, fair by arrival time.

**Trade-offs:** Short tasks may wait behind long tasks, increasing average wait time.

### Optimized (SJF + IO-Prefer)

The scheduler scans both queues for the shortest available task. Ties are broken in favor of IO tasks.

**Advantages:** Reduces average wait time and improves throughput for short tasks.

**Trade-offs:** Longer CPU tasks may wait significantly longer, reducing fairness for CPU-bound work.

---

## Metrics Collected

- Total runtime
- Makespan
- Average wait time (overall, IO only, CPU only)
- Average turnaround time
- Maximum wait time
- CPU and IO task counts
- Average CPU usage
- Average workers active
- Monitor samples and CSV export

---

## Project Architecture

```
Task Generator Thread
        ↓
Shared Dual Queues (CPU Queue + IO Queue)
        ↓
Worker Pool (8 threads)
        ↓
Metrics Collector + Monitor Thread → monitor_log.csv
```

---

## Experiments

### Experiment A — FIFO Baseline

Configuration:

- 1000 tasks
- 70% IO tasks / 30% CPU tasks
- 8 workers
- FIFO scheduling

Result: FIFO kept all workers busy but produced higher average wait times as longer CPU tasks created bottlenecks in the queue.

### Experiment B — Optimized Scheduling

Configuration:

- 1000 tasks
- 70% IO tasks / 30% CPU tasks
- 8 workers
- Shortest Job First with IO preference

Result: Optimized scheduling reduced average wait time by ~14% compared to FIFO (7583ms vs 8793ms). IO tasks saw significantly lower wait times (4418ms avg) while CPU tasks waited longer (15294ms avg), reflecting the policy's trade-off between throughput and fairness.

---

## Synchronization Strategy

### `Mutex`

Protects the CPU queue, IO queue, active worker counter, and completed task results list.

### `Condvar`

Blocks worker threads while both queues are empty. Workers wake only when a new task arrives or shutdown is signaled. This eliminates busy waiting.

### `Arc`

Wraps shared state so the generator, workers, and monitor thread can all safely hold references to the same data.

---

## Shutdown Behavior

When the generator finishes producing all tasks, it sets a shared `done` flag to `true` and calls `notify_all()` on the condition variable. Workers check this flag inside their wait loop and exit cleanly once the queues are empty and `done` is set.

---

## Challenges Encountered

**Problem:** Worker threads remained blocked indefinitely after task generation ended, causing the program to hang.

**Fix:** A `done` flag was added to `SharedQueues`. After the last task is enqueued, the generator sets `done = true` and calls `notify_all()`, waking all blocked workers so they can exit.

---

## Future Improvements

- Worker-specific queues with work stealing
- Aging mechanism to reduce CPU task starvation
- Dynamic priority adjustment
- Bounded queue backpressure
- Real IO simulation using async I/O

---

## Tool Use Disclosure

**Tools used:** Claude (Anthropic) for code review and structural suggestions.

**Advice accepted:** Separating CPU and IO tasks into two distinct `VecDeque`s instead of one combined queue. This made the optimized scheduling policy cleaner and enabled per-class metrics without extra bookkeeping.

**Advice rejected:** Using `crossbeam-channel` for the generator-to-worker communication path. Stayed with `Mutex<VecDeque>` + `Condvar` to keep the project within standard-library concurrency primitives as required by the spec.
