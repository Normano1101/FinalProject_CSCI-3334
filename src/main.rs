// Concurrent Task Dispatcher
// Generator thread → dual queues (CPU/IO) → worker pool
// Monitor thread samples active workers every 10ms → monitor_log.csv
// Two experiments: FIFO baseline vs Optimized (SJF + IO-prefer)
 
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
 
// ─── Task model ────────────────────────────────────────────────────────────
 
#[derive(Clone, Debug, PartialEq)]
enum TaskKind {
    CPU,
    IO,
}
 
#[derive(Clone, Debug)]
struct Task {
    id: usize,
    arrival_offset_ms: u64,
    kind: TaskKind,
    duration_ms: u64,
    created_at: Instant,
}
 
// ─── Shared state ──────────────────────────────────────────────────────────
 
struct SharedQueues {
    cpu: VecDeque<Task>,
    io: VecDeque<Task>,
    done: bool,
    active_workers: usize,
}
 
impl SharedQueues {
    fn new() -> Self {
        SharedQueues { cpu: VecDeque::new(), io: VecDeque::new(), done: false, active_workers: 0 }
    }
    fn is_empty(&self) -> bool { self.cpu.is_empty() && self.io.is_empty() }
}
 
// ─── Completed task record ─────────────────────────────────────────────────
 
#[derive(Debug)]
struct CompletedTask {
    id: usize,
    kind: TaskKind,
    wait_ms: u64,
    turnaround_ms: u64,
}
 
// ─── Monitor sample ────────────────────────────────────────────────────────
 
struct MonitorSample {
    elapsed_ms: u64,
    active_workers: usize,
}
 
// ─── Task generator ────────────────────────────────────────────────────────
 
fn generate_tasks(count: usize, cpu_bias: f64, seed: u64) -> Vec<Task> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut tasks = Vec::with_capacity(count);
    let mut offset: u64 = 0;
    let placeholder = Instant::now();
 
    for i in 0..count {
        let kind = if rng.gen_bool(cpu_bias) { TaskKind::CPU } else { TaskKind::IO };
        let duration_ms = match kind {
            TaskKind::CPU => rng.gen_range(150..450),
            TaskKind::IO  => rng.gen_range(50..250),
        };
        offset += rng.gen_range(1u64..6);
        tasks.push(Task { id: i, arrival_offset_ms: offset, kind, duration_ms, created_at: placeholder });
    }
    tasks
}
 
// ─── Scheduling policies ───────────────────────────────────────────────────
 
fn pick_fifo(q: &mut SharedQueues) -> Option<Task> {
    q.io.pop_front().or_else(|| q.cpu.pop_front())
}
 
fn pick_optimized(q: &mut SharedQueues) -> Option<Task> {
    let best_io  = q.io.iter().enumerate().min_by_key(|(_, t)| t.duration_ms).map(|(i, t)| (i, t.duration_ms));
    let best_cpu = q.cpu.iter().enumerate().min_by_key(|(_, t)| t.duration_ms).map(|(i, t)| (i, t.duration_ms));
    match (best_io, best_cpu) {
        (None, None)                     => None,
        (Some((i, _)), None)             => q.io.remove(i),
        (None, Some((i, _)))             => q.cpu.remove(i),
        (Some((ii, id)), Some((ic, cd))) => if id <= cd { q.io.remove(ii) } else { q.cpu.remove(ic) },
    }
}
 
// ─── Worker thread ─────────────────────────────────────────────────────────
 
fn worker(
    worker_id: usize,
    shared: Arc<(Mutex<SharedQueues>, Condvar)>,
    results: Arc<Mutex<Vec<CompletedTask>>>,
    use_optimized: bool,
) {
    let (lock, cvar) = &*shared;
    loop {
        let task = {
            let mut q = lock.lock().unwrap();
            loop {
                if !q.is_empty() { break; }
                if q.done { return; }
                q = cvar.wait(q).unwrap();
            }
            let t = if use_optimized { pick_optimized(&mut q) } else { pick_fifo(&mut q) };
            if t.is_some() { q.active_workers += 1; }
            t
        };
 
        if let Some(t) = task {
            let wait_ms = Instant::now().duration_since(t.created_at).as_millis() as u64;
            thread::sleep(Duration::from_millis(t.duration_ms));
            let turnaround_ms = Instant::now().duration_since(t.created_at).as_millis() as u64;
 
            println!(
                "  [Worker {:>2}] Task {:>4} ({:?}, {:>3}ms) | wait {:>5}ms | turn {:>5}ms",
                worker_id, t.id, t.kind, t.duration_ms, wait_ms, turnaround_ms
            );
 
            lock.lock().unwrap().active_workers -= 1;
            results.lock().unwrap().push(CompletedTask { id: t.id, kind: t.kind, wait_ms, turnaround_ms });
        }
    }
}
 
// ─── Experiment runner ─────────────────────────────────────────────────────
 
struct ExperimentConfig {
    label: &'static str,
    task_count: usize,
    cpu_bias: f64,
    worker_count: usize,
    seed: u64,
    use_optimized: bool,
    csv_path: &'static str,
}
 
fn run_experiment(cfg: &ExperimentConfig) {
    let io_pct  = ((1.0 - cfg.cpu_bias) * 100.0).round() as usize;
    let cpu_pct = (cfg.cpu_bias * 100.0).round() as usize;
 
    println!("\n== {} ==", cfg.label);
    println!("{} tasks, {}% IO / {}% CPU, {} workers, cap 100%",
        cfg.task_count, io_pct, cpu_pct, cfg.worker_count);
 
    let tasks = generate_tasks(cfg.task_count, cfg.cpu_bias, cfg.seed);
    let shared = Arc::new((Mutex::new(SharedQueues::new()), Condvar::new()));
    let results: Arc<Mutex<Vec<CompletedTask>>> = Arc::new(Mutex::new(Vec::new()));
    let monitor_samples: Arc<Mutex<Vec<MonitorSample>>> = Arc::new(Mutex::new(Vec::new()));
 
    // Spawn workers
    let mut handles = Vec::new();
    for wid in 0..cfg.worker_count {
        let s = Arc::clone(&shared);
        let r = Arc::clone(&results);
        let opt = cfg.use_optimized;
        handles.push(thread::spawn(move || worker(wid, s, r, opt)));
    }
 
    // Monitor thread: samples active_workers every 10ms
    let mon_shared  = Arc::clone(&shared);
    let mon_samples = Arc::clone(&monitor_samples);
    let mon_start   = Instant::now();
    let mon_handle  = thread::spawn(move || {
        let (lock, _) = &*mon_shared;
        loop {
            thread::sleep(Duration::from_millis(10));
            let q = lock.lock().unwrap();
            let active = q.active_workers;
            let finished = q.done && q.is_empty();
            drop(q);
            mon_samples.lock().unwrap().push(MonitorSample {
                elapsed_ms: mon_start.elapsed().as_millis() as u64,
                active_workers: active,
            });
            if finished { break; }
        }
    });
 
    // Generator thread
    let gen_shared = Arc::clone(&shared);
    let wall_start = Instant::now();
 
    let gen_handle = thread::spawn(move || {
        let t0 = Instant::now();
        for mut task in tasks {
            let target = Duration::from_millis(task.arrival_offset_ms);
            let elapsed = t0.elapsed();
            if target > elapsed { thread::sleep(target - elapsed); }
            task.created_at = Instant::now();
            let (lock, cvar) = &*gen_shared;
            {
                let mut q = lock.lock().unwrap();
                match task.kind {
                    TaskKind::CPU => q.cpu.push_back(task),
                    TaskKind::IO  => q.io.push_back(task),
                }
            }
            cvar.notify_one();
        }
        let (lock, cvar) = &*gen_shared;
        lock.lock().unwrap().done = true;
        cvar.notify_all();
    });
 
    gen_handle.join().unwrap();
    for h in handles { h.join().unwrap(); }
    mon_handle.join().unwrap();
 
    let total_runtime_ms = wall_start.elapsed().as_millis() as u64;
    let makespan_ms = total_runtime_ms;
 
    // Metrics
    let completed = results.lock().unwrap();
    let n = completed.len() as f64;
 
    let avg_wait = completed.iter().map(|t| t.wait_ms).sum::<u64>() as f64 / n;
    let avg_turn = completed.iter().map(|t| t.turnaround_ms).sum::<u64>() as f64 / n;
    let (max_wait, max_wait_id) = completed.iter()
        .map(|t| (t.wait_ms, t.id))
        .max_by_key(|(w, _)| *w)
        .unwrap_or((0, 0));
    let cpu_done = completed.iter().filter(|t| t.kind == TaskKind::CPU).count();
    let io_done  = completed.iter().filter(|t| t.kind == TaskKind::IO).count();
 
    let io_avg_wait = {
        let v: Vec<u64> = completed.iter().filter(|t| t.kind == TaskKind::IO).map(|t| t.wait_ms).collect();
        if v.is_empty() { 0.0 } else { v.iter().sum::<u64>() as f64 / v.len() as f64 }
    };
    let cpu_avg_wait = {
        let v: Vec<u64> = completed.iter().filter(|t| t.kind == TaskKind::CPU).map(|t| t.wait_ms).collect();
        if v.is_empty() { 0.0 } else { v.iter().sum::<u64>() as f64 / v.len() as f64 }
    };
 
    let samples = monitor_samples.lock().unwrap();
    let sample_count = samples.len();
    let avg_active = if sample_count > 0 {
        samples.iter().map(|s| s.active_workers).sum::<usize>() as f64 / sample_count as f64
    } else { 0.0 };
    let avg_cpu_usage = avg_active / cfg.worker_count as f64 * 100.0;
 
    // Write CSV
    {
        let mut f = File::create(cfg.csv_path).unwrap();
        writeln!(f, "elapsed_ms,active_workers").unwrap();
        for s in samples.iter() {
            writeln!(f, "{},{}", s.elapsed_ms, s.active_workers).unwrap();
        }
    }
 
    // Print results — matching professor's format
    println!("\n— results —");
    println!("total runtime        : {} ms", total_runtime_ms);
    println!("makespan             : {} ms", makespan_ms);
    println!("tasks completed      : {}  (IO={}, CPU={})", completed.len(), io_done, cpu_done);
    println!("avg wait time        : {:.2} ms", avg_wait);
    if cfg.use_optimized {
        println!("avg wait (IO only)   : {:.2} ms", io_avg_wait);
        println!("avg wait (CPU only)  : {:.2} ms", cpu_avg_wait);
    }
    println!("avg turnaround time  : {:.2} ms", avg_turn);
    println!("max wait time        : {} ms (task #{})", max_wait, max_wait_id);
    println!("avg CPU usage        : {:.2} %", avg_cpu_usage);
    println!("avg workers active   : {:.2} / {}", avg_active, cfg.worker_count);
    println!("monitor samples      : {}", sample_count);
    println!("monitor csv          : {}", cfg.csv_path);
}
 
// ─── Main ──────────────────────────────────────────────────────────────────
 
fn main() {
    // Experiment A: FIFO — 1000 tasks, 70% IO / 30% CPU
    run_experiment(&ExperimentConfig {
        label:         "FIFO simulation",
        task_count:    1000,
        cpu_bias:      0.30,
        worker_count:  8,
        seed:          42,
        use_optimized: false,
        csv_path:      "monitor_log_fifo.csv",
    });
 
    // Experiment B: Optimized — same workload
    run_experiment(&ExperimentConfig {
        label:         "Optimized simulation",
        task_count:    1000,
        cpu_bias:      0.30,
        worker_count:  8,
        seed:          42,
        use_optimized: true,
        csv_path:      "monitor_log.csv",
    });
}
 