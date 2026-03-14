<p align="center">
  <img src="assets/icon-256.png" alt="HornetMQ" width="128" />
</p>

<h1 align="center">HornetMQ</h1>

A fast, Redis-backed job queue for Rust. **Fully compatible with [BullMQ](https://docs.bullmq.io/)** — use HornetMQ as a worker/producer alongside existing BullMQ (Node.js) producers/workers, or use it standalone.

## Features

- **Queue** (producer) — enqueue jobs with priority, delay, retry options
- **Worker** (consumer) — process jobs with configurable concurrency
- **Retry with backoff** — fixed or exponential backoff strategies
- **Graceful shutdown** — SIGINT/SIGTERM handling, drains active jobs before exit
- **Stalled job detection** — recovers jobs whose workers crashed mid-processing
- **Job events** — state transitions published to Redis streams
- **Priority queues** — lower number = higher priority
- **Worker macro** — `#[worker]` attribute macro for zero-boilerplate worker creation
- **BullMQ compatible** — uses the same Redis data structures and Lua scripts

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
hornetmq = { git = "https://github.com/tavindev/hornetmq" }
```

Requires a running Redis instance.

## Quick Start

### Producer

```rust
use hornetmq::queue::{Queue, AddJobOptions};
use serde::Serialize;

#[derive(Serialize)]
struct Email {
    to: String,
    subject: String,
}

fn main() {
    let mut queue = Queue::new(
        "emails".to_string(),
        "redis://localhost:6379".to_string(),
    );

    let job_id = queue.add(
        "send-email",
        Email { to: "user@example.com".into(), subject: "Hello!".into() },
        AddJobOptions::default(),
    ).unwrap();

    println!("Enqueued job {job_id}");
}
```

### Worker

```rust
use anyhow::Result;
use hornetmq::{core::job::Job, worker::Worker};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

fn send_email(job: &Job<Email>) -> Result<String> {
    println!("Sending to {}: {}", job.data.to, job.data.subject);
    Ok("sent".into())
}

#[tokio::main]
async fn main() {
    let mut worker = Worker::new(
        "emails".to_string(),
        "redis://localhost:6379".to_string(),
        4, // concurrency
        send_email,
    );

    worker.run().await.unwrap();
}
```

The worker handles SIGINT/SIGTERM automatically — press Ctrl+C and it will finish active jobs before exiting.

### Worker Macro

Use the `#[worker]` attribute macro to define workers with zero boilerplate:

```rust
use anyhow::Result;
use hornetmq::{worker, Job};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

#[worker(queue = "emails", concurrency = 4)]
fn send_email(job: &Job<Email>) -> Result<String> {
    println!("Sending to {}: {}", job.data.to, job.data.subject);
    Ok("sent".into())
}

#[tokio::main]
async fn main() {
    let mut worker = SendEmailWorker::new("redis://localhost:6379");
    worker.run().await.unwrap();
}
```

The macro generates a `SendEmailWorker` struct (PascalCase function name + `Worker`) with `new(redis_url)` and `async run()`. Data and return types are inferred from the function signature.

#### Macro Options

```rust
#[worker(
    queue = "tasks",              // required — queue name
    concurrency = 10,             // default: 1
    retry = 5,                    // default: 0
    backoff = "fixed(1000)",      // default: none
    lock_duration = 60000,        // default: 30000 (ms)
)]
fn handle_task(job: &Job<Payload>) -> Result<String> { /* ... */ }
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `queue` | string | *required* | Redis queue name |
| `concurrency` | integer | `1` | Max concurrent job processors |
| `retry` | integer | `0` | Max retry attempts |
| `backoff` | string | none | `"fixed(<ms>)"` or `"exponential(<base>, <max>)"` |
| `lock_duration` | integer | `30000` | Job lock duration in ms |

## Job Options

```rust
use hornetmq::queue::AddJobOptions;
use hornetmq::core::backoff::BackoffStrategy;

let opts = AddJobOptions {
    attempts: Some(5),                          // max retry attempts
    delay: Some(60_000),                        // delay 60s before first processing
    priority: Some(1),                          // lower = higher priority
    backoff: Some(BackoffStrategy::Exponential { // retry backoff
        base: 1000,                             // 1s, 2s, 4s, 8s...
        max: 30_000,                            // capped at 30s
    }),
};
```

### Backoff Strategies

| Strategy | Behavior |
|----------|----------|
| `BackoffStrategy::Fixed(ms)` | Same delay every retry |
| `BackoffStrategy::Exponential { base, max }` | `base * 2^(attempt-1)`, capped at `max` |
| `None` (default) | Immediate retry |

## Worker Configuration

```rust
let mut worker = Worker::new(queue_name, redis_url, concurrency, processor_fn)
    .with_lock_duration(60_000)                                    // lock duration in ms (default: 30s)
    .with_backoff(BackoffStrategy::Exponential { base: 1000, max: 30_000 }); // default backoff for jobs without one
```

### Programmatic Shutdown

```rust
let shutdown = worker.shutdown_flag();

// From another task or thread:
shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
```

### Stall Detection

The worker automatically detects stalled jobs — jobs that were being processed when a worker crashed. It checks every `lock_duration / 2` milliseconds. Stalled jobs are re-queued if retries remain, or moved to failed.

## BullMQ Compatibility

HornetMQ uses the same Redis keys, Lua scripts, and data format as BullMQ. You can:

- Enqueue jobs with BullMQ (Node.js), process with HornetMQ (Rust)
- Enqueue jobs with HornetMQ, process with BullMQ
- Mix producers and consumers across both

Queue key format: `bull:{queue_name}:{key}`

## Examples

```sh
cargo run --example basic               # producer + consumer end-to-end
cargo run --example retry_backoff        # exponential backoff retry
cargo run --example graceful_shutdown    # Ctrl+C graceful drain
cargo run --example priority_queue       # priority-based processing
```

## Benchmarks

```sh
cargo bench                        # run all benchmarks
cargo bench --bench core_benchmarks    # core logic (backoff, retry, stall)
cargo bench --bench queue_benchmarks   # enqueue throughput + payload sizes
cargo bench --bench worker_benchmarks  # end-to-end worker throughput
```

Requires a running Redis instance for queue and worker benchmarks. Results are saved to `target/criterion/` with HTML reports.

## License

MIT
