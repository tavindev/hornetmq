# Hornet

A fast, Redis-backed job queue for Rust. **Fully compatible with [BullMQ](https://docs.bullmq.io/)** — use Hornet as a worker/producer alongside existing BullMQ (Node.js) producers/workers, or use it standalone.

## Features

- **Queue** (producer) — enqueue jobs with priority, delay, retry options
- **Worker** (consumer) — process jobs with configurable concurrency
- **Retry with backoff** — fixed or exponential backoff strategies
- **Graceful shutdown** — SIGINT/SIGTERM handling, drains active jobs before exit
- **Stalled job detection** — recovers jobs whose workers crashed mid-processing
- **Job events** — state transitions published to Redis streams
- **Priority queues** — lower number = higher priority
- **BullMQ compatible** — uses the same Redis data structures and Lua scripts

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
hornet = { git = "https://github.com/tavindev/hornet" }
```

Requires a running Redis instance.

## Quick Start

### Producer

```rust
use hornet::queue::{Queue, AddJobOptions};
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
use hornet::{core::job::Job, worker::Worker};
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

## Job Options

```rust
use hornet::queue::AddJobOptions;
use hornet::core::backoff::BackoffStrategy;

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
    .with_lock_duration(60_000); // lock duration in ms (default: 30s)
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

Hornet uses the same Redis keys, Lua scripts, and data format as BullMQ. You can:

- Enqueue jobs with BullMQ (Node.js), process with Hornet (Rust)
- Enqueue jobs with Hornet, process with BullMQ
- Mix producers and consumers across both

Queue key format: `bull:{queue_name}:{key}`

## Architecture

```
hornet/
  src/
    core/           # Pure domain logic (no Redis dependency)
      backoff.rs    # BackoffStrategy + compute_delay()
      retry.rs      # should_retry(), next_retry_delay()
      stall.rs      # is_stalled()
      events.rs     # JobEvent enum
      job.rs        # Job, JobOptions, JobBuilder
    scripts/        # Redis Lua script wrappers
    queue.rs        # Producer
    worker.rs       # Consumer
```

Core logic is separated from infrastructure for testability. Unit tests cover core logic without Redis; integration tests run against a real Redis instance.

## Examples

```sh
cargo run --example basic               # producer + consumer end-to-end
cargo run --example retry_backoff        # exponential backoff retry
cargo run --example graceful_shutdown    # Ctrl+C graceful drain
cargo run --example priority_queue       # priority-based processing
```

## License

MIT
