# Hornet DDD Restructure + Feature Completion

## Module Structure

```
hornet/src/
  core/
    mod.rs
    job.rs          # Job, JobOptions, JobState
    backoff.rs      # BackoffStrategy + compute_delay(attempt) -> Duration
    retry.rs        # RetryPolicy — should_retry(attempts, max), next_delay(strategy, attempt)
    stall.rs        # is_stalled(lock_expires_at, now) -> bool
    events.rs       # JobEvent enum

  infra/
    mod.rs
    redis_connection.rs   # connection wrapper
    scripts/
      mod.rs
      loader.rs           # existing script loader (unchanged)
      macros.rs           # existing generate_script_struct (unchanged)
      add_standard_job.rs # AddStandardJob::run() — NEW
      move_to_active.rs   # existing (move here)
      move_to_finished.rs # existing (move here)
      retry_job.rs        # existing (move here)
      commands/           # .lua files (unchanged)

  queue.rs          # Queue::add(name, data, opts) -> Result<JobId>
  worker.rs         # Worker with shutdown, stall detection, backoff
  queue_keys.rs     # existing (unchanged)
  lib.rs
```

## Shared Kernel (core/)

### job.rs
- `Job<Data>` — existing struct, move here
- `JobOptions` — add `backoff: Option<BackoffStrategy>`, `delay: u64`
- `JobState` enum: `Waiting, Delayed, Active, Completed, Failed`
- `JobBuilder<Data>` — existing, move here

### backoff.rs
- `BackoffStrategy` enum: `Fixed(u64)`, `Exponential { base: u64, max: u64 }`
- `compute_delay(strategy: &BackoffStrategy, attempt: u32) -> u64` (returns ms)
- Fixed: always returns the fixed value
- Exponential: `min(base * 2^(attempt-1), max)`

### retry.rs
- `should_retry(attempts_made: u32, max_attempts: u32) -> bool`
- `next_retry_delay(strategy: &Option<BackoffStrategy>, attempt: u32) -> u64`
  - None strategy → 0 (immediate)

### stall.rs
- `is_stalled(lock_expires_at: u64, now: u64) -> bool`
  - stalled if now > lock_expires_at

### events.rs
- `JobEvent` enum: `Added, Active, Completed, Failed, Retrying, Stalled`
- Pure data, no behavior — infra publishes these to Redis streams

## Queue Domain (queue.rs)

```rust
pub struct Queue {
    name: String,
    client: redis::Client,
    prefix: String,
}

impl Queue {
    pub fn new(name: String, redis_url: String) -> Self;
    pub fn add<D: Serialize>(&self, job_name: &str, data: D, opts: AddJobOptions) -> Result<String>;
}

pub struct AddJobOptions {
    pub delay: Option<u64>,
    pub priority: Option<u32>,
    pub attempts: Option<u32>,
    pub backoff: Option<BackoffStrategy>,
}
```

Uses `AddStandardJob` lua script under the hood. Returns job ID.

## Worker Domain (worker.rs)

### Existing behavior preserved
- Concurrency via tokio tasks + mpsc channel
- moveToActive / moveToFinished / retryJob lua scripts

### New: Backoff on retry
- When retry needed, compute delay via `core::retry::next_retry_delay()`
- If delay > 0, move job to delayed set with score = now + delay (use moveToFinished with delayed target)
- If delay == 0, use existing retryJob (immediate re-queue)

### New: Graceful shutdown
- `Worker::shutdown()` method sets atomic bool
- Main loop checks flag, stops accepting new jobs
- Waits for active_tasks to drain to 0
- `run()` returns after shutdown complete
- Register SIGTERM/SIGINT handler that calls shutdown

### New: Stalled job detection
- Background tokio task, runs every `stall_interval` (default: lock_duration / 2)
- Checks active jobs whose locks have expired
- For stalled jobs: re-queue or fail based on max attempts
- Uses existing retryJob or moveToFinished scripts

### New: Job events
- After each state transition, publish event to Redis stream (`{prefix}events`)
- Events: `added`, `active`, `completed`, `failed`, `retrying`, `stalled`
- Use XADD on the events key

## Testing Strategy

### Unit tests (core/) — no Redis
- backoff: compute_delay returns correct values for fixed/exponential
- retry: should_retry boundary conditions, next_retry_delay calculations
- stall: is_stalled with various time comparisons
- job: JobBuilder builds correctly, JobState transitions

### Integration tests (infra/ + queue + worker) — real Redis
- Queue::add creates job in Redis, verify with HGETALL
- Worker processes job end-to-end
- Worker retries failed job with backoff (verify delay in sorted set)
- Worker graceful shutdown (send shutdown, verify drains)
- Stalled job detection (create job with expired lock, verify re-queued)
- Queue + Worker interop (add job, worker picks up, completes)

All integration tests use a unique queue prefix to avoid collisions. Cleanup after each test.

## Agent Decomposition

3 parallel agents working on vertical slices:

1. **Shared Kernel Agent**: core/ module — job.rs, backoff.rs, retry.rs, stall.rs, events.rs + unit tests
2. **Queue Agent**: Queue struct, AddStandardJob::run(), integration tests
3. **Worker Agent**: restructure worker to use core/, add backoff, graceful shutdown, stall detection, events, integration tests

Kernel agent goes first (other agents depend on core types). Queue and Worker agents run in parallel after.
