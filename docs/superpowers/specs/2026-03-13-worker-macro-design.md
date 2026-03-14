# Worker Macro Design

## Summary

Proc macro `#[worker(...)]` that generates a thin wrapper struct around `Worker<Data, Return>`, eliminating boilerplate for worker creation.

## API

```rust
#[worker(
    queue = "emails",
    concurrency = 5,        // default: 1
    retry = 3,              // default: 0
    backoff = "fixed(1000)",// or "exponential(1000, 30000)", default: none
    lock_duration = 60000,  // default: 30000
)]
fn process_email(job: &Job<EmailData>) -> Result<String> {
    // processing logic
}
```

## Generated Code

```rust
fn process_email(job: &Job<EmailData>) -> Result<String> { /* preserved */ }

pub struct ProcessEmailWorker {
    inner: hornetmq::Worker<EmailData, String>,
}

impl ProcessEmailWorker {
    pub fn new(redis_url: &str) -> Self {
        let worker = hornetmq::Worker::new(
            "emails".to_string(),
            redis_url.to_string(),
            5,
            process_email,
        )
        .with_lock_duration(60000)
        .with_backoff(hornetmq::BackoffStrategy::Fixed(1000));
        Self { inner: worker }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.inner.run().await
    }
}
```

## Type Inference

- `Data`: extracted from `job: &Job<EmailData>` → `EmailData`
- `Return`: extracted from `-> Result<String>` → `String`

## Backoff String Parsing

Parsed at compile time by proc macro:
- `"fixed(1000)"` → `BackoffStrategy::Fixed(1000)`
- `"exponential(1000, 30000)"` → `BackoffStrategy::Exponential { base: 1000, max: 30000 }`

Invalid formats produce compile errors.

## Naming

Struct name = PascalCase(fn_name) + `Worker`. `process_email` → `ProcessEmailWorker`.

## Worker Changes

- Add `default_backoff: Option<BackoffStrategy>` field
- Add `with_backoff(self, strategy: BackoffStrategy) -> Self` builder method
- Retry logic uses `job.opts.backoff.or(self.default_backoff)` for delay computation

## Defaults

| Param | Default |
|-------|---------|
| concurrency | 1 |
| retry | 0 |
| backoff | none |
| lock_duration | 30000 |
