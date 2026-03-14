//! Demonstrates retry with exponential backoff.
//!
//! Enqueues a job that fails twice, then succeeds on the 3rd attempt.
//!
//! ```sh
//! cargo run --example retry_backoff
//! ```

use anyhow::Result;
use hornetmq::{
    core::{backoff::BackoffStrategy, job::Job},
    queue::{AddJobOptions, Queue},
    worker::Worker,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct Task {
    name: String,
}

static ATTEMPT_COUNT: AtomicU32 = AtomicU32::new(0);

fn flaky_processor(job: &Job<Task>) -> Result<String> {
    let attempt = ATTEMPT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
    println!(
        "[job {}] Attempt {attempt} for \"{}\"",
        job.id, job.data.name
    );

    if attempt < 3 {
        println!("[job {}] Failed! Will retry with backoff...", job.id);
        anyhow::bail!("simulated failure on attempt {attempt}");
    }

    println!("[job {}] Success on attempt {attempt}!", job.id);
    Ok("done".into())
}

#[tokio::main]
async fn main() {
    let redis_url = "redis://localhost:6379".to_string();
    let queue_name = "retry-demo".to_string();

    // Enqueue a job with 5 attempts and exponential backoff (1s base, 10s max)
    let mut queue = Queue::new(queue_name.clone(), redis_url.clone());
    let id = queue
        .add(
            "flaky-task",
            Task {
                name: "import-data".into(),
            },
            AddJobOptions {
                attempts: Some(5),
                backoff: Some(BackoffStrategy::Exponential {
                    base: 1000,
                    max: 10_000,
                }),
                ..Default::default()
            },
        )
        .unwrap();

    println!("Enqueued job {id} with 5 attempts + exponential backoff\n");

    // Run worker — will process, fail twice, then succeed
    let mut worker = Worker::new(queue_name, redis_url, 1, flaky_processor);
    let shutdown = worker.shutdown_flag();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        shutdown.store(true, Ordering::SeqCst);
    });

    worker.run().await.unwrap();
    println!("\nDone.");
}
