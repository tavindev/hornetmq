//! Demonstrates graceful shutdown with SIGINT/SIGTERM.
//!
//! Starts a long-running worker. Press Ctrl+C to trigger graceful shutdown —
//! the worker will finish processing active jobs before exiting.
//!
//! ```sh
//! cargo run --example graceful_shutdown
//! ```

use anyhow::Result;
use hornetmq::{
    core::job::Job,
    queue::{AddJobOptions, Queue},
    worker::Worker,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    message: String,
}

fn slow_processor(job: &Job<Payload>) -> Result<String> {
    println!("[job {}] Processing: {}", job.id, job.data.message);
    // Simulate slow work
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("[job {}] Done.", job.id);
    Ok("ok".into())
}

#[tokio::main]
async fn main() {
    let redis_url = "redis://localhost:6379";
    let queue_name = "shutdown-demo";

    // Enqueue some jobs
    let mut queue = Queue::new(queue_name, redis_url).unwrap();
    for i in 1..=10 {
        queue
            .add(
                "slow-task",
                Payload {
                    message: format!("Task #{i}"),
                },
                AddJobOptions::default(),
            )
            .unwrap();
    }
    println!("Enqueued 10 jobs.\n");

    // Worker with concurrency 3 — handles SIGINT/SIGTERM automatically
    let mut worker = Worker::new(queue_name, redis_url, 3, slow_processor).unwrap();

    println!("Worker running (concurrency=3). Press Ctrl+C to shut down gracefully.\n");
    worker.run().await.unwrap();
    println!("\nAll active jobs drained. Goodbye.");
}
