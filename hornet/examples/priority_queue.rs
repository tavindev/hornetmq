//! Demonstrates priority-based job processing.
//!
//! Enqueues jobs with different priorities. Lower number = higher priority.
//!
//! ```sh
//! cargo run --example priority_queue
//! ```

use anyhow::Result;
use hornetmq::{
    core::job::Job,
    queue::{AddJobOptions, Queue},
    worker::Worker,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct Alert {
    severity: String,
    message: String,
}

fn alert_handler(job: &Job<Alert>) -> Result<String> {
    println!(
        "[job {} | priority={}] [{:>8}] {}",
        job.id, job.priority, job.data.severity, job.data.message
    );
    Ok("handled".into())
}

#[tokio::main]
async fn main() {
    let redis_url = "redis://localhost:6379".to_string();
    let queue_name = "alerts".to_string();

    let mut queue = Queue::new(queue_name.clone(), redis_url.clone());

    // Enqueue with different priorities (lower = processed first)
    let alerts = vec![
        ("info", "Disk usage at 60%", 10),
        ("critical", "Database connection lost", 1),
        ("warning", "Memory usage high", 5),
        ("critical", "API latency >5s", 1),
        ("info", "Deployment started", 10),
        ("warning", "Queue backlog growing", 5),
    ];

    for (severity, message, priority) in &alerts {
        let id = queue
            .add(
                "alert",
                Alert {
                    severity: severity.to_string(),
                    message: message.to_string(),
                },
                AddJobOptions {
                    priority: Some(*priority),
                    ..Default::default()
                },
            )
            .unwrap();
        println!("Enqueued job {id} (priority={priority}): [{severity}] {message}");
    }

    println!("\nProcessing (critical first, then warning, then info):\n");

    // Process with concurrency=1 to see priority ordering
    let mut worker = Worker::new(queue_name, redis_url, 1, alert_handler);
    let shutdown = worker.shutdown_flag();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        shutdown.store(true, Ordering::SeqCst);
    });

    worker.run().await.unwrap();
}
