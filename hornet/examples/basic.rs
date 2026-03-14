//! Basic producer/consumer example.
//!
//! 1. Enqueues 5 jobs via Queue
//! 2. Processes them with a Worker
//!
//! ```sh
//! cargo run --example basic
//! ```

use anyhow::Result;
use hornet::{
    core::job::Job,
    queue::{AddJobOptions, Queue},
    worker::Worker,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

fn send_email(job: &Job<Email>) -> Result<String> {
    println!(
        "[job {}] Sending email to {} — \"{}\"",
        job.id, job.data.to, job.data.subject
    );
    Ok(format!("sent to {}", job.data.to))
}

#[tokio::main]
async fn main() {
    let redis_url = "redis://localhost:6379".to_string();
    let queue_name = "emails".to_string();

    // --- Producer ---
    let mut queue = Queue::new(queue_name.clone(), redis_url.clone());

    let recipients = vec![
        ("alice@example.com", "Welcome!"),
        ("bob@example.com", "Your order shipped"),
        ("carol@example.com", "Password reset"),
        ("dave@example.com", "Invoice #1234"),
        ("eve@example.com", "Weekly digest"),
    ];

    for (to, subject) in &recipients {
        let id = queue
            .add(
                "send-email",
                Email {
                    to: to.to_string(),
                    subject: subject.to_string(),
                },
                AddJobOptions::default(),
            )
            .unwrap();
        println!("Enqueued job {id}");
    }

    // --- Consumer ---
    let mut worker = Worker::new(queue_name, redis_url, 2, send_email);
    let shutdown = worker.shutdown_flag();

    // Auto-shutdown after 3 seconds
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        shutdown.store(true, Ordering::SeqCst);
    });

    println!("\nWorker started (concurrency=2), shutting down in 3s...\n");
    worker.run().await.unwrap();
    println!("\nWorker shut down gracefully.");
}
