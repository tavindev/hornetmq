use hornetmq::worker;
use hornetmq::Job;
use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct EmailData {
    to: String,
}

#[worker(queue = "emails", concurrency = 5)]
fn process_email(job: &Job<EmailData>) -> Result<String> {
    Ok(format!("sent to {}", job.data.to))
}

fn main() {
    // Verify the struct was generated with correct name
    let _w = ProcessEmailWorker::new("redis://localhost:6379").unwrap();
}
