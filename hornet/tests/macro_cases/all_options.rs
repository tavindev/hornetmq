use hornetmq::worker;
use hornetmq::Job;
use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payload {
    value: u32,
}

#[worker(
    queue = "tasks",
    concurrency = 10,
    backoff = "exponential(1000, 30000)",
    lock_duration = 60000,
    limiter = "100, 5000"
)]
fn handle_task(job: &Job<Payload>) -> Result<u32> {
    Ok(job.data.value * 2)
}

fn main() {
    let _w = HandleTaskWorker::new("redis://localhost:6379").unwrap();
}
