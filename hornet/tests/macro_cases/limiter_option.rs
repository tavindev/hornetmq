use hornetmq::worker;
use hornetmq::Job;
use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payload {
    value: u32,
}

#[worker(
    queue = "rate-limited-queue",
    concurrency = 4,
    limiter = "10, 1000"
)]
fn rate_limited_task(job: &Job<Payload>) -> Result<u32> {
    Ok(job.data.value)
}

fn main() {
    let _w = RateLimitedTaskWorker::new("redis://localhost:6379");
}
