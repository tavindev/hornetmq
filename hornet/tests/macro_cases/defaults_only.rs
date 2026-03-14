use hornetmq::worker;
use hornetmq::Job;
use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct SimpleData {
    msg: String,
}

#[worker(queue = "simple")]
fn handle_simple(job: &Job<SimpleData>) -> Result<String> {
    Ok(job.data.msg.clone())
}

fn main() {
    // defaults: concurrency=1, retry=0, no backoff, lock_duration=30000
    let _w = HandleSimpleWorker::new("redis://localhost:6379").unwrap();
}
