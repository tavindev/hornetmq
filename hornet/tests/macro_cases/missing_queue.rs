use hornetmq::worker;
use hornetmq::Job;
use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Data {
    x: u32,
}

#[worker(concurrency = 5)]
fn process(job: &Job<Data>) -> Result<u32> {
    Ok(job.data.x)
}

fn main() {}
