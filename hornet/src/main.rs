use anyhow::Result;
use hornetmq::{core::job::Job, worker::Worker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct ProcessorData {
    name: String,
    age: u8,
}

fn test_processor(job: &Job<ProcessorData>) -> Result<String> {
    println!("Processing: {job:?}");

    Ok("Done".to_string())
}

#[tokio::main]
async fn main() {
    let mut worker = Worker::new(
        "new-queue",
        "redis://localhost:6379",
        1,
        test_processor,
    )
    .unwrap();

    worker.run().await.unwrap();
}
