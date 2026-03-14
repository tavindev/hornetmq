use anyhow::Result;
use hornetmq::{core::job::Job, queue::AddJobOptions, worker::Worker, BackoffStrategy, Queue};
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::process::Command;
use std::sync::atomic::Ordering;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct TestData {
    value: String,
}

fn unique_queue_name() -> String {
    format!("test-compat-{}", Uuid::new_v4())
}

fn prefix_for(queue_name: &str) -> String {
    format!("bull:{queue_name}:")
}

fn cleanup_queue(conn: &mut redis::Connection, queue_name: &str) {
    let prefix = prefix_for(queue_name);
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}*"))
        .query(conn)
        .unwrap_or_default();
    for key in keys {
        let _: () = conn.del(&key).unwrap_or(());
    }
}

fn scripts_dir() -> String {
    format!(
        "{}/tests/fixtures/scripts",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// Use real BullMQ to add a job. Returns the job ID.
fn bullmq_produce(queue_name: &str, job_name: &str, data_json: &str, opts_json: &str) -> String {
    let output = Command::new("bun")
        .arg("run")
        .arg(format!("{}/bullmq_produce.mjs", scripts_dir()))
        .arg(queue_name)
        .arg(job_name)
        .arg(data_json)
        .arg(opts_json)
        .output()
        .expect("failed to run bullmq_produce.mjs — is bun installed?");

    assert!(
        output.status.success(),
        "bullmq_produce failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// Use real BullMQ to read a job by ID. Returns the parsed JSON.
fn bullmq_read_job(queue_name: &str, job_id: &str) -> serde_json::Value {
    let output = Command::new("bun")
        .arg("run")
        .arg(format!("{}/bullmq_read_job.mjs", scripts_dir()))
        .arg(queue_name)
        .arg(job_id)
        .output()
        .expect("failed to run bullmq_read_job.mjs — is bun installed?");

    assert!(
        output.status.success(),
        "bullmq_read_job failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();
    assert_ne!(trimmed, "NOT_FOUND", "BullMQ could not find the job");
    serde_json::from_str(trimmed).unwrap()
}

/// Use real BullMQ Worker to consume a job. Returns the processed job JSON.
fn bullmq_consume(queue_name: &str, timeout_ms: u64) -> Option<serde_json::Value> {
    let output = Command::new("bun")
        .arg("run")
        .arg(format!("{}/bullmq_consume.mjs", scripts_dir()))
        .arg(queue_name)
        .arg(timeout_ms.to_string())
        .output()
        .expect("failed to run bullmq_consume.mjs — is bun installed?");

    assert!(
        output.status.success(),
        "bullmq_consume failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();
    if trimmed == "TIMEOUT" {
        return None;
    }
    Some(serde_json::from_str(trimmed).unwrap())
}

fn success_processor(job: &Job<TestData>) -> Result<String> {
    Ok(format!("processed:{}", job.data.value))
}

// ============================================================
// Producer side: HornetMQ → BullMQ readable
// ============================================================

#[test]
fn hornet_job_readable_by_bullmq() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    let job_id = queue
        .add(
            "compat-job",
            serde_json::json!({"value": "hello-from-hornet"}),
            AddJobOptions::default(),
        )
        .unwrap();

    // Use real BullMQ to read the job HornetMQ created
    let job = bullmq_read_job(&queue_name, &job_id);

    assert_eq!(job["name"], "compat-job");
    assert_eq!(job["data"]["value"], "hello-from-hornet");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

#[test]
fn hornet_job_with_backoff_readable_by_bullmq() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    let job_id = queue
        .add(
            "backoff-job",
            serde_json::json!({"x": 1}),
            AddJobOptions {
                attempts: Some(3),
                backoff: Some(BackoffStrategy::Fixed(5000)),
                ..Default::default()
            },
        )
        .unwrap();

    let job = bullmq_read_job(&queue_name, &job_id);

    assert_eq!(job["name"], "backoff-job");
    // BullMQ should see the backoff in its expected format
    assert_eq!(job["opts"]["backoff"]["type"], "fixed");
    assert_eq!(job["opts"]["backoff"]["delay"], 5000);
    assert_eq!(job["opts"]["attempts"], 3);

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

#[test]
fn hornet_job_consumable_by_bullmq_worker() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    queue
        .add(
            "consume-test",
            serde_json::json!({"value": "for-bullmq"}),
            AddJobOptions::default(),
        )
        .unwrap();

    // Use real BullMQ Worker to consume the job
    let result = bullmq_consume(&queue_name, 10000);
    assert!(result.is_some(), "BullMQ worker should have consumed the job");

    let job = result.unwrap();
    assert_eq!(job["name"], "consume-test");
    assert_eq!(job["data"]["value"], "for-bullmq");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

// ============================================================
// Consumer side: BullMQ → HornetMQ readable
// ============================================================

#[tokio::test]
async fn hornet_reads_bullmq_created_job() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    // Use real BullMQ to produce a job with backoff and extra fields
    bullmq_produce(
        &queue_name,
        "bullmq-job",
        r#"{"value":"from-bullmq"}"#,
        r#"{"attempts":3,"backoff":{"type":"fixed","delay":2000},"removeOnComplete":true}"#,
    );

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        success_processor,
    );

    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.store(true, Ordering::SeqCst);
    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let prefix = prefix_for(&queue_name);

    // removeOnComplete:true means the job is removed from the completed set after processing.
    // Verify it was processed by checking that wait and active are empty (job was consumed).
    let wait_key = format!("{prefix}wait");
    let active_key = format!("{prefix}active");
    let wait_len: u64 = redis::cmd("LLEN")
        .arg(&wait_key)
        .query(&mut conn)
        .unwrap();
    let active_len: u64 = redis::cmd("LLEN")
        .arg(&active_key)
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        wait_len, 0,
        "Wait list should be empty after job is processed"
    );
    assert_eq!(
        active_len, 0,
        "Active list should be empty after job is processed"
    );

    // With removeOnComplete:true, the completed set should be empty too
    let completed_key = format!("{prefix}completed");
    let members: Vec<String> = conn.zrange(&completed_key, 0, -1).unwrap();
    assert!(
        members.is_empty(),
        "With removeOnComplete:true, completed set should be empty, got: {members:?}"
    );

    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn hornet_reads_bullmq_job_without_backoff() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    bullmq_produce(
        &queue_name,
        "simple-job",
        r#"{"value":"no-backoff"}"#,
        r#"{"attempts":1}"#,
    );

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        success_processor,
    );

    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.store(true, Ordering::SeqCst);
    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let prefix = prefix_for(&queue_name);
    let completed_key = format!("{prefix}completed");
    let members: Vec<String> = conn.zrange(&completed_key, 0, -1).unwrap();
    assert!(
        !members.is_empty(),
        "HornetMQ should have completed the BullMQ job without backoff"
    );

    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn hornet_reads_bullmq_job_minimal_opts() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    // BullMQ with empty opts
    bullmq_produce(
        &queue_name,
        "minimal-job",
        r#"{"value":"minimal"}"#,
        r#"{}"#,
    );

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        success_processor,
    );

    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.store(true, Ordering::SeqCst);
    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let prefix = prefix_for(&queue_name);
    let completed_key = format!("{prefix}completed");
    let members: Vec<String> = conn.zrange(&completed_key, 0, -1).unwrap();
    assert!(
        !members.is_empty(),
        "HornetMQ should have completed the BullMQ job with minimal opts"
    );

    cleanup_queue(&mut conn, &queue_name);
}
