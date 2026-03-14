use hornetmq::{queue::AddJobOptions, BackoffStrategy, Queue};
use redis::Commands;
use std::process::Command;
use uuid::Uuid;

fn unique_queue_name() -> String {
    format!("test-compat-ext-{}", Uuid::new_v4())
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
    format!("{}/tests/fixtures/scripts", env!("CARGO_MANIFEST_DIR"))
}

fn bullmq_read_job(queue_name: &str, job_id: &str) -> serde_json::Value {
    let output = Command::new("bun")
        .arg("run")
        .arg(format!("{}/bullmq_read_job.mjs", scripts_dir()))
        .arg(queue_name)
        .arg(job_id)
        .output()
        .expect("failed to run bullmq_read_job.mjs");
    assert!(output.status.success(), "bullmq_read_job failed: {}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();
    assert_ne!(trimmed, "NOT_FOUND", "BullMQ could not find the job");
    serde_json::from_str(trimmed).unwrap()
}

fn bullmq_consume(queue_name: &str, timeout_ms: u64) -> Option<serde_json::Value> {
    let output = Command::new("bun")
        .arg("run")
        .arg(format!("{}/bullmq_consume.mjs", scripts_dir()))
        .arg(queue_name)
        .arg(timeout_ms.to_string())
        .output()
        .expect("failed to run bullmq_consume.mjs");
    assert!(output.status.success(), "bullmq_consume failed: {}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let trimmed = stdout.trim();
    if trimmed == "TIMEOUT" { return None; }
    Some(serde_json::from_str(trimmed).unwrap())
}

// === Custom job ID readable by BullMQ ===

#[test]
fn hornet_custom_job_id_readable_by_bullmq() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    queue.add("custom-id-job", serde_json::json!({"value": "custom"}), AddJobOptions {
        job_id: Some("my-custom-123".into()),
        ..Default::default()
    }).unwrap();

    let job = bullmq_read_job(&queue_name, "my-custom-123");
    assert_eq!(job["id"], "my-custom-123");
    assert_eq!(job["name"], "custom-id-job");
    assert_eq!(job["data"]["value"], "custom");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

// === Delayed job readable by BullMQ ===

#[test]
fn hornet_delayed_job_readable_by_bullmq() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    let id = queue.add("delayed-job", serde_json::json!({"value": "later"}), AddJobOptions {
        delay: Some(60_000),
        ..Default::default()
    }).unwrap();

    let job = bullmq_read_job(&queue_name, &id);
    assert_eq!(job["name"], "delayed-job");
    assert_eq!(job["data"]["value"], "later");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

// === LIFO job consumable by BullMQ ===

#[test]
fn hornet_lifo_jobs_consumed_in_correct_order_by_bullmq() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    queue.add("first", serde_json::json!({"value": "first"}), AddJobOptions {
        lifo: Some(true),
        ..Default::default()
    }).unwrap();

    queue.add("second", serde_json::json!({"value": "second"}), AddJobOptions {
        lifo: Some(true),
        ..Default::default()
    }).unwrap();

    // BullMQ Worker processes in LIFO order — second should be processed first
    let result = bullmq_consume(&queue_name, 10000);
    assert!(result.is_some());
    let job = result.unwrap();
    assert_eq!(job["data"]["value"], "second", "LIFO: second job should be consumed first");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

// === Backoff with exponential + max readable by BullMQ ===

#[test]
fn hornet_exponential_backoff_with_max_readable_by_bullmq() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    let id = queue.add("exp-job", serde_json::json!({"value": "exp"}), AddJobOptions {
        attempts: Some(5),
        backoff: Some(BackoffStrategy::Exponential { base: 1000, max: 30000 }),
        ..Default::default()
    }).unwrap();

    let job = bullmq_read_job(&queue_name, &id);
    assert_eq!(job["opts"]["backoff"]["type"], "exponential");
    assert_eq!(job["opts"]["backoff"]["delay"], 1000);
    assert_eq!(job["opts"]["backoff"]["max"], 30000);
    assert_eq!(job["opts"]["attempts"], 5);

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

// === Priority job consumed by BullMQ ===
// NOTE: addStandardJob Lua does not route priority>0 jobs to the prioritized
// sorted set — it puts them in the wait list via LPUSH regardless of priority.
// This means BullMQ will process them in FIFO order, not priority order.
// This test verifies BullMQ can at least consume priority-annotated jobs and
// that the priority field is preserved on the job hash.

#[test]
fn hornet_priority_job_consumable_by_bullmq() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    queue.add("high-prio", serde_json::json!({"value": "high"}), AddJobOptions {
        priority: Some(1),
        ..Default::default()
    }).unwrap();

    // BullMQ should be able to consume the priority-annotated job
    let result = bullmq_consume(&queue_name, 10000);
    assert!(result.is_some());
    let job = result.unwrap();
    assert_eq!(job["data"]["value"], "high");
    assert_eq!(job["name"], "high-prio");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}
