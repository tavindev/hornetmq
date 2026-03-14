use hornetmq::queue::{AddJobOptions, Queue};
use redis::Commands;
use uuid::Uuid;

fn unique_queue_name() -> String {
    format!("test-queue-{}", Uuid::new_v4())
}

fn cleanup_queue(queue_name: &str) {
    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut con = client.get_connection().unwrap();
    let prefix = format!("bull:{queue_name}:");

    let suffixes = [
        "wait",
        "paused",
        "meta",
        "id",
        "completed",
        "events",
        "marker",
        "active",
        "prioritized",
        "stalled",
        "limiter",
        "delayed",
        "pc",
        "metrics",
    ];

    let keys: Vec<String> = suffixes.iter().map(|s| format!("{prefix}{s}")).collect();

    // Also delete any job keys (bull:queue_name:1, bull:queue_name:2, etc.)
    let job_keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}[0-9]*"))
        .query(&mut con)
        .unwrap_or_default();

    let mut all_keys = keys;
    all_keys.extend(job_keys);

    if !all_keys.is_empty() {
        let _: () = redis::cmd("DEL")
            .arg(&all_keys)
            .query(&mut con)
            .unwrap_or_default();
    }
}

#[test]
fn test_add_job() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    let job_id = queue
        .add(
            "my-job",
            serde_json::json!({"foo": "bar"}),
            AddJobOptions::default(),
        )
        .expect("failed to add job");

    // Verify job exists in Redis
    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut con = client.get_connection().unwrap();
    let job_key = format!("bull:{queue_name}:{job_id}");

    let name: String = con.hget(&job_key, "name").expect("missing name field");
    assert_eq!(name, "my-job");

    let data: String = con.hget(&job_key, "data").expect("missing data field");
    let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert_eq!(parsed, serde_json::json!({"foo": "bar"}));

    let opts_str: String = con.hget(&job_key, "opts").expect("missing opts field");
    let opts: serde_json::Value = serde_json::from_str(&opts_str).unwrap();
    // Default opts should exist as a JSON object
    assert!(opts.is_object());

    // Job should be in the wait list
    let wait_key = format!("bull:{queue_name}:wait");
    let wait_members: Vec<String> = con.lrange(&wait_key, 0, -1).unwrap();
    assert!(wait_members.contains(&job_id));

    cleanup_queue(&queue_name);
}

#[test]
fn test_add_job_with_delay() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    let job_id = queue
        .add(
            "delayed-job",
            serde_json::json!({"task": "later"}),
            AddJobOptions {
                delay: Some(60_000),
                ..Default::default()
            },
        )
        .expect("failed to add delayed job");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut con = client.get_connection().unwrap();

    // Job data should exist
    let job_key = format!("bull:{queue_name}:{job_id}");
    let name: String = con.hget(&job_key, "name").expect("missing name field");
    assert_eq!(name, "delayed-job");

    // Delay should be stored on the job hash
    let delay: String = con.hget(&job_key, "delay").expect("missing delay field");
    assert_eq!(delay, "60000");

    cleanup_queue(&queue_name);
}

#[test]
fn test_add_job_with_priority() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    let job_id = queue
        .add(
            "priority-job",
            serde_json::json!({"urgent": true}),
            AddJobOptions {
                priority: Some(10),
                ..Default::default()
            },
        )
        .expect("failed to add priority job");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut con = client.get_connection().unwrap();

    // Job data should exist
    let job_key = format!("bull:{queue_name}:{job_id}");
    let name: String = con.hget(&job_key, "name").expect("missing name field");
    assert_eq!(name, "priority-job");

    // Priority should be stored on the job hash
    let priority: String = con
        .hget(&job_key, "priority")
        .expect("missing priority field");
    assert_eq!(priority, "10");

    cleanup_queue(&queue_name);
}

#[test]
fn test_add_job_default_options() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    let job_id = queue
        .add(
            "default-opts-job",
            "simple string data",
            AddJobOptions::default(),
        )
        .expect("failed to add job with default opts");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut con = client.get_connection().unwrap();

    let job_key = format!("bull:{queue_name}:{job_id}");

    // Verify defaults: delay=0, priority=0
    let delay: String = con.hget(&job_key, "delay").expect("missing delay field");
    assert_eq!(delay, "0");

    let priority: String = con
        .hget(&job_key, "priority")
        .expect("missing priority field");
    assert_eq!(priority, "0");

    // Verify timestamp exists and is reasonable
    let timestamp: String = con.hget(&job_key, "timestamp").expect("missing timestamp");
    let ts: u128 = timestamp.parse().expect("timestamp should be numeric");
    assert!(ts > 0);

    // Verify the job is in the wait list (not delayed or prioritized)
    let wait_key = format!("bull:{queue_name}:wait");
    let wait_members: Vec<String> = con.lrange(&wait_key, 0, -1).unwrap();
    assert!(wait_members.contains(&job_id));

    cleanup_queue(&queue_name);
}
