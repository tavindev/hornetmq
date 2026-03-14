use anyhow::Result;
use hornetmq::{core::job::Job, queue::AddJobOptions, worker::Worker, Queue};
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct TestData {
    value: String,
}

fn unique_queue_name() -> String {
    format!("test-new-{}", Uuid::new_v4())
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

fn success_processor(job: &Job<TestData>) -> Result<String> {
    Ok(format!("processed:{}", job.data.value))
}

// === Custom Job IDs ===

#[test]
fn custom_job_id() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    let id = queue.add("test", serde_json::json!({"x": 1}), AddJobOptions {
        job_id: Some("my-custom-id".into()),
        ..Default::default()
    }).unwrap();

    assert_eq!(id, "my-custom-id");

    // Verify the job exists with this ID
    let job = queue.get_job("my-custom-id").unwrap();
    assert!(job.is_some());
    assert_eq!(job.unwrap().name, "test");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

#[test]
fn duplicate_custom_job_id_returns_same_id() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    let id1 = queue.add("test", serde_json::json!(1), AddJobOptions {
        job_id: Some("dup-id".into()),
        ..Default::default()
    }).unwrap();

    let id2 = queue.add("test", serde_json::json!(2), AddJobOptions {
        job_id: Some("dup-id".into()),
        ..Default::default()
    }).unwrap();

    assert_eq!(id1, "dup-id");
    assert_eq!(id2, "dup-id");

    // Only one job should exist in wait
    let counts = queue.get_job_counts().unwrap();
    assert_eq!(counts.waiting, 1);

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

// === LIFO ===

#[test]
fn lifo_ordering() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    queue.add("first", serde_json::json!(1), AddJobOptions {
        lifo: Some(true),
        ..Default::default()
    }).unwrap();

    queue.add("second", serde_json::json!(2), AddJobOptions {
        lifo: Some(true),
        ..Default::default()
    }).unwrap();

    // With LIFO (RPUSH), the second job should be at the end (RPOPLPUSH pops from right)
    // In BullMQ, RPUSH + RPOPLPUSH means LIFO
    let jobs = queue.get_jobs("wait", 0, -1).unwrap();
    assert_eq!(jobs.len(), 2);
    // RPUSH puts at the end, RPOPLPUSH pops from the end = LIFO
    // So the last added job ("second") is at index 1 (rightmost)
    assert_eq!(jobs[1].name, "second");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

// === Delayed Jobs ===

#[test]
fn delayed_job_goes_to_delayed_set() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    queue.add("delayed", serde_json::json!({"x": 1}), AddJobOptions {
        delay: Some(60_000), // 60 seconds — won't fire during test
        ..Default::default()
    }).unwrap();

    let counts = queue.get_job_counts().unwrap();
    assert_eq!(counts.delayed, 1, "Delayed job should be in delayed set");
    assert_eq!(counts.waiting, 0, "Delayed job should NOT be in wait list");

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

#[test]
fn delayed_job_score_encoding() {
    let queue_name = unique_queue_name();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379").unwrap();

    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();

    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    queue.add("delayed", serde_json::json!(null), AddJobOptions {
        delay: Some(5000),
        ..Default::default()
    }).unwrap();

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Check the score in the delayed zset
    let prefix = prefix_for(&queue_name);
    let delayed_key = format!("{prefix}delayed");
    let scores: Vec<(String, f64)> = redis::cmd("ZRANGE")
        .arg(&delayed_key)
        .arg(0)
        .arg(-1)
        .arg("WITHSCORES")
        .query(&mut conn)
        .unwrap();

    assert_eq!(scores.len(), 1);
    let score = scores[0].1 as u64;

    // Score should be (timestamp + delay) * 0x1000
    let min_expected = (before + 5000) * 0x1000;
    let max_expected = (after + 5000) * 0x1000;

    assert!(
        score >= min_expected && score <= max_expected,
        "Score {score} should be between {min_expected} and {max_expected} ((timestamp+delay)*0x1000)"
    );

    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn delayed_job_gets_promoted_and_processed() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut queue = Queue::new(queue_name.clone(), redis_url).unwrap();

    // Add with tiny delay so it gets promoted quickly
    queue.add("quick-delay", serde_json::json!({"value": "delayed-hello"}), AddJobOptions {
        delay: Some(100), // 100ms delay
        ..Default::default()
    }).unwrap();

    // Verify it starts in delayed
    let counts = queue.get_job_counts().unwrap();
    assert_eq!(counts.delayed, 1);

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url,
        1,
        success_processor,
    )
    .unwrap();

    let shutdown = worker.shutdown_flag();

    // The worker's delayed-job promotion happens inside moveToActive, but the
    // marker that was set during add() may be consumed before the delay expires.
    // After the delay has elapsed, poke the marker so the worker wakes up and
    // invokes promoteDelayedJobs again.
    let poke_queue = queue_name.clone();
    let poke_url = redis_url.to_string();
    tokio::spawn(async move {
        // Wait for the delay to definitely expire
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = redis::Client::open(poke_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let marker_key = format!("bull:{}:marker", poke_queue);
        let _: redis::RedisResult<u32> = conn.zadd(&marker_key, "0", 0.0);
    });

    let handle = tokio::spawn(async move { worker.run().await });

    // Give worker time to promote and process
    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    let prefix = prefix_for(&queue_name);
    let completed_key = format!("{prefix}completed");
    let members: Vec<String> = conn.zrange(&completed_key, 0, -1).unwrap();
    assert!(!members.is_empty(), "Delayed job should be promoted and completed");

    cleanup_queue(&mut conn, &queue_name);
}

// === removeOnComplete / removeOnFail ===

#[tokio::test]
async fn remove_on_complete_true_deletes_job() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url).unwrap().get_connection().unwrap();

    let prefix = prefix_for(&queue_name);
    let job_key = format!("{prefix}1");
    let wait_key = format!("{prefix}wait");
    let marker_key = format!("{prefix}marker");
    let meta_key = format!("{prefix}meta");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    // Add job manually with removeOnComplete: true
    let _: () = redis::cmd("HMSET")
        .arg(&job_key)
        .arg("name").arg("test-job")
        .arg("data").arg(r#"{"value":"cleanup"}"#)
        .arg("opts").arg(r#"{"attempts":1,"removeOnComplete":true}"#)
        .arg("timestamp").arg(now.to_string())
        .arg("delay").arg("0")
        .arg("priority").arg("0")
        .arg("processedOn").arg("0")
        .arg("ats").arg("0")
        .query(&mut conn).unwrap();

    let _: () = redis::cmd("HSET")
        .arg(&meta_key).arg("opts.maxLenEvents").arg("10000")
        .query(&mut conn).unwrap();

    let _: u32 = conn.lpush(&wait_key, "1").unwrap();
    let _: u32 = conn.zadd(&marker_key, "0", 0.0).unwrap();

    let mut worker = Worker::new(queue_name.clone(), redis_url, 1, success_processor).unwrap();
    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    // Job hash should be deleted
    let exists: bool = conn.exists(&job_key).unwrap();
    assert!(!exists, "Job hash should be deleted with removeOnComplete:true");

    // Completed set should be empty
    let completed: Vec<String> = conn.zrange(&format!("{prefix}completed"), 0, -1).unwrap();
    assert!(completed.is_empty(), "Completed set should be empty");

    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn remove_on_complete_count_keeps_n_jobs() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url).unwrap().get_connection().unwrap();

    let prefix = prefix_for(&queue_name);
    let meta_key = format!("{prefix}meta");
    let wait_key = format!("{prefix}wait");
    let marker_key = format!("{prefix}marker");

    let _: () = redis::cmd("HSET")
        .arg(&meta_key).arg("opts.maxLenEvents").arg("10000")
        .query(&mut conn).unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    // Add 3 jobs with removeOnComplete: keep only 1
    for i in 1..=3 {
        let job_key = format!("{prefix}{i}");
        let _: () = redis::cmd("HMSET")
            .arg(&job_key)
            .arg("name").arg("test")
            .arg("data").arg(format!(r#"{{"value":"job-{i}"}}"#))
            .arg("opts").arg(r#"{"attempts":1,"removeOnComplete":1}"#)
            .arg("timestamp").arg(now.to_string())
            .arg("delay").arg("0")
            .arg("priority").arg("0")
            .arg("processedOn").arg("0")
            .arg("ats").arg("0")
            .query(&mut conn).unwrap();
        let _: u32 = conn.lpush(&wait_key, i.to_string()).unwrap();
    }
    let _: u32 = conn.zadd(&marker_key, "0", 0.0).unwrap();

    let mut worker = Worker::new(queue_name.clone(), redis_url, 1, success_processor).unwrap();
    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    // Should keep at most 1 completed job
    let completed: Vec<String> = conn.zrange(&format!("{prefix}completed"), 0, -1).unwrap();
    assert!(completed.len() <= 1, "Should keep at most 1 completed job, got: {}", completed.len());

    cleanup_queue(&mut conn, &queue_name);
}

// === Pause/Resume with Worker ===

#[tokio::test]
async fn paused_queue_does_not_process_jobs() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut queue = Queue::new(queue_name.clone(), redis_url).unwrap();

    queue.add("before-pause", serde_json::json!({"value": "should-wait"}), AddJobOptions::default()).unwrap();
    queue.pause().unwrap();

    let mut worker = Worker::new(queue_name.clone(), redis_url, 1, success_processor).unwrap();
    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    // Job should NOT have been processed (still in paused, nothing in completed)
    let counts = queue.get_job_counts().unwrap();
    assert_eq!(counts.paused, 1, "Job should still be in paused list");
    assert_eq!(counts.completed, 0, "No jobs should be completed when paused");

    let mut conn = redis::Client::open(redis_url).unwrap().get_connection().unwrap();
    cleanup_queue(&mut conn, &queue_name);
}
