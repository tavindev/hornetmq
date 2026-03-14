use anyhow::Result;
use hornet::{core::job::Job, worker::Worker};
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
    format!("test-{}", Uuid::new_v4())
}

fn prefix_for(queue_name: &str) -> String {
    format!("bull:{}:", queue_name)
}

/// Add a job to Redis manually, simulating a producer.
fn add_job_to_redis(
    conn: &mut redis::Connection,
    queue_name: &str,
    job_id: &str,
    data: &TestData,
    opts_json: &str,
) {
    let prefix = prefix_for(queue_name);
    let job_key = format!("{}{}", prefix, job_id);
    let wait_key = format!("{}wait", prefix);
    let marker_key = format!("{}marker", prefix);
    let meta_key = format!("{}meta", prefix);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let data_json = serde_json::to_string(data).unwrap();

    // HSET the job hash
    let _: () = redis::cmd("HMSET")
        .arg(&job_key)
        .arg("name")
        .arg("test-job")
        .arg("data")
        .arg(&data_json)
        .arg("opts")
        .arg(opts_json)
        .arg("timestamp")
        .arg(now.to_string())
        .arg("delay")
        .arg("0")
        .arg("priority")
        .arg("0")
        .arg("processedOn")
        .arg("0")
        .arg("ats")
        .arg("0")
        .query(conn)
        .unwrap();

    // Set meta key (needed by lua scripts)
    let _: () = redis::cmd("HSET")
        .arg(&meta_key)
        .arg("opts.maxLenEvents")
        .arg("10000")
        .query(conn)
        .unwrap();

    // LPUSH to wait list
    let _: u32 = conn.lpush(&wait_key, job_id).unwrap();

    // Set marker so worker wakes up
    let _: u32 = conn.zadd(&marker_key, "1", 0.0).unwrap();
}

/// Clean up all keys for a queue.
fn cleanup_queue(conn: &mut redis::Connection, queue_name: &str) {
    let prefix = prefix_for(queue_name);
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}*", prefix))
        .query(conn)
        .unwrap_or_default();

    for key in keys {
        let _: () = conn.del(&key).unwrap_or(());
    }
}

fn success_processor(job: &Job<TestData>) -> Result<String> {
    Ok(format!("processed:{}", job.data.value))
}

fn always_fail_processor(_job: &Job<TestData>) -> Result<String> {
    Err(anyhow::anyhow!("intentional failure"))
}

#[tokio::test]
async fn test_worker_processes_job() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    let job_id = "1";
    let data = TestData {
        value: "hello".into(),
    };
    add_job_to_redis(
        &mut conn,
        &queue_name,
        job_id,
        &data,
        r#"{"attempts": 1}"#,
    );

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        success_processor,
    );

    let shutdown = worker.shutdown_flag();

    // Run worker in background, shut down after a short delay
    let handle = tokio::spawn(async move {
        worker.run().await
    });

    // Give worker time to process
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.store(true, Ordering::SeqCst);

    let result = handle.await.unwrap();
    assert!(result.is_ok());

    // Verify job is in completed set
    let prefix = prefix_for(&queue_name);
    let completed_key = format!("{}completed", prefix);
    let members: Vec<String> = conn.zrange(&completed_key, 0, -1).unwrap();
    assert!(
        members.contains(&job_id.to_string()),
        "Job should be in completed set, got: {:?}",
        members
    );

    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn test_worker_retries_with_backoff() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    let job_id = "2";
    let data = TestData {
        value: "retry-me".into(),
    };
    // attempts=3, with Fixed(2000ms) backoff
    add_job_to_redis(
        &mut conn,
        &queue_name,
        job_id,
        &data,
        r#"{"attempts": 3, "backoff": {"Fixed": 2000}}"#,
    );

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        always_fail_processor,
    );

    let shutdown = worker.shutdown_flag();

    let handle = tokio::spawn(async move {
        worker.run().await
    });

    // Give worker time to process the first attempt and move to delayed
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.store(true, Ordering::SeqCst);

    let result = handle.await.unwrap();
    assert!(result.is_ok());

    let prefix = prefix_for(&queue_name);
    let delayed_key = format!("{}delayed", prefix);

    // Job should be in the delayed set (waiting for backoff)
    let delayed_members: Vec<String> = conn.zrange(&delayed_key, 0, -1).unwrap();

    // It could also have been re-processed and moved to failed if enough time passed.
    // Check either delayed or failed.
    let failed_key = format!("{}failed", prefix);
    let failed_members: Vec<String> = conn.zrange(&failed_key, 0, -1).unwrap();

    assert!(
        delayed_members.contains(&job_id.to_string())
            || failed_members.contains(&job_id.to_string()),
        "Job should be in delayed or failed set. delayed={:?}, failed={:?}",
        delayed_members,
        failed_members
    );

    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn test_worker_graceful_shutdown() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        success_processor,
    );

    let shutdown = worker.shutdown_flag();

    let handle = tokio::spawn(async move {
        worker.run().await
    });

    // Trigger shutdown immediately
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.store(true, Ordering::SeqCst);

    // Worker should return (not hang)
    let result = tokio::time::timeout(Duration::from_secs(10), handle).await;
    assert!(result.is_ok(), "Worker should stop within timeout");
    assert!(result.unwrap().unwrap().is_ok());

    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();
    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn test_worker_fails_after_max_attempts() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    let job_id = "4";
    let data = TestData {
        value: "fail-me".into(),
    };
    // attempts=2, no backoff (immediate retry)
    add_job_to_redis(
        &mut conn,
        &queue_name,
        job_id,
        &data,
        r#"{"attempts": 2}"#,
    );

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        always_fail_processor,
    );

    let shutdown = worker.shutdown_flag();

    let handle = tokio::spawn(async move {
        worker.run().await
    });

    // Give worker time to process both attempts
    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.store(true, Ordering::SeqCst);

    let result = handle.await.unwrap();
    assert!(result.is_ok());

    // Verify job ends up in failed set
    let prefix = prefix_for(&queue_name);
    let failed_key = format!("{}failed", prefix);
    let members: Vec<String> = conn.zrange(&failed_key, 0, -1).unwrap();
    assert!(
        members.contains(&job_id.to_string()),
        "Job should be in failed set, got: {:?}",
        members
    );

    cleanup_queue(&mut conn, &queue_name);
}

#[tokio::test]
async fn test_stall_detection() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis::Client::open(redis_url)
        .unwrap()
        .get_connection()
        .unwrap();

    let prefix = prefix_for(&queue_name);
    let job_id = "5";
    let data = TestData {
        value: "stall-me".into(),
    };

    // Add job hash
    add_job_to_redis(
        &mut conn,
        &queue_name,
        job_id,
        &data,
        r#"{"attempts": 3}"#,
    );

    // Manually move job from wait to active (simulating it was picked up but stalled)
    let wait_key = format!("{}wait", prefix);
    let active_key = format!("{}active", prefix);

    // Remove from wait, add to active
    let _: u32 = conn.lrem(&wait_key, 0, job_id).unwrap();
    let _: u32 = conn.lpush(&active_key, job_id).unwrap();

    // Do NOT set a lock key, so stall detection will see it as stalled

    // Use a short lock_duration so the stall checker runs quickly (interval = 1s)
    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url.to_string(),
        1,
        success_processor,
    )
    .with_lock_duration(2_000);

    let shutdown = worker.shutdown_flag();

    let handle = tokio::spawn(async move {
        worker.run().await
    });

    // Stall checker runs at lock_duration/2 = 1s with our short lock duration.
    // Wait long enough for at least one stall check cycle + processing.
    tokio::time::sleep(Duration::from_secs(5)).await;
    shutdown.store(true, Ordering::SeqCst);

    let _ = handle.await;

    // The job should have been re-queued or completed by now.
    // Check that it's no longer stuck in active without a lock.
    let active_members: Vec<String> = conn.lrange(&active_key, 0, -1).unwrap();
    let wait_members: Vec<String> = conn.lrange(&wait_key, 0, -1).unwrap();
    let completed_key = format!("{}completed", prefix);
    let completed_members: Vec<String> = conn.zrange(&completed_key, 0, -1).unwrap();

    // It should have been detected as stalled and re-queued, then potentially processed
    let is_resolved = !active_members.contains(&job_id.to_string())
        || wait_members.contains(&job_id.to_string())
        || completed_members.contains(&job_id.to_string());

    assert!(
        is_resolved,
        "Stalled job should be re-queued or completed. active={:?}, wait={:?}, completed={:?}",
        active_members, wait_members, completed_members
    );

    cleanup_queue(&mut conn, &queue_name);
}
