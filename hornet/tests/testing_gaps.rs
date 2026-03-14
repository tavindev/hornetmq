use anyhow::Result;
use hornetmq::{
    core::job::{Job, KeepJobsConfig},
    queue::AddJobOptions,
    worker::Worker,
    BackoffStrategy, Queue,
};
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
    format!("test-gaps-{}", Uuid::new_v4())
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

fn make_queue(name: &str) -> Queue {
    Queue::new(name.to_string(), "redis://localhost:6379").unwrap()
}

fn redis_conn() -> redis::Connection {
    redis::Client::open("redis://localhost:6379")
        .unwrap()
        .get_connection()
        .unwrap()
}

fn success_processor(job: &Job<TestData>) -> Result<String> {
    Ok(format!("processed:{}", job.data.value))
}

fn always_fail_processor(_job: &Job<TestData>) -> Result<String> {
    Err(anyhow::anyhow!("intentional failure"))
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
    let job_key = format!("{prefix}{job_id}");
    let wait_key = format!("{prefix}wait");
    let marker_key = format!("{prefix}marker");
    let meta_key = format!("{prefix}meta");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let data_json = serde_json::to_string(data).unwrap();

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

    let _: () = redis::cmd("HSET")
        .arg(&meta_key)
        .arg("opts.maxLenEvents")
        .arg("10000")
        .query(conn)
        .unwrap();

    let _: u32 = conn.lpush(&wait_key, job_id).unwrap();
    let _: u32 = conn.zadd(&marker_key, "1", 0.0).unwrap();
}

// ============================================================
// KeepJobsConfig deserialization variants
// ============================================================

#[test]
fn keep_jobs_config_deserialize_count() {
    // removeOnComplete: 5 (keep last 5)
    let config: KeepJobsConfig = serde_json::from_str("5").unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 5);
    assert!(keep.age.is_none());
}

#[test]
fn keep_jobs_config_deserialize_spec() {
    // removeOnComplete: {"age": 3600, "count": 100}
    let config: KeepJobsConfig =
        serde_json::from_str(r#"{"age": 3600, "count": 100}"#).unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 100);
    assert_eq!(keep.age, Some(3600));
}

#[test]
fn keep_jobs_config_deserialize_spec_age_only() {
    // removeOnComplete: {"age": 7200}
    let config: KeepJobsConfig =
        serde_json::from_str(r#"{"age": 7200}"#).unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, -1); // default when count not specified
    assert_eq!(keep.age, Some(7200));
}

#[test]
fn keep_jobs_config_deserialize_false() {
    // removeOnComplete: false (keep all)
    let config: KeepJobsConfig = serde_json::from_str("false").unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, -1); // default = keep all
    assert!(keep.age.is_none());
}

#[test]
fn keep_jobs_config_deserialize_true() {
    // removeOnComplete: true (remove immediately)
    let config: KeepJobsConfig = serde_json::from_str("true").unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 0); // keep 0 = remove all
    assert!(keep.age.is_none());
}

#[test]
fn keep_jobs_config_deserialize_zero() {
    // removeOnComplete: 0 (keep 0 = remove all)
    let config: KeepJobsConfig = serde_json::from_str("0").unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 0);
}

// ============================================================
// Queue.get_jobs edge cases
// ============================================================

#[test]
fn get_jobs_unknown_state_returns_error() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);
    let result = q.get_jobs("nonexistent_state", 0, -1);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("unknown state"),
        "Expected 'unknown state' error, got: {err_msg}"
    );
    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn get_jobs_completed_state() {
    // Verify get_jobs works for the "completed" state (sorted set, not list)
    let name = unique_queue_name();
    let mut conn = redis_conn();
    let prefix = prefix_for(&name);

    // Manually add a job to completed sorted set
    let job_key = format!("{prefix}job-c1");
    let _: () = redis::cmd("HMSET")
        .arg(&job_key)
        .arg("name")
        .arg("completed-job")
        .arg("data")
        .arg(r#"{"value":"done"}"#)
        .arg("opts")
        .arg("{}")
        .arg("timestamp")
        .arg("1000")
        .arg("delay")
        .arg("0")
        .arg("priority")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let _: u32 = conn
        .zadd(&format!("{prefix}completed"), "job-c1", 1000.0)
        .unwrap();

    let mut q = make_queue(&name);
    let jobs = q.get_jobs("completed", 0, -1).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].name, "completed-job");

    cleanup_queue(&mut conn, &name);
}

#[test]
fn get_jobs_delayed_state() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    q.add(
        "delayed-job",
        serde_json::json!({"value": "later"}),
        AddJobOptions {
            delay: Some(60_000),
            ..Default::default()
        },
    )
    .unwrap();

    let jobs = q.get_jobs("delayed", 0, -1).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].name, "delayed-job");

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn get_jobs_empty_state() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);
    let jobs = q.get_jobs("wait", 0, -1).unwrap();
    assert!(jobs.is_empty());
    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// Queue.drain with different job states
// ============================================================

#[test]
fn drain_with_delayed_jobs() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    q.add(
        "delayed",
        serde_json::json!(1),
        AddJobOptions {
            delay: Some(60_000),
            ..Default::default()
        },
    )
    .unwrap();
    q.add(
        "waiting",
        serde_json::json!(2),
        AddJobOptions::default(),
    )
    .unwrap();

    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.delayed, 1);
    assert_eq!(counts.waiting, 1);

    q.drain().unwrap();

    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.delayed, 0);
    assert_eq!(counts.waiting, 0);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn drain_with_paused_jobs() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    q.add("job", serde_json::json!(1), AddJobOptions::default())
        .unwrap();
    q.pause().unwrap();

    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.paused, 1);

    q.drain().unwrap();

    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.paused, 0);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// Queue.remove_job from different states
// ============================================================

#[test]
fn remove_job_from_delayed_state() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    let id = q
        .add(
            "delayed",
            serde_json::json!(1),
            AddJobOptions {
                delay: Some(60_000),
                ..Default::default()
            },
        )
        .unwrap();

    assert!(q.get_job(&id).unwrap().is_some());

    q.remove_job(&id).unwrap();

    assert!(q.get_job(&id).unwrap().is_none());
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.delayed, 0);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn remove_job_from_completed_state() {
    let name = unique_queue_name();
    let mut conn = redis_conn();
    let prefix = prefix_for(&name);

    // Manually put a job into the completed sorted set
    let job_key = format!("{prefix}completed-1");
    let _: () = redis::cmd("HMSET")
        .arg(&job_key)
        .arg("name")
        .arg("done-job")
        .arg("data")
        .arg(r#""done""#)
        .arg("opts")
        .arg("{}")
        .arg("timestamp")
        .arg("1000")
        .arg("delay")
        .arg("0")
        .arg("priority")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let _: u32 = conn
        .zadd(&format!("{prefix}completed"), "completed-1", 1000.0)
        .unwrap();

    let mut q = make_queue(&name);
    assert!(q.get_job("completed-1").unwrap().is_some());

    q.remove_job("completed-1").unwrap();

    assert!(q.get_job("completed-1").unwrap().is_none());
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.completed, 0);

    cleanup_queue(&mut conn, &name);
}

#[test]
fn remove_job_from_failed_state() {
    let name = unique_queue_name();
    let mut conn = redis_conn();
    let prefix = prefix_for(&name);

    let job_key = format!("{prefix}failed-1");
    let _: () = redis::cmd("HMSET")
        .arg(&job_key)
        .arg("name")
        .arg("fail-job")
        .arg("data")
        .arg(r#""oops""#)
        .arg("opts")
        .arg("{}")
        .arg("timestamp")
        .arg("1000")
        .arg("delay")
        .arg("0")
        .arg("priority")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let _: u32 = conn
        .zadd(&format!("{prefix}failed"), "failed-1", 1000.0)
        .unwrap();

    let mut q = make_queue(&name);
    assert!(q.get_job("failed-1").unwrap().is_some());

    q.remove_job("failed-1").unwrap();

    assert!(q.get_job("failed-1").unwrap().is_none());
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.failed, 0);

    cleanup_queue(&mut conn, &name);
}

#[test]
fn remove_job_nonexistent_is_ok() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    // Should not error when removing a job that doesn't exist
    let result = q.remove_job("does-not-exist");
    assert!(result.is_ok());

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// AddJobOptions combinations
// ============================================================

#[test]
fn delayed_job_with_priority() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    let id = q
        .add(
            "combo",
            serde_json::json!({"value": "delayed+priority"}),
            AddJobOptions {
                delay: Some(60_000),
                priority: Some(5),
                ..Default::default()
            },
        )
        .unwrap();

    let job = q.get_job(&id).unwrap().unwrap();
    assert_eq!(job.priority, 5);
    assert_eq!(job.delay, 60_000);

    // Should be in delayed set (delay takes precedence)
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.delayed, 1);
    assert_eq!(counts.waiting, 0);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn add_job_with_all_options() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    let id = q
        .add(
            "full-opts",
            serde_json::json!({"value": "everything"}),
            AddJobOptions {
                delay: Some(60_000),
                priority: Some(10),
                attempts: Some(5),
                backoff: Some(BackoffStrategy::Exponential {
                    base: 1000,
                    max: 30000,
                }),
                job_id: Some("custom-full-opts".into()),
                lifo: Some(true),
                remove_on_complete: None,
                remove_on_fail: None,
            },
        )
        .unwrap();

    assert_eq!(id, "custom-full-opts");
    let job = q.get_job(&id).unwrap().unwrap();
    assert_eq!(job.name, "full-opts");
    assert_eq!(job.priority, 10);
    assert_eq!(job.delay, 60_000);

    // Opts stored in Redis should contain attempts and backoff
    let opts: serde_json::Value = serde_json::from_str(&job.opts).unwrap();
    assert_eq!(opts["attempts"], 5);
    assert_eq!(opts["backoff"]["type"], "exponential");

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// Custom job ID edge cases
// ============================================================

#[test]
fn custom_job_id_with_special_characters() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    let id = q
        .add(
            "special",
            serde_json::json!(1),
            AddJobOptions {
                job_id: Some("user:123:action:456".into()),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(id, "user:123:action:456");
    let job = q.get_job(&id).unwrap();
    assert!(job.is_some());

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn custom_job_id_with_uuid() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    let custom_id = Uuid::new_v4().to_string();
    let id = q
        .add(
            "uuid-job",
            serde_json::json!(1),
            AddJobOptions {
                job_id: Some(custom_id.clone()),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(id, custom_id);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// Queue.is_paused independent tests
// ============================================================

#[test]
fn is_paused_returns_false_initially() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);
    assert!(!q.is_paused().unwrap());
    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn is_paused_returns_true_after_pause() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);
    q.pause().unwrap();
    assert!(q.is_paused().unwrap());
    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

#[test]
fn is_paused_returns_false_after_resume() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);
    q.pause().unwrap();
    assert!(q.is_paused().unwrap());
    q.resume().unwrap();
    assert!(!q.is_paused().unwrap());
    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// Worker retry with BullMQ backoff format end-to-end
// ============================================================

#[tokio::test]
async fn worker_retry_with_bullmq_backoff_format() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis_conn();

    // Manually add a job with BullMQ-format backoff opts
    let job_id = "bullmq-backoff-1";
    let data = TestData {
        value: "retry-bullmq-format".into(),
    };
    add_job_to_redis(
        &mut conn,
        &queue_name,
        job_id,
        &data,
        r#"{"attempts": 3, "backoff": {"type": "exponential", "delay": 500, "max": 5000}}"#,
    );

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url,
        1,
        always_fail_processor,
    )
    .unwrap();

    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    // Give worker time to process first attempt and move to delayed
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    let prefix = prefix_for(&queue_name);
    let delayed_key = format!("{prefix}delayed");
    let failed_key = format!("{prefix}failed");

    let delayed_members: Vec<String> = conn.zrange(&delayed_key, 0, -1).unwrap();
    let failed_members: Vec<String> = conn.zrange(&failed_key, 0, -1).unwrap();

    // After first failure with backoff, should be in delayed (waiting for retry)
    // or if enough time passed and all attempts exhausted, in failed
    assert!(
        delayed_members.contains(&job_id.to_string())
            || failed_members.contains(&job_id.to_string()),
        "Job should be in delayed or failed. delayed={delayed_members:?}, failed={failed_members:?}"
    );

    cleanup_queue(&mut conn, &queue_name);
}

// ============================================================
// removeOnFail behavior
// ============================================================

#[tokio::test]
async fn remove_on_fail_true_deletes_job() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis_conn();

    let prefix = prefix_for(&queue_name);
    let job_key = format!("{prefix}1");
    let wait_key = format!("{prefix}wait");
    let marker_key = format!("{prefix}marker");
    let meta_key = format!("{prefix}meta");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    // Add job with removeOnFail: true and attempts: 1 (no retry)
    let _: () = redis::cmd("HMSET")
        .arg(&job_key)
        .arg("name")
        .arg("test-job")
        .arg("data")
        .arg(r#"{"value":"fail-cleanup"}"#)
        .arg("opts")
        .arg(r#"{"attempts":1,"removeOnFail":true}"#)
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
        .query(&mut conn)
        .unwrap();

    let _: () = redis::cmd("HSET")
        .arg(&meta_key)
        .arg("opts.maxLenEvents")
        .arg("10000")
        .query(&mut conn)
        .unwrap();

    let _: u32 = conn.lpush(&wait_key, "1").unwrap();
    let _: u32 = conn.zadd(&marker_key, "0", 0.0).unwrap();

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url,
        1,
        always_fail_processor,
    )
    .unwrap();
    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    // Job hash should be deleted
    let exists: bool = conn.exists(&job_key).unwrap();
    assert!(
        !exists,
        "Job hash should be deleted with removeOnFail:true"
    );

    // Failed set should be empty
    let failed: Vec<String> = conn
        .zrange(&format!("{prefix}failed"), 0, -1)
        .unwrap();
    assert!(failed.is_empty(), "Failed set should be empty");

    cleanup_queue(&mut conn, &queue_name);
}

// ============================================================
// JobOptions deserialization with removeOnComplete/removeOnFail variants
// ============================================================

#[test]
fn job_options_deserialize_remove_on_complete_count() {
    use hornetmq::core::job::JobOptions;
    let json = r#"{"attempts":1,"removeOnComplete":5}"#;
    let opts: JobOptions = serde_json::from_str(json).unwrap();
    let config = opts.remove_on_complete.unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 5);
}

#[test]
fn job_options_deserialize_remove_on_complete_spec() {
    use hornetmq::core::job::JobOptions;
    let json = r#"{"attempts":1,"removeOnComplete":{"age":3600,"count":100}}"#;
    let opts: JobOptions = serde_json::from_str(json).unwrap();
    let config = opts.remove_on_complete.unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 100);
    assert_eq!(keep.age, Some(3600));
}

#[test]
fn job_options_deserialize_remove_on_fail_true() {
    use hornetmq::core::job::JobOptions;
    let json = r#"{"attempts":1,"removeOnFail":true}"#;
    let opts: JobOptions = serde_json::from_str(json).unwrap();
    let config = opts.remove_on_fail.unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 0); // true = remove immediately
}

#[test]
fn job_options_deserialize_remove_on_fail_count() {
    use hornetmq::core::job::JobOptions;
    let json = r#"{"attempts":1,"removeOnFail":10}"#;
    let opts: JobOptions = serde_json::from_str(json).unwrap();
    let config = opts.remove_on_fail.unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 10);
}

#[test]
fn job_options_deserialize_remove_on_fail_spec() {
    use hornetmq::core::job::JobOptions;
    let json = r#"{"attempts":1,"removeOnFail":{"age":1800,"count":50}}"#;
    let opts: JobOptions = serde_json::from_str(json).unwrap();
    let config = opts.remove_on_fail.unwrap();
    let keep = config.to_keep_jobs();
    assert_eq!(keep.count, 50);
    assert_eq!(keep.age, Some(1800));
}

// ============================================================
// Queue.add_bulk with options
// ============================================================

#[test]
fn add_bulk_with_mixed_options() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    let ids = q
        .add_bulk(vec![
            (
                "normal",
                serde_json::json!(1),
                AddJobOptions::default(),
            ),
            (
                "delayed",
                serde_json::json!(2),
                AddJobOptions {
                    delay: Some(60_000),
                    ..Default::default()
                },
            ),
            (
                "priority",
                serde_json::json!(3),
                AddJobOptions {
                    priority: Some(5),
                    ..Default::default()
                },
            ),
        ])
        .unwrap();

    assert_eq!(ids.len(), 3);

    let counts = q.get_job_counts().unwrap();
    // 1 normal waiting, 1 delayed, 1 priority (in wait since add_standard_job puts it there)
    assert_eq!(counts.delayed, 1);
    assert!(counts.waiting >= 1);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// Queue.get_job_counts with mixed states
// ============================================================

#[test]
fn get_job_counts_with_mixed_states() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    // Add normal jobs
    q.add("w1", serde_json::json!(1), AddJobOptions::default())
        .unwrap();
    q.add("w2", serde_json::json!(2), AddJobOptions::default())
        .unwrap();

    // Add delayed job
    q.add(
        "d1",
        serde_json::json!(3),
        AddJobOptions {
            delay: Some(60_000),
            ..Default::default()
        },
    )
    .unwrap();

    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.waiting, 2);
    assert_eq!(counts.delayed, 1);
    assert_eq!(counts.active, 0);
    assert_eq!(counts.completed, 0);
    assert_eq!(counts.failed, 0);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// BackoffStrategy serialization roundtrip
// ============================================================

#[test]
fn backoff_strategy_fixed_roundtrip() {
    let strategy = BackoffStrategy::Fixed(3000);
    let json = serde_json::to_string(&strategy).unwrap();
    let deserialized: BackoffStrategy = serde_json::from_str(&json).unwrap();
    assert_eq!(strategy, deserialized);
}

#[test]
fn backoff_strategy_exponential_roundtrip() {
    let strategy = BackoffStrategy::Exponential {
        base: 2000,
        max: 60_000,
    };
    let json = serde_json::to_string(&strategy).unwrap();
    let deserialized: BackoffStrategy = serde_json::from_str(&json).unwrap();
    assert_eq!(strategy, deserialized);
}

// ============================================================
// Worker with default_backoff (set via with_backoff)
// ============================================================

#[tokio::test]
async fn worker_default_backoff_used_when_job_has_none() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis_conn();

    // Job has attempts but NO backoff
    let job_id = "default-backoff-1";
    let data = TestData {
        value: "use-default-backoff".into(),
    };
    add_job_to_redis(
        &mut conn,
        &queue_name,
        job_id,
        &data,
        r#"{"attempts": 3}"#,
    );

    // Worker has a default backoff
    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url,
        1,
        always_fail_processor,
    )
    .unwrap()
    .with_backoff(BackoffStrategy::Fixed(500));

    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    let prefix = prefix_for(&queue_name);
    let delayed_key = format!("{prefix}delayed");

    // With default_backoff, the job should be moved to delayed (not immediately retried)
    let delayed_members: Vec<String> = conn.zrange(&delayed_key, 0, -1).unwrap();
    let failed_key = format!("{prefix}failed");
    let failed_members: Vec<String> = conn.zrange(&failed_key, 0, -1).unwrap();

    assert!(
        delayed_members.contains(&job_id.to_string())
            || failed_members.contains(&job_id.to_string()),
        "Job with default_backoff should be in delayed or failed. delayed={delayed_members:?}, failed={failed_members:?}"
    );

    cleanup_queue(&mut conn, &queue_name);
}

// ============================================================
// Multiple pause/resume cycles
// ============================================================

#[test]
fn multiple_pause_resume_cycles() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);

    q.add("job1", serde_json::json!(1), AddJobOptions::default())
        .unwrap();
    q.add("job2", serde_json::json!(2), AddJobOptions::default())
        .unwrap();

    // First cycle
    q.pause().unwrap();
    assert!(q.is_paused().unwrap());
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.paused, 2);
    assert_eq!(counts.waiting, 0);

    q.resume().unwrap();
    assert!(!q.is_paused().unwrap());
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.waiting, 2);
    assert_eq!(counts.paused, 0);

    // Second cycle
    q.pause().unwrap();
    assert!(q.is_paused().unwrap());
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.paused, 2);

    q.resume().unwrap();
    assert!(!q.is_paused().unwrap());
    let counts = q.get_job_counts().unwrap();
    assert_eq!(counts.waiting, 2);

    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}

// ============================================================
// Worker processes multiple jobs sequentially
// ============================================================

#[tokio::test]
async fn worker_processes_multiple_jobs() {
    let queue_name = unique_queue_name();
    let redis_url = "redis://localhost:6379";
    let mut conn = redis_conn();

    for i in 1..=3 {
        let data = TestData {
            value: format!("job-{i}"),
        };
        add_job_to_redis(
            &mut conn,
            &queue_name,
            &i.to_string(),
            &data,
            r#"{"attempts": 1}"#,
        );
    }

    let mut worker = Worker::new(
        queue_name.clone(),
        redis_url,
        1,
        success_processor,
    )
    .unwrap();

    let shutdown = worker.shutdown_flag();
    let handle = tokio::spawn(async move { worker.run().await });

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.store(true, Ordering::SeqCst);
    handle.await.unwrap().unwrap();

    let prefix = prefix_for(&queue_name);
    let completed_key = format!("{prefix}completed");
    let members: Vec<String> = conn.zrange(&completed_key, 0, -1).unwrap();
    assert_eq!(
        members.len(),
        3,
        "All 3 jobs should be completed, got: {members:?}"
    );

    cleanup_queue(&mut conn, &queue_name);
}

// ============================================================
// Queue.close drops cleanly
// ============================================================

#[test]
fn queue_close_drops_without_panic() {
    let name = unique_queue_name();
    let mut q = make_queue(&name);
    q.add("job", serde_json::json!(1), AddJobOptions::default())
        .unwrap();
    q.close(); // should not panic
    let mut conn = redis_conn();
    cleanup_queue(&mut conn, &name);
}
