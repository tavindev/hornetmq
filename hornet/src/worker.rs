use crate::{
    core::{
        backoff::BackoffStrategy,
        events::JobEvent,
        job::Job,
        retry::{next_retry_delay, should_retry},
    },
    scripts::{
        move_to_active::{Limiter, MoveToActive, MoveToActiveArgs, MoveToActiveReturn},
        move_to_finished::{
            MoveToFinished, MoveToFinishedArgs, MoveToFinishedReturn, MoveToFinishedTarget,
        },
        retry_job::{RetryJob, RetryJobReturn},
    },
};
use anyhow::Result;
use lazy_static::lazy_static;
use redis::{Client, Commands};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

lazy_static! {
    static ref MOVE_TO_ACTIVE: MoveToActive = MoveToActive::new();
    static ref MOVE_TO_FINISHED: MoveToFinished = MoveToFinished::new();
    static ref RETRY_JOB: RetryJob = RetryJob::new();
}

const DEFAULT_LOCK_DURATION: u64 = 30_000;

struct WorkerToken {
    token: String,
    postfix: u64,
}

impl WorkerToken {
    fn new() -> Self {
        WorkerToken {
            token: Uuid::new_v4().to_string(),
            postfix: 0,
        }
    }

    fn next(&mut self) -> String {
        self.postfix += 1;
        format!("{}:{}", self.token, self.postfix)
    }
}

enum TaskEvent {
    Freed,
}

type ProcessFn<Data, Return> = fn(&Job<Data>) -> Result<Return>;

fn emit_event(client: &mut Client, prefix: &str, event: JobEvent, job_id: &str) {
    let events_key = format!("{prefix}events");
    let _: redis::RedisResult<String> = redis::cmd("XADD")
        .arg(&events_key)
        .arg("*")
        .arg("event")
        .arg(event.as_str())
        .arg("jobId")
        .arg(job_id)
        .query(client);
}

fn move_to_delayed(client: &mut Client, prefix: &str, job_id: &str, token: &str, delay_ms: u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let delayed_score = ((now + delay_ms) as u128) * 0x1000;
    let marker_score = now + delay_ms;

    let active_key = format!("{prefix}active");
    let delayed_key = format!("{prefix}delayed");
    let job_key = format!("{prefix}{job_id}");
    let lock_key = format!("{job_key}:lock");
    let marker_key = format!("{prefix}marker");

    // Remove lock
    let lock_val: redis::RedisResult<String> = client.get(&lock_key);
    if let Ok(val) = lock_val {
        if val == token {
            let _: redis::RedisResult<()> = client.del(&lock_key);
        }
    }

    // Remove from active list
    let _: redis::RedisResult<u32> = client.lrem(&active_key, 0, job_id);

    // Increment attempts made
    let _: redis::RedisResult<u32> = redis::cmd("HINCRBY")
        .arg(&job_key)
        .arg("atm")
        .arg(1)
        .query(client);

    // Add to delayed sorted set with score = timestamp * 0x1000
    let _: redis::RedisResult<u32> = client.zadd(&delayed_key, job_id, delayed_score as f64);

    // Set marker so delayed job promoter picks it up
    let _: redis::RedisResult<u32> = client.zadd(&marker_key, "0", marker_score as f64);
}

pub struct Worker<Data, Return>
where
    Data: DeserializeOwned + 'static,
    Return: Serialize + 'static,
{
    queue_name: String,
    concurrency: usize,
    active_tasks: usize,
    client: Client,
    receiver: tokio::sync::mpsc::Receiver<TaskEvent>,
    sender: tokio::sync::mpsc::Sender<TaskEvent>,
    process_fn: ProcessFn<Data, Return>,
    token: WorkerToken,
    drained: bool,
    shutdown_flag: Arc<AtomicBool>,
    lock_duration: u64,
    default_backoff: Option<BackoffStrategy>,
    limiter: Option<Limiter>,
}

impl<JobData, ReturnType> Worker<JobData, ReturnType>
where
    JobData: DeserializeOwned + 'static,
    ReturnType: Serialize + 'static,
{
    pub fn new(
        queue_name: String,
        redis_url: String,
        concurrency: usize,
        process_fn: ProcessFn<JobData, ReturnType>,
    ) -> Self {
        let client = Client::open(redis_url).unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(concurrency);

        Worker {
            queue_name,
            concurrency,
            active_tasks: 0,
            client,
            receiver,
            sender,
            process_fn,
            token: WorkerToken::new(),
            drained: false,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            lock_duration: DEFAULT_LOCK_DURATION,
            default_backoff: None,
            limiter: None,
        }
    }

    pub fn with_limiter(mut self, max: u32, duration: u64) -> Self {
        self.limiter = Some(Limiter { max, duration });
        self
    }

    pub fn with_lock_duration(mut self, lock_duration: u64) -> Self {
        self.lock_duration = lock_duration;
        self
    }

    pub fn with_backoff(mut self, strategy: BackoffStrategy) -> Self {
        self.default_backoff = Some(strategy);
        self
    }

    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
    }

    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        self.shutdown_flag.clone()
    }

    fn start_processor_task(&mut self) {
        let prefix = self.get_prefixed_key("");
        let token = self.token.next();
        let mut client = self.client.clone();
        let sender = self.sender.clone();
        let process_fn = self.process_fn;
        let lock_duration = self.lock_duration;
        let default_backoff = self.default_backoff.clone();
        let limiter = self.limiter.clone();

        tokio::spawn(async move {
            // Move to active script
            while let Ok(job) = MOVE_TO_ACTIVE.run::<JobData>(
                &prefix,
                &mut client,
                MoveToActiveArgs {
                    token: token.clone(),
                    lock_duration: 10_000,
                    limiter: limiter.clone(),
                },
            ) {
                match job {
                    MoveToActiveReturn::Job(job) => {
                        // Emit active event
                        emit_event(&mut client, &prefix, JobEvent::Active, &job.id);

                        match process_fn(&job) {
                            Ok(result) => {
                                // Move job to completed
                                let stringified_result = serde_json::to_string(&result).unwrap();

                                let keep_jobs = job
                                    .opts
                                    .remove_on_complete
                                    .as_ref()
                                    .map(|k| k.to_keep_jobs())
                                    .unwrap_or_default();

                                match MOVE_TO_FINISHED.run(
                                    &prefix,
                                    &mut client,
                                    &job.id,
                                    stringified_result.as_str(),
                                    MoveToFinishedTarget::Completed,
                                    MoveToFinishedArgs {
                                        token: token.clone(),
                                        keep_jobs,
                                        lock_duration,
                                        max_attempts: job.opts.attempts,
                                        max_metrics_size: 100,
                                        fail_parent_on_fail: false,
                                        remove_dependency_on_fail: false,
                                    },
                                ) {
                                    Ok(MoveToFinishedReturn::Ok) => {
                                        emit_event(
                                            &mut client,
                                            &prefix,
                                            JobEvent::Completed,
                                            &job.id,
                                        );
                                    }
                                    res => {
                                        println!("Error moving job to completed: {res:?}");
                                    }
                                }
                            }
                            Err(err) => {
                                let attempts_made = job.attempts_made.unwrap_or(0) + 1;

                                if should_retry(attempts_made, job.opts.attempts) {
                                    let effective_backoff = job.opts.backoff.as_ref().or(default_backoff.as_ref()).cloned();
                                    let delay = next_retry_delay(&effective_backoff, attempts_made);

                                    if delay > 0 {
                                        // Move to delayed set with backoff delay
                                        move_to_delayed(
                                            &mut client,
                                            &prefix,
                                            &job.id,
                                            &token,
                                            delay,
                                        );
                                        emit_event(
                                            &mut client,
                                            &prefix,
                                            JobEvent::Retrying,
                                            &job.id,
                                        );
                                    } else {
                                        // Immediate retry
                                        match RETRY_JOB.run(&prefix, &mut client, &job.id, &token, job.opts.lifo) {
                                            Ok(RetryJobReturn::Ok) => {
                                                emit_event(
                                                    &mut client,
                                                    &prefix,
                                                    JobEvent::Retrying,
                                                    &job.id,
                                                );
                                            }
                                            res => {
                                                println!("Error retrying job: {res:?}");
                                            }
                                        }
                                    }
                                } else {
                                    // Move job to failed
                                    let keep_jobs = job
                                        .opts
                                        .remove_on_fail
                                        .as_ref()
                                        .map(|k| k.to_keep_jobs())
                                        .unwrap_or_default();

                                    match MOVE_TO_FINISHED.run(
                                        &prefix,
                                        &mut client,
                                        &job.id,
                                        err.to_string().as_str(),
                                        MoveToFinishedTarget::Failed,
                                        MoveToFinishedArgs {
                                            token: token.clone(),
                                            keep_jobs,
                                            lock_duration,
                                            max_attempts: job.opts.attempts,
                                            max_metrics_size: 100,
                                            fail_parent_on_fail: false,
                                            remove_dependency_on_fail: false,
                                        },
                                    ) {
                                        Ok(MoveToFinishedReturn::Ok) => {
                                            emit_event(
                                                &mut client,
                                                &prefix,
                                                JobEvent::Failed,
                                                &job.id,
                                            );
                                        }
                                        res => {
                                            println!("Error moving job to failed: {res:?}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    MoveToActiveReturn::None => {
                        // No job to process
                        break;
                    }
                    MoveToActiveReturn::RateLimited(_) => {
                        break; // will retry after marker wait
                    }
                }
            }

            // Emits a signal to the worker that it's done processing jobs
            let _ = sender.send(TaskEvent::Freed).await;
        });
    }

    fn spawn_stall_checker(&self) {
        let client = self.client.clone();
        let prefix = self.get_prefixed_key("");
        let shutdown = self.shutdown_flag.clone();
        let interval_ms = self.lock_duration / 2;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));

            loop {
                interval.tick().await;

                if shutdown.load(Ordering::SeqCst) {
                    break;
                }

                let mut conn = match client.get_connection() {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                let active_key = format!("{prefix}active");

                // Get all job IDs in the active list
                let active_jobs: Vec<String> = match conn.lrange(&active_key, 0, -1) {
                    Ok(jobs) => jobs,
                    Err(_) => continue,
                };

                for job_id in active_jobs {
                    let lock_key = format!("{prefix}{job_id}:lock");
                    let exists: bool = conn.exists(&lock_key).unwrap_or(true);

                    if !exists {
                        // Job is stalled - lock expired but still in active list
                        emit_event(&mut client.clone(), &prefix, JobEvent::Stalled, &job_id);

                        // Check attempts to decide retry vs fail
                        let job_key = format!("{prefix}{job_id}");
                        let atm: u32 = conn.hget::<_, _, u32>(&job_key, "atm").unwrap_or(0);
                        let opts_str: String = match conn.hget(&job_key, "opts") {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        let opts: std::result::Result<crate::core::job::JobOptions, _> =
                            serde_json::from_str(&opts_str);
                        let max_attempts = opts.as_ref().map(|o| o.attempts).unwrap_or(1);

                        if should_retry(atm, max_attempts) {
                            // Re-queue: remove from active, push to wait
                            let wait_key = format!("{prefix}wait");
                            let _: redis::RedisResult<u32> = conn.lrem(&active_key, 0, &job_id);
                            let _: redis::RedisResult<u32> = conn.lpush(&wait_key, &job_id);
                            let _: redis::RedisResult<u32> = redis::cmd("HINCRBY")
                                .arg(&job_key)
                                .arg("atm")
                                .arg(1)
                                .query(&mut conn);
                            // Set marker to wake worker
                            let marker_key = format!("{prefix}marker");
                            let _: redis::RedisResult<u32> = conn.zadd(&marker_key, "1", 0.0);
                        } else {
                            // Move to failed
                            let failed_key = format!("{prefix}failed");
                            let _: redis::RedisResult<u32> = conn.lrem(&active_key, 0, &job_id);
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            let _: redis::RedisResult<u32> =
                                conn.zadd(&failed_key, &job_id, now as f64);
                            let _: redis::RedisResult<()> = redis::cmd("HMSET")
                                .arg(&job_key)
                                .arg("failedReason")
                                .arg("job stalled more than allowable limit")
                                .arg("finishedOn")
                                .arg(now.to_string())
                                .query(&mut conn);

                            emit_event(&mut client.clone(), &prefix, JobEvent::Failed, &job_id);
                        }
                    }
                }
            }
        });
    }

    pub async fn run(&mut self) -> Result<()> {
        // Spawn stall detection background task
        self.spawn_stall_checker();

        // Spawn signal handler
        let shutdown_flag = self.shutdown_flag.clone();
        tokio::spawn(async move {
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).unwrap();
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = sigterm.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                let _ = ctrl_c.await;
            }
            shutdown_flag.store(true, Ordering::SeqCst);
        });

        loop {
            if self.shutdown_flag.load(Ordering::SeqCst) {
                break;
            }

            // Does not clear all the buffer
            while self.active_tasks >= self.concurrency {
                if let Some(TaskEvent::Freed) = self.receiver.recv().await {
                    self.active_tasks -= 1;
                    self.drained = true;
                }

                if self.shutdown_flag.load(Ordering::SeqCst) {
                    break;
                }
            }

            if self.shutdown_flag.load(Ordering::SeqCst) {
                break;
            }

            if self.drained {
                // Marker is used to notify worker of new jobs.
                // Run blocking bzpopmin on a separate thread so we don't block the executor.
                let marker_key = self.get_prefixed_key("marker");
                let client = self.client.clone();
                let got_marker = tokio::task::spawn_blocking(move || {
                    let mut conn = client.get_connection().ok()?;
                    conn.bzpopmin::<String, (String, String, f64)>(marker_key, 2.)
                        .ok()
                })
                .await
                .unwrap_or(None);

                if self.shutdown_flag.load(Ordering::SeqCst) {
                    break;
                }

                if got_marker.is_none() {
                    continue;
                }

                self.drained = false;
            }

            self.active_tasks += 1;
            self.start_processor_task();
        }

        // Drain active tasks
        while self.active_tasks > 0 {
            if let Some(TaskEvent::Freed) = self.receiver.recv().await {
                self.active_tasks -= 1;
            }
        }

        Ok(())
    }

    fn get_prefixed_key(&self, key: &str) -> String {
        format!("bull:{}:{}", self.queue_name, key)
    }
}
