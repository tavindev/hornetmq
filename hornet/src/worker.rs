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
    FreedEmpty,
}

type ProcessFn<Data, Return> = Arc<dyn Fn(&Job<Data>) -> Result<Return> + Send + Sync>;

fn emit_event(conn: &mut impl redis::ConnectionLike, prefix: &str, event: JobEvent, job_id: &str) {
    let events_key = format!("{prefix}events");
    let result: redis::RedisResult<String> = redis::cmd("XADD")
        .arg(&events_key)
        .arg("*")
        .arg("event")
        .arg(event.as_str())
        .arg("jobId")
        .arg(job_id)
        .query(conn);
    if let Err(e) = result {
        eprintln!("Failed to emit {event:?} event for job {job_id}: {e}");
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

fn move_to_delayed(conn: &mut impl redis::ConnectionLike, prefix: &str, job_id: &str, token: &str, delay_ms: u64) -> redis::RedisResult<()> {
    let now = now_ms();
    let delayed_score = (now + delay_ms).saturating_mul(0x1000);
    let marker_score = now + delay_ms;

    let active_key = format!("{prefix}active");
    let delayed_key = format!("{prefix}delayed");
    let job_key = format!("{prefix}{job_id}");
    let lock_key = format!("{job_key}:lock");
    let marker_key = format!("{prefix}marker");

    // Remove lock if owned by this token
    let lock_val: redis::RedisResult<String> = redis::cmd("GET").arg(&lock_key).query(conn);
    if let Ok(val) = lock_val {
        if val == token {
            redis::cmd("DEL").arg(&lock_key).query::<()>(conn)?;
        }
    }

    // Remove from active list
    redis::cmd("LREM").arg(&active_key).arg(0).arg(job_id).query::<u32>(conn)?;

    // Increment attempts made
    redis::cmd("HINCRBY").arg(&job_key).arg("atm").arg(1).query::<u32>(conn)?;

    // Add to delayed sorted set with score = timestamp * 0x1000
    redis::cmd("ZADD").arg(&delayed_key).arg(delayed_score).arg(job_id).query::<u32>(conn)?;

    // Set marker so delayed job promoter picks it up
    redis::cmd("ZADD").arg(&marker_key).arg(marker_score).arg("0").query::<u32>(conn)?;

    Ok(())
}

pub struct Worker<Data, Return>
where
    Data: DeserializeOwned + 'static,
    Return: Serialize + 'static,
{
    prefix: String,
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
        queue_name: impl Into<String>,
        redis_url: &str,
        concurrency: usize,
        process_fn: fn(&Job<JobData>) -> Result<ReturnType>,
    ) -> Result<Self> {
        let client = Client::open(redis_url)?;
        let queue_name = queue_name.into();
        let prefix = format!("bull:{queue_name}:");
        let (sender, receiver) = tokio::sync::mpsc::channel(concurrency);

        Ok(Worker {
            prefix,
            concurrency,
            active_tasks: 0,
            client,
            receiver,
            sender,
            process_fn: Arc::new(process_fn),
            token: WorkerToken::new(),
            drained: false,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            lock_duration: DEFAULT_LOCK_DURATION,
            default_backoff: None,
            limiter: None,
        })
    }

    pub fn with_processor<F>(
        queue_name: impl Into<String>,
        redis_url: &str,
        concurrency: usize,
        processor: F,
    ) -> Result<Self>
    where
        F: Fn(&Job<JobData>) -> Result<ReturnType> + Send + Sync + 'static,
    {
        let client = Client::open(redis_url)?;
        let queue_name = queue_name.into();
        let prefix = format!("bull:{queue_name}:");
        let (sender, receiver) = tokio::sync::mpsc::channel(concurrency);

        Ok(Worker {
            prefix,
            concurrency,
            active_tasks: 0,
            client,
            receiver,
            sender,
            process_fn: Arc::new(processor),
            token: WorkerToken::new(),
            drained: false,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            lock_duration: DEFAULT_LOCK_DURATION,
            default_backoff: None,
            limiter: None,
        })
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
        let client = self.client.clone();
        let sender = self.sender.clone();
        let process_fn = self.process_fn.clone();
        let lock_duration = self.lock_duration;
        let default_backoff = self.default_backoff.clone();
        let limiter = self.limiter.clone();

        tokio::spawn(async move {
            struct FreedOnDrop(Option<tokio::sync::mpsc::Sender<TaskEvent>>);
            impl Drop for FreedOnDrop {
                fn drop(&mut self) {
                    if let Some(s) = self.0.take() {
                        let _ = s.try_send(TaskEvent::FreedEmpty);
                    }
                }
            }
            let mut _guard = FreedOnDrop(Some(sender.clone()));

            let mut conn = match client.get_connection() {
                Ok(c) => c,
                Err(_) => {
                    return;
                }
            };

            let mut processed_any = false;

            // Move to active script
            while let Ok(job) = MOVE_TO_ACTIVE.run::<JobData>(
                &prefix,
                &mut conn,
                MoveToActiveArgs {
                    token: token.clone(),
                    lock_duration: lock_duration as u32,
                    limiter: limiter.clone(),
                },
            ) {
                match job {
                    MoveToActiveReturn::Job(job) => {
                        processed_any = true;

                        // Emit active event
                        emit_event(&mut conn, &prefix, JobEvent::Active, &job.id);

                        match process_fn(&job) {
                            Ok(result) => {
                                // Move job to completed
                                let stringified_result = match serde_json::to_string(&result) {
                                    Ok(s) => s,
                                    Err(e) => format!("\"serialization error: {e}\""),
                                };

                                let keep_jobs = job
                                    .opts
                                    .remove_on_complete
                                    .as_ref()
                                    .map(|k| k.to_keep_jobs())
                                    .unwrap_or_default();

                                match MOVE_TO_FINISHED.run(
                                    &prefix,
                                    &mut conn,
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
                                            &mut conn,
                                            &prefix,
                                            JobEvent::Completed,
                                            &job.id,
                                        );
                                    }
                                    res => {
                                        eprintln!("Error moving job to completed: {res:?}");
                                    }
                                }
                            }
                            Err(err) => {
                                let attempts_made = job.attempts_made.unwrap_or(0) + 1;

                                if should_retry(attempts_made, job.opts.attempts) {
                                    let effective_backoff = job.opts.backoff.as_ref().or(default_backoff.as_ref());
                                    let delay = next_retry_delay(effective_backoff, attempts_made);

                                    if delay > 0 {
                                        // Move to delayed set with backoff delay
                                        match move_to_delayed(
                                            &mut conn,
                                            &prefix,
                                            &job.id,
                                            &token,
                                            delay,
                                        ) {
                                            Ok(()) => {
                                                emit_event(
                                                    &mut conn,
                                                    &prefix,
                                                    JobEvent::Retrying,
                                                    &job.id,
                                                );
                                            }
                                            Err(e) => {
                                                eprintln!("Error moving job to delayed: {e}");
                                            }
                                        }
                                    } else {
                                        // Immediate retry
                                        match RETRY_JOB.run(&prefix, &mut conn, &job.id, &token, job.opts.lifo) {
                                            Ok(RetryJobReturn::Ok) => {
                                                emit_event(
                                                    &mut conn,
                                                    &prefix,
                                                    JobEvent::Retrying,
                                                    &job.id,
                                                );
                                            }
                                            res => {
                                                eprintln!("Error retrying job: {res:?}");
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
                                        &mut conn,
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
                                                &mut conn,
                                                &prefix,
                                                JobEvent::Failed,
                                                &job.id,
                                            );
                                        }
                                        res => {
                                            eprintln!("Error moving job to failed: {res:?}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    MoveToActiveReturn::None => {
                        break;
                    }
                    MoveToActiveReturn::RateLimited(_) => {
                        break; // will retry after marker wait
                    }
                }
            }

            // Disarm the guard and send the appropriate signal
            _guard.0 = None;
            if processed_any {
                let _ = sender.send(TaskEvent::Freed).await;
            } else {
                let _ = sender.send(TaskEvent::FreedEmpty).await;
            }
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
                        emit_event(&mut conn, &prefix, JobEvent::Stalled, &job_id);

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
                            let _: redis::RedisResult<u32> = redis::cmd("HINCRBY")
                                .arg(&job_key)
                                .arg("ats")
                                .arg(1)
                                .query(&mut conn);
                            // Set marker to wake worker
                            let marker_key = format!("{prefix}marker");
                            let _: redis::RedisResult<u32> = conn.zadd(&marker_key, "1", 0.0);
                        } else {
                            // Move to failed
                            let failed_key = format!("{prefix}failed");
                            let _: redis::RedisResult<u32> = conn.lrem(&active_key, 0, &job_id);
                            let now = now_ms();
                            let _: redis::RedisResult<u32> =
                                conn.zadd(&failed_key, &job_id, now);
                            let _: redis::RedisResult<()> = redis::cmd("HMSET")
                                .arg(&job_key)
                                .arg("failedReason")
                                .arg("job stalled more than allowable limit")
                                .arg("finishedOn")
                                .arg(now.to_string())
                                .query(&mut conn);

                            emit_event(&mut conn, &prefix, JobEvent::Failed, &job_id);
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

            while self.active_tasks >= self.concurrency {
                match self.receiver.recv().await {
                    Some(TaskEvent::Freed) => {
                        self.active_tasks -= 1;
                    }
                    Some(TaskEvent::FreedEmpty) => {
                        self.active_tasks -= 1;
                        self.drained = true;
                    }
                    None => break,
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
            match self.receiver.recv().await {
                Some(TaskEvent::Freed | TaskEvent::FreedEmpty) => {
                    self.active_tasks -= 1;
                }
                None => break,
            }
        }

        Ok(())
    }

    fn get_prefixed_key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}
