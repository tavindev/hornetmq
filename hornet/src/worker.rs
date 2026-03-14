use crate::{
    core::job::Job,
    scripts::{
        move_to_active::{MoveToActive, MoveToActiveArgs, MoveToActiveReturn},
        move_to_finished::{
            KeepJobs, MoveToFinished, MoveToFinishedArgs, MoveToFinishedReturn,
            MoveToFinishedTarget,
        },
        retry_job::{RetryJob, RetryJobReturn},
    },
};
use anyhow::Result;
use lazy_static::lazy_static;
use redis::{Client, Commands};
use serde::{de::DeserializeOwned, Serialize};
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
        }
    }

    fn start_processor_task(&mut self) {
        let prefix = self.get_prefixed_key("");
        let token = self.token.next();
        let mut client = self.client.clone();
        let sender = self.sender.clone();
        let process_fn = self.process_fn;

        let _ = tokio::spawn(async move {
            // Move to active script
            while let Ok(job) = MOVE_TO_ACTIVE.run::<JobData>(
                &prefix,
                &mut client,
                MoveToActiveArgs {
                    token: token.clone(),
                    lock_duration: 10_000,
                },
            ) {
                match job {
                    MoveToActiveReturn::Job(job) => {
                        match process_fn(&job) {
                            Ok(result) => {
                                // Move job to completed
                                let stringified_result = serde_json::to_string(&result).unwrap();

                                match MOVE_TO_FINISHED.run(
                                    &prefix,
                                    &mut client,
                                    &job.id,
                                    stringified_result.as_str(),
                                    MoveToFinishedTarget::Completed,
                                    MoveToFinishedArgs {
                                        token: token.clone(),
                                        keep_jobs: KeepJobs { count: -1 },
                                        lock_duration: DEFAULT_LOCK_DURATION,
                                        max_attempts: 1,
                                        max_metrics_size: 100,
                                        fail_parent_on_fail: false,
                                        remove_dependency_on_fail: false,
                                    },
                                ) {
                                    Ok(MoveToFinishedReturn::Ok) => {}
                                    res => {
                                        println!("Error moving job to completed: {:?}", res);
                                    }
                                }
                            }
                            Err(err) => {
                                // Check if we should retry
                                if job.attempts_made.unwrap_or(0) + 1 < job.opts.attempts {
                                    match RETRY_JOB.run(&prefix, &mut client, &job.id, &token) {
                                        Ok(RetryJobReturn::Ok) => {
                                            println!("Retrying job");
                                        }
                                        res => {
                                            println!("Error retrying job: {:?}", res);
                                        }
                                    }
                                } else {
                                    // Move job to failed
                                    match MOVE_TO_FINISHED.run(
                                        &prefix,
                                        &mut client,
                                        &job.id,
                                        err.to_string().as_str(),
                                        MoveToFinishedTarget::Failed,
                                        MoveToFinishedArgs {
                                            token: token.clone(),
                                            keep_jobs: KeepJobs { count: -1 },
                                            lock_duration: DEFAULT_LOCK_DURATION,
                                            max_attempts: job.opts.attempts,
                                            max_metrics_size: 100,
                                            fail_parent_on_fail: false,
                                            remove_dependency_on_fail: false,
                                        },
                                    ) {
                                        Ok(MoveToFinishedReturn::Ok) => {}
                                        res => {
                                            println!("Error moving job to failed: {:?}", res);
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
                }
            }

            // Emits a signal to the worker that it's done processing jobs
            let _ = sender.send(TaskEvent::Freed).await;
        });
    }

    pub async fn run(&mut self) {
        let mut connection = self.client.get_connection().unwrap();

        loop {
            // Does not clear all the buffer
            // What if a message is dropped?
            while self.active_tasks >= self.concurrency {
                if let Some(TaskEvent::Freed) = self.receiver.recv().await {
                    self.active_tasks -= 1;
                    self.drained = true;
                }
            }

            if self.drained {
                // Marker is used to notify worker of new jobs
                if let Err(_) = connection.bzpopmin::<String, (String, String, f64)>(
                    self.get_prefixed_key("marker"),
                    10000.,
                ) {
                    continue;
                }

                self.drained = false;
            }

            self.active_tasks += 1;
            self.start_processor_task();
        }
    }

    fn get_prefixed_key(&self, key: &str) -> String {
        format!("bull:{}:{}", self.queue_name, key)
    }
}
