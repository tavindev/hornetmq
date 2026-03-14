use anyhow::Result;
use lazy_static::lazy_static;
use serde::Serialize;

use crate::{
    core::backoff::BackoffStrategy,
    scripts::add_standard_job::{AddStandardJob, AddStandardJobOpts},
};

lazy_static! {
    static ref ADD_STANDARD_JOB: AddStandardJob = AddStandardJob::new();
}

#[derive(Default)]
pub struct AddJobOptions {
    pub delay: Option<u64>,
    pub priority: Option<u32>,
    pub attempts: Option<u32>,
    pub backoff: Option<BackoffStrategy>,
}

pub struct Queue {
    name: String,
    client: redis::Client,
}

impl Queue {
    pub fn new(name: String, redis_url: String) -> Self {
        let client = redis::Client::open(redis_url).unwrap();
        Queue { name, client }
    }

    /// Add a job to the queue. Returns the job ID.
    pub fn add<D: Serialize>(
        &mut self,
        job_name: &str,
        data: D,
        opts: AddJobOptions,
    ) -> Result<String> {
        let prefix = format!("bull:{}:", self.name);
        let json_data = serde_json::to_string(&data)?;

        let script_opts = AddStandardJobOpts {
            delay: opts.delay,
            priority: opts.priority,
            attempts: opts.attempts,
            backoff: opts.backoff,
        };

        ADD_STANDARD_JOB.run(&prefix, &mut self.client, job_name, &json_data, script_opts)
    }
}
