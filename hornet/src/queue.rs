use std::collections::HashMap;

use anyhow::Result;
use lazy_static::lazy_static;
use redis::Commands;
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
    pub job_id: Option<String>,
    pub lifo: Option<bool>,
    pub remove_on_complete: Option<crate::core::job::KeepJobs>,
    pub remove_on_fail: Option<crate::core::job::KeepJobs>,
}

#[derive(Debug, Clone)]
pub struct RawJob {
    pub id: String,
    pub name: String,
    pub data: String,
    pub opts: String,
    pub timestamp: u64,
    pub delay: u64,
    pub priority: u32,
}

#[derive(Debug, Clone, Default)]
pub struct JobCounts {
    pub waiting: u64,
    pub active: u64,
    pub completed: u64,
    pub failed: u64,
    pub delayed: u64,
    pub prioritized: u64,
    pub paused: u64,
}

pub struct Queue {
    prefix: String,
    client: redis::Client,
}

impl Queue {
    pub fn new(name: impl Into<String>, redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let name = name.into();
        Ok(Queue { prefix: format!("bull:{name}:"), client })
    }

    /// Add a job to the queue. Returns the job ID.
    pub fn add<D: Serialize>(
        &mut self,
        job_name: &str,
        data: D,
        opts: AddJobOptions,
    ) -> Result<String> {
        let prefix = &self.prefix;
        let json_data = serde_json::to_string(&data)?;

        let script_opts = AddStandardJobOpts {
            delay: opts.delay,
            priority: opts.priority,
            attempts: opts.attempts,
            backoff: opts.backoff,
            lifo: opts.lifo,
        };

        let custom_id = opts.job_id.as_deref().unwrap_or("");
        let mut conn = self.client.get_connection()?;
        ADD_STANDARD_JOB.run(prefix, &mut conn, job_name, &json_data, script_opts, custom_id)
    }

    /// Get a single job by ID.
    pub fn get_job(&mut self, id: &str) -> Result<Option<RawJob>> {
        let mut conn = self.client.get_connection()?;
        let key = format!("{}{id}", self.prefix);
        let exists: bool = conn.exists(&key)?;
        if !exists {
            return Ok(None);
        }
        let mut fields: HashMap<String, String> =
            redis::cmd("HGETALL").arg(&key).query(&mut conn)?;
        Ok(Some(RawJob {
            id: id.to_string(),
            timestamp: fields
                .get("timestamp")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            delay: fields
                .get("delay")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            priority: fields
                .get("priority")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            name: fields.remove("name").unwrap_or_default(),
            data: fields.remove("data").unwrap_or_default(),
            opts: fields.remove("opts").unwrap_or_default(),
        }))
    }

    /// Get jobs in a given state. State can be "wait", "active", "completed",
    /// "failed", "delayed", "prioritized", or "paused".
    pub fn get_jobs(&mut self, state: &str, start: isize, end: isize) -> Result<Vec<RawJob>> {
        let mut conn = self.client.get_connection()?;
        let prefix = &self.prefix;
        let state_key = format!("{prefix}{state}");

        let job_ids: Vec<String> = match state {
            "wait" | "active" | "paused" => redis::cmd("LRANGE")
                .arg(&state_key)
                .arg(start)
                .arg(end)
                .query(&mut conn)?,
            "completed" | "failed" | "delayed" | "prioritized" => redis::cmd("ZRANGE")
                .arg(&state_key)
                .arg(start)
                .arg(end)
                .query(&mut conn)?,
            _ => return Err(anyhow::anyhow!("unknown state: {state}")),
        };

        let mut jobs = Vec::new();
        for id in job_ids {
            if let Some(job) = self.get_job(&id)? {
                jobs.push(job);
            }
        }
        Ok(jobs)
    }

    /// Get counts of jobs in each state.
    pub fn get_job_counts(&mut self) -> Result<JobCounts> {
        let mut conn = self.client.get_connection()?;
        let prefix = &self.prefix;

        let waiting: u64 = redis::cmd("LLEN")
            .arg(format!("{prefix}wait"))
            .query(&mut conn)?;
        let active: u64 = redis::cmd("LLEN")
            .arg(format!("{prefix}active"))
            .query(&mut conn)?;
        let completed: u64 = redis::cmd("ZCARD")
            .arg(format!("{prefix}completed"))
            .query(&mut conn)?;
        let failed: u64 = redis::cmd("ZCARD")
            .arg(format!("{prefix}failed"))
            .query(&mut conn)?;
        let delayed: u64 = redis::cmd("ZCARD")
            .arg(format!("{prefix}delayed"))
            .query(&mut conn)?;
        let prioritized: u64 = redis::cmd("ZCARD")
            .arg(format!("{prefix}prioritized"))
            .query(&mut conn)?;
        let paused: u64 = redis::cmd("LLEN")
            .arg(format!("{prefix}paused"))
            .query(&mut conn)?;

        Ok(JobCounts {
            waiting,
            active,
            completed,
            failed,
            delayed,
            prioritized,
            paused,
        })
    }

    /// Pause the queue — sets meta.paused and moves waiting jobs to the paused list.
    pub fn pause(&mut self) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let prefix = &self.prefix;
        let meta_key = format!("{prefix}meta");
        let wait_key = format!("{prefix}wait");
        let paused_key = format!("{prefix}paused");

        redis::cmd("HSET")
            .arg(&meta_key)
            .arg("paused")
            .arg("1")
            .query::<()>(&mut conn)?;

        let paused_len: u64 = redis::cmd("LLEN")
            .arg(&paused_key)
            .query(&mut conn)?;
        if paused_len == 0 {
            let wait_len: u64 = redis::cmd("LLEN")
                .arg(&wait_key)
                .query(&mut conn)?;
            if wait_len > 0 {
                redis::cmd("RENAME")
                    .arg(&wait_key)
                    .arg(&paused_key)
                    .query::<()>(&mut conn)?;
            }
        } else {
            loop {
                let val: Option<String> = redis::cmd("RPOPLPUSH")
                    .arg(&wait_key)
                    .arg(&paused_key)
                    .query(&mut conn)?;
                if val.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Resume the queue — removes meta.paused and moves paused jobs back to wait.
    pub fn resume(&mut self) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let prefix = &self.prefix;
        let meta_key = format!("{prefix}meta");
        let wait_key = format!("{prefix}wait");
        let paused_key = format!("{prefix}paused");
        let marker_key = format!("{prefix}marker");

        redis::cmd("HDEL")
            .arg(&meta_key)
            .arg("paused")
            .query::<()>(&mut conn)?;

        let wait_len: u64 = redis::cmd("LLEN")
            .arg(&wait_key)
            .query(&mut conn)?;
        if wait_len == 0 {
            let paused_len: u64 = redis::cmd("LLEN")
                .arg(&paused_key)
                .query(&mut conn)?;
            if paused_len > 0 {
                redis::cmd("RENAME")
                    .arg(&paused_key)
                    .arg(&wait_key)
                    .query::<()>(&mut conn)?;
            }
        } else {
            loop {
                let val: Option<String> = redis::cmd("RPOPLPUSH")
                    .arg(&paused_key)
                    .arg(&wait_key)
                    .query(&mut conn)?;
                if val.is_none() {
                    break;
                }
            }
        }

        redis::cmd("ZADD")
            .arg(&marker_key)
            .arg(0)
            .arg("0")
            .query::<()>(&mut conn)?;
        Ok(())
    }

    /// Check whether the queue is paused.
    pub fn is_paused(&mut self) -> Result<bool> {
        let mut conn = self.client.get_connection()?;
        let meta_key = format!("{}meta", self.prefix);
        let exists: bool = redis::cmd("HEXISTS")
            .arg(&meta_key)
            .arg("paused")
            .query(&mut conn)?;
        Ok(exists)
    }

    /// Drain the queue — remove all waiting, delayed, prioritized, and paused jobs.
    pub fn drain(&mut self) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let prefix = &self.prefix;

        let wait_ids: Vec<String> = redis::cmd("LRANGE")
            .arg(format!("{prefix}wait"))
            .arg(0isize)
            .arg(-1isize)
            .query(&mut conn)?;
        let paused_ids: Vec<String> = redis::cmd("LRANGE")
            .arg(format!("{prefix}paused"))
            .arg(0isize)
            .arg(-1isize)
            .query(&mut conn)?;
        let delayed_ids: Vec<String> = redis::cmd("ZRANGE")
            .arg(format!("{prefix}delayed"))
            .arg(0isize)
            .arg(-1isize)
            .query(&mut conn)?;
        let prioritized_ids: Vec<String> = redis::cmd("ZRANGE")
            .arg(format!("{prefix}prioritized"))
            .arg(0isize)
            .arg(-1isize)
            .query(&mut conn)?;

        for id in wait_ids
            .iter()
            .chain(paused_ids.iter())
            .chain(delayed_ids.iter())
            .chain(prioritized_ids.iter())
        {
            let job_key = format!("{prefix}{id}");
            redis::cmd("DEL").arg(&job_key).query::<()>(&mut conn)?;
        }

        redis::cmd("DEL")
            .arg(format!("{prefix}wait"))
            .arg(format!("{prefix}paused"))
            .arg(format!("{prefix}delayed"))
            .arg(format!("{prefix}prioritized"))
            .query::<()>(&mut conn)?;

        Ok(())
    }

    /// Remove a specific job by ID from all state lists and delete its hash/associated keys.
    pub fn remove_job(&mut self, id: &str) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let prefix = &self.prefix;
        let job_key = format!("{prefix}{id}");

        redis::cmd("LREM")
            .arg(format!("{prefix}wait"))
            .arg(0)
            .arg(id)
            .query::<u32>(&mut conn)?;
        redis::cmd("LREM")
            .arg(format!("{prefix}active"))
            .arg(0)
            .arg(id)
            .query::<u32>(&mut conn)?;
        redis::cmd("LREM")
            .arg(format!("{prefix}paused"))
            .arg(0)
            .arg(id)
            .query::<u32>(&mut conn)?;
        redis::cmd("ZREM")
            .arg(format!("{prefix}completed"))
            .arg(id)
            .query::<u32>(&mut conn)?;
        redis::cmd("ZREM")
            .arg(format!("{prefix}failed"))
            .arg(id)
            .query::<u32>(&mut conn)?;
        redis::cmd("ZREM")
            .arg(format!("{prefix}delayed"))
            .arg(id)
            .query::<u32>(&mut conn)?;
        redis::cmd("ZREM")
            .arg(format!("{prefix}prioritized"))
            .arg(id)
            .query::<u32>(&mut conn)?;

        redis::cmd("DEL")
            .arg(&job_key)
            .arg(format!("{job_key}:lock"))
            .arg(format!("{job_key}:logs"))
            .arg(format!("{job_key}:processed"))
            .arg(format!("{job_key}:dependencies"))
            .query::<()>(&mut conn)?;

        Ok(())
    }

    /// Add multiple jobs to the queue sequentially. Returns the list of job IDs.
    pub fn add_bulk<D: Serialize>(
        &mut self,
        jobs: Vec<(&str, D, AddJobOptions)>,
    ) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        for (name, data, opts) in jobs {
            let id = self.add(name, data, opts)?;
            ids.push(id);
        }
        Ok(ids)
    }

    /// Close the queue, dropping the Redis client.
    pub fn close(self) {
        drop(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn unique_queue_name() -> String {
        format!("test-queue-api-{}", Uuid::new_v4())
    }

    fn cleanup(queue_name: &str) {
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let mut conn = client.get_connection().unwrap();
        let prefix = format!("bull:{}:", queue_name);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{}*", prefix))
            .query(&mut conn)
            .unwrap_or_default();
        for key in keys {
            redis::cmd("DEL")
                .arg(&key)
                .query::<()>(&mut conn)
                .unwrap_or(());
        }
    }

    fn make_queue(name: &str) -> Queue {
        Queue::new(name, "redis://localhost:6379").unwrap()
    }

    #[test]
    fn test_new_with_bad_url_returns_err() {
        let result = Queue::new("test", "not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_job() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        let id = q
            .add("my-job", serde_json::json!({"hello": "world"}), AddJobOptions::default())
            .unwrap();
        let job = q.get_job(&id).unwrap().expect("job should exist");
        assert_eq!(job.id, id);
        assert_eq!(job.name, "my-job");
        assert!(job.data.contains("hello"));
        cleanup(&name);
    }

    #[test]
    fn test_get_job_not_found() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        let job = q.get_job("nonexistent-id").unwrap();
        assert!(job.is_none());
        cleanup(&name);
    }

    #[test]
    fn test_get_jobs() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        q.add("j1", serde_json::json!(1), AddJobOptions::default()).unwrap();
        q.add("j2", serde_json::json!(2), AddJobOptions::default()).unwrap();
        let jobs = q.get_jobs("wait", 0, -1).unwrap();
        assert_eq!(jobs.len(), 2);
        cleanup(&name);
    }

    #[test]
    fn test_get_job_counts() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        q.add("a", serde_json::json!(1), AddJobOptions::default()).unwrap();
        q.add("b", serde_json::json!(2), AddJobOptions::default()).unwrap();
        let counts = q.get_job_counts().unwrap();
        assert_eq!(counts.waiting, 2);
        assert_eq!(counts.active, 0);
        cleanup(&name);
    }

    #[test]
    fn test_pause_resume() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        q.add("x", serde_json::json!("data"), AddJobOptions::default()).unwrap();

        assert!(!q.is_paused().unwrap());

        q.pause().unwrap();
        assert!(q.is_paused().unwrap());

        let counts = q.get_job_counts().unwrap();
        assert_eq!(counts.waiting, 0);
        assert_eq!(counts.paused, 1);

        q.resume().unwrap();
        assert!(!q.is_paused().unwrap());

        let counts = q.get_job_counts().unwrap();
        assert_eq!(counts.waiting, 1);
        assert_eq!(counts.paused, 0);

        cleanup(&name);
    }

    #[test]
    fn test_drain() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        q.add("a", serde_json::json!(1), AddJobOptions::default()).unwrap();
        q.add("b", serde_json::json!(2), AddJobOptions::default()).unwrap();

        let counts = q.get_job_counts().unwrap();
        assert_eq!(counts.waiting, 2);

        q.drain().unwrap();

        let counts = q.get_job_counts().unwrap();
        assert_eq!(counts.waiting, 0);

        cleanup(&name);
    }

    #[test]
    fn test_remove_job() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        let id = q
            .add("removeme", serde_json::json!("bye"), AddJobOptions::default())
            .unwrap();

        assert!(q.get_job(&id).unwrap().is_some());

        q.remove_job(&id).unwrap();

        assert!(q.get_job(&id).unwrap().is_none());
        let counts = q.get_job_counts().unwrap();
        assert_eq!(counts.waiting, 0);

        cleanup(&name);
    }

    #[test]
    fn test_add_bulk() {
        let name = unique_queue_name();
        let mut q = make_queue(&name);
        let ids = q
            .add_bulk(vec![
                ("j1", serde_json::json!(1), AddJobOptions::default()),
                ("j2", serde_json::json!(2), AddJobOptions::default()),
                ("j3", serde_json::json!(3), AddJobOptions::default()),
            ])
            .unwrap();
        assert_eq!(ids.len(), 3);
        let counts = q.get_job_counts().unwrap();
        assert_eq!(counts.waiting, 3);
        cleanup(&name);
    }
}
