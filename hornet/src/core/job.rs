use serde::{Deserialize, Serialize};

use super::backoff::BackoffStrategy;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Waiting,
    Delayed,
    Active,
    Completed,
    Failed,
}

#[derive(Debug, Deserialize)]
pub struct JobOptions {
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub backoff: Option<BackoffStrategy>,
    #[serde(default)]
    pub delay: u64,
}

#[derive(Debug)]
pub struct Job<Data> {
    pub id: String,
    pub name: String,
    pub data: Data,
    pub opts: JobOptions,
    pub timestamp: u128,
    pub delay: u128,
    pub priority: u32,
    pub processed_on: u128,
    pub attempts_started: u32,
    pub attempts_made: Option<u32>,
}

pub struct JobBuilder<Data> {
    id: Option<String>,
    name: Option<String>,
    data: Option<Data>,
    opts: Option<JobOptions>,
    timestamp: Option<u128>,
    delay: Option<u128>,
    priority: Option<u32>,
    processed_on: Option<u128>,
    attempts_started: Option<u32>,
    attempts_made: Option<u32>,
}

impl<Data> Default for JobBuilder<Data> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Data> JobBuilder<Data> {
    pub fn new() -> Self {
        JobBuilder {
            id: None,
            name: None,
            data: None,
            opts: None,
            timestamp: None,
            delay: None,
            priority: None,
            processed_on: None,
            attempts_started: None,
            attempts_made: None,
        }
    }

    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn data(mut self, data: Data) -> Self {
        self.data = Some(data);
        self
    }

    pub fn opts(mut self, opts: String) -> Self {
        self.opts =
            Some(serde_json::from_str(&opts).expect("Failed to parse job options from string"));
        self
    }

    pub fn timestamp(mut self, timestamp: u128) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn delay(mut self, delay: u128) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn processed_on(mut self, processed_on: u128) -> Self {
        self.processed_on = Some(processed_on);
        self
    }

    pub fn attempts_started(mut self, attempts_started: u32) -> Self {
        self.attempts_started = Some(attempts_started);
        self
    }

    pub fn attempts_made(mut self, attempts_made: u32) -> Self {
        self.attempts_made = Some(attempts_made);
        self
    }

    pub fn build(self) -> Job<Data> {
        Job {
            id: self.id.unwrap(),
            name: self.name.unwrap(),
            data: self.data.unwrap(),
            opts: self.opts.unwrap(),
            timestamp: self.timestamp.unwrap(),
            delay: self.delay.unwrap(),
            priority: self.priority.unwrap(),
            processed_on: self.processed_on.unwrap(),
            attempts_started: self.attempts_started.unwrap(),
            attempts_made: self.attempts_made,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_builder_builds_correctly() {
        let job: Job<String> = JobBuilder::new()
            .id("1".into())
            .name("test".into())
            .data("payload".into())
            .opts(r#"{"attempts": 3}"#.into())
            .timestamp(1000)
            .delay(0)
            .priority(0)
            .processed_on(0)
            .attempts_started(0)
            .build();

        assert_eq!(job.id, "1");
        assert_eq!(job.name, "test");
        assert_eq!(job.data, "payload");
        assert_eq!(job.opts.attempts, 3);
        assert!(job.attempts_made.is_none());
    }

    #[test]
    fn job_builder_with_attempts_made() {
        let job: Job<String> = JobBuilder::new()
            .id("1".into())
            .name("test".into())
            .data("payload".into())
            .opts(r#"{"attempts": 3}"#.into())
            .timestamp(1000)
            .delay(0)
            .priority(0)
            .processed_on(0)
            .attempts_started(0)
            .attempts_made(2)
            .build();

        assert_eq!(job.attempts_made, Some(2));
    }

    #[test]
    fn job_state_equality() {
        assert_eq!(JobState::Waiting, JobState::Waiting);
        assert_ne!(JobState::Waiting, JobState::Active);
    }

    #[test]
    fn job_options_deserialize_with_backoff() {
        let json = r#"{"attempts": 5, "backoff": {"type": "exponential", "delay": 1000, "max": 30000}}"#;
        let opts: JobOptions = serde_json::from_str(json).unwrap();
        assert_eq!(opts.attempts, 5);
        assert!(opts.backoff.is_some());
    }

    #[test]
    fn job_options_deserialize_without_backoff() {
        let json = r#"{"attempts": 3}"#;
        let opts: JobOptions = serde_json::from_str(json).unwrap();
        assert_eq!(opts.attempts, 3);
        assert!(opts.backoff.is_none());
        assert_eq!(opts.delay, 0);
    }

    #[test]
    fn job_options_deserializes_bullmq_opts_with_backoff() {
        let json = r#"{"attempts":3,"backoff":{"type":"fixed","delay":5000},"delay":0}"#;
        let opts: JobOptions = serde_json::from_str(json).unwrap();
        assert_eq!(opts.attempts, 3);
        assert_eq!(
            opts.backoff,
            Some(crate::core::backoff::BackoffStrategy::Fixed(5000))
        );
        assert_eq!(opts.delay, 0);
    }

    #[test]
    fn job_options_deserializes_bullmq_opts_minimal() {
        let json = r#"{}"#;
        let opts: JobOptions = serde_json::from_str(json).unwrap();
        assert_eq!(opts.attempts, 0);
        assert!(opts.backoff.is_none());
        assert_eq!(opts.delay, 0);
    }

    #[test]
    fn job_options_deserializes_bullmq_opts_with_extra_fields() {
        let json = r#"{"attempts":2,"delay":0,"removeOnComplete":true,"removeOnFail":false,"jobId":"custom-123"}"#;
        let opts: JobOptions = serde_json::from_str(json).unwrap();
        assert_eq!(opts.attempts, 2);
        assert_eq!(opts.delay, 0);
        assert!(opts.backoff.is_none());
    }
}
