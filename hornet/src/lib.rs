extern crate rmp;

pub mod core;
pub mod queue;
pub(crate) mod queue_keys;
pub(crate) mod scripts;
pub mod worker;

// Re-export core types at crate root
pub use core::backoff::BackoffStrategy;
pub use core::job::{Job, KeepJobs, KeepJobsConfig};
pub use queue::{AddJobOptions, JobCounts, Queue, RawJob};
pub use worker::Worker;
pub use scripts::move_to_active::Limiter;

pub use hornetmq_macros::worker;
