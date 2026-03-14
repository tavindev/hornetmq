extern crate rmp;

pub mod core;
pub mod queue;
pub(crate) mod queue_keys;
pub(crate) mod scripts;
pub mod worker;

// Re-export core types at crate root
pub use core::backoff::BackoffStrategy;
pub use core::job;
pub use core::job::Job;
pub use queue::Queue;
pub use worker::Worker;

pub use hornetmq_macros::worker;
