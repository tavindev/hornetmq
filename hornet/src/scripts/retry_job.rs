use anyhow::Result;
use redis::FromRedisValue;
use std::time::SystemTime;

use crate::{generate_script_struct, queue_keys::QueueKeys};

generate_script_struct!(RetryJob, "./src/scripts/commands/retryJob-10.lua");

#[derive(Debug)]
pub enum RetryJobReturn {
    Ok,
    MissingKey,
    MissingLock,
}

impl FromRedisValue for RetryJobReturn {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        match v {
            redis::Value::Int(0) => Ok(RetryJobReturn::Ok),
            redis::Value::Int(-1) => Ok(RetryJobReturn::MissingKey),
            redis::Value::Int(-2) => Ok(RetryJobReturn::MissingLock),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Unknown return value",
            ))),
        }
    }
}

impl RetryJob {
    pub fn run(
        &self,
        prefix: &str,
        con: &mut impl redis::ConnectionLike,
        job_id: &str,
        token: &str,
        lifo: bool,
    ) -> Result<RetryJobReturn> {
        let mut script = &mut self.0.prepare_invoke();

        let timestamp = (SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64)
            .to_string();

        for key in [
            QueueKeys::Active,
            QueueKeys::Wait,
            QueueKeys::Paused,
            QueueKeys::Custom(job_id.to_string()),
            QueueKeys::Meta,
            QueueKeys::Events,
            QueueKeys::Delayed,
            QueueKeys::Prioritized,
            QueueKeys::Pc,
            QueueKeys::Marker,
        ] {
            script = script.key(key.with_prefix(prefix));
        }

        let push_cmd = if lifo { "RPUSH" } else { "LPUSH" };

        let res = script
            .arg(prefix)
            .arg(timestamp)
            .arg(push_cmd)
            .arg(job_id)
            .arg(token)
            .invoke::<RetryJobReturn>(con)?;

        Ok(res)
    }
}
