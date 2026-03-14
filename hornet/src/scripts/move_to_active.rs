use std::time::SystemTime;

use crate::{
    core::job::{Job, JobBuilder},
    generate_script_struct,
    queue_keys::QueueKeys,
};

use anyhow::Result;
use redis::{FromRedisValue, ToRedisArgs};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

generate_script_struct!(MoveToActive, "./src/scripts/commands/moveToActive-11.lua");

impl MoveToActive {
    pub fn run<JobData: DeserializeOwned>(
        &self,
        prefix: &str,
        con: &mut impl redis::ConnectionLike,
        opts: MoveToActiveArgs,
    ) -> Result<MoveToActiveReturn<JobData>> {
        let mut script = &mut self.0.prepare_invoke();

        let timestamp = (SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64)
            .to_string();

        for key in [
            QueueKeys::Wait,
            QueueKeys::Active,
            QueueKeys::Prioritized,
            QueueKeys::Events,
            QueueKeys::Stalled,
            QueueKeys::Limiter,
            QueueKeys::Delayed,
            QueueKeys::Paused,
            QueueKeys::Meta,
            QueueKeys::Pc,
            QueueKeys::Marker,
        ] {
            script = script.key(key.with_prefix(prefix));
        }

        let res = script
            .arg(prefix)
            .arg(timestamp)
            .arg(opts)
            .invoke::<MoveToActiveReturn<JobData>>(con)?;

        Ok(res)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Limiter {
    pub max: u32,
    pub duration: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MoveToActiveArgs {
    pub token: String,
    #[serde(rename = "lockDuration")]
    pub lock_duration: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limiter: Option<Limiter>,
}

impl ToRedisArgs for MoveToActiveArgs {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        rmp_serde::encode::to_vec_named(self)
            .expect("MoveToActiveArgs serialization should never fail")
            .write_redis_args(out)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum MoveToActiveReturn<JobData> {
    Job(Job<JobData>),
    None,
    RateLimited(u64),
}

fn redis_type_err(msg: impl ToString) -> redis::RedisError {
    redis::RedisError::from((redis::ErrorKind::TypeError, "Job parse error", msg.to_string()))
}

fn parse_redis_str(data: &[u8]) -> redis::RedisResult<String> {
    String::from_utf8(data.to_vec()).map_err(redis_type_err)
}

fn parse_redis_field<T: std::str::FromStr>(data: &[u8], field: &str) -> redis::RedisResult<T>
where
    T::Err: std::fmt::Display,
{
    let s = parse_redis_str(data)?;
    s.parse::<T>().map_err(|e| redis_type_err(format!("invalid {field}: {e}")))
}

impl<JobData: DeserializeOwned> FromRedisValue for MoveToActiveReturn<JobData> {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        use redis::Value;

        match *v {
            Value::Bulk(ref items) => match items.as_slice() {
                [Value::Int(0), Value::Int(0), Value::Int(0), Value::Int(0)] => {
                    Ok(MoveToActiveReturn::None)
                }
                [Value::Int(0), Value::Int(0), Value::Int(expire), Value::Int(0)]
                    if *expire > 0 =>
                {
                    Ok(MoveToActiveReturn::RateLimited(*expire as u64))
                }
                [Value::Bulk(raw_job), Value::Data(job_id), Value::Int(_), Value::Int(_)] => {
                    let mut job_builder: JobBuilder<JobData> = JobBuilder::new();

                    job_builder = job_builder.id(parse_redis_str(job_id)?);

                    for slice in raw_job.chunks(2) {
                        if let [Value::Data(key), Value::Data(value)] = slice {
                            let key = parse_redis_str(key)?;

                            job_builder = match key.as_str() {
                                "name" => job_builder.name(parse_redis_str(value)?),
                                "data" => job_builder.data(
                                    serde_json::from_slice(value)
                                        .map_err(|e| redis_type_err(format!("invalid data: {e}")))?,
                                ),
                                "opts" => job_builder.opts(parse_redis_str(value)?),
                                "timestamp" => {
                                    job_builder.timestamp(parse_redis_field(value, "timestamp")?)
                                }
                                "delay" => {
                                    job_builder.delay(parse_redis_field(value, "delay")?)
                                }
                                "priority" => {
                                    job_builder.priority(parse_redis_field(value, "priority")?)
                                }
                                "processedOn" => {
                                    job_builder.processed_on(parse_redis_field(value, "processedOn")?)
                                }
                                "ats" => {
                                    job_builder.attempts_started(parse_redis_field(value, "ats")?)
                                }
                                "atm" => {
                                    job_builder.attempts_made(parse_redis_field(value, "atm")?)
                                }
                                _ => job_builder,
                            };
                        }
                    }

                    match job_builder.build() {
                        Ok(job) => Ok(MoveToActiveReturn::Job(job)),
                        Err(e) => Err(redis::RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Failed to build job from Redis data",
                            e,
                        ))),
                    }
                }
                _ => Err(redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Invalid response type",
                ))),
            },
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Invalid response type",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::queue_keys::QueueKeys;

    use super::*;

    #[test]
    fn loads() {
        let script = MoveToActive::new();
        let mut script = &mut script.0.prepare_invoke();
        let mut redis = redis::Client::open("redis://localhost:6379").unwrap();
        let prefix = "bull:my_queue:";

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        let keys: Vec<String> = vec![
            QueueKeys::Wait,
            QueueKeys::Active,
            QueueKeys::Prioritized,
            QueueKeys::Events,
            QueueKeys::Stalled,
            QueueKeys::Limiter,
            QueueKeys::Delayed,
            QueueKeys::Paused,
            QueueKeys::Meta,
            QueueKeys::Pc,
            QueueKeys::Marker,
        ]
        .iter()
        .map(|s| s.with_prefix(prefix))
        .collect();

        for key in keys {
            script = script.key(key)
        }

        let res = script
            .arg(prefix)
            .arg(timestamp)
            .arg(MoveToActiveArgs {
                token: "test".to_string(),
                lock_duration: 10_000,
                limiter: None,
            })
            .invoke(&mut redis);

        dbg!(&res);

        assert!(res.is_ok());

        let res: MoveToActiveReturn<String> = res.unwrap();

        dbg!(res);
    }
}
