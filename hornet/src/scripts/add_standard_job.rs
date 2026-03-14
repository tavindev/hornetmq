use std::time::SystemTime;

use crate::{generate_script_struct, queue_keys::QueueKeys};

use anyhow::Result;
use redis::ToRedisArgs;
use serde::Serialize;

generate_script_struct!(
    AddStandardJob,
    "./src/scripts/commands/addStandardJob-7.lua"
);

/// Options packed into ARGV[3] via msgpack (named map).
/// The lua script reads opts['delay'], opts['priority'], opts['lifo'], etc.
#[derive(Debug, Serialize)]
pub struct AddStandardJobOpts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff: Option<crate::core::backoff::BackoffStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifo: Option<bool>,
}

impl ToRedisArgs for AddStandardJobOpts {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        rmp_serde::to_vec_named(self).unwrap().write_redis_args(out)
    }
}

/// Build ARGV[1]: a msgpack array of 9 elements matching lua args[1]..args[9].
fn build_argv1(prefix: &str, job_name: &str, timestamp: &str, custom_id: &str) -> Vec<u8> {
    use rmp::encode;

    let mut buf = Vec::new();

    // 9-element array
    encode::write_array_len(&mut buf, 9).unwrap();

    // args[1] = prefix
    encode::write_str(&mut buf, prefix).unwrap();

    // args[2] = custom id ("" means auto-generate)
    encode::write_str(&mut buf, custom_id).unwrap();

    // args[3] = name
    encode::write_str(&mut buf, job_name).unwrap();

    // args[4] = timestamp (as integer)
    let ts: i64 = timestamp.parse().unwrap();
    encode::write_sint(&mut buf, ts).unwrap();

    // args[5] = parentKey (nil)
    encode::write_nil(&mut buf).unwrap();

    // args[6] = waitChildrenKey (nil)
    encode::write_nil(&mut buf).unwrap();

    // args[7] = parentDependenciesKey (nil)
    encode::write_nil(&mut buf).unwrap();

    // args[8] = parent (nil)
    encode::write_nil(&mut buf).unwrap();

    // args[9] = repeatJobKey (nil)
    encode::write_nil(&mut buf).unwrap();

    buf
}

impl AddStandardJob {
    pub fn run(
        &self,
        prefix: &str,
        client: &mut redis::Client,
        job_name: &str,
        data: &str,
        opts: AddStandardJobOpts,
        custom_id: &str,
    ) -> Result<String> {
        let mut script = &mut self.0.prepare_invoke();

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        // KEYS[1..7]: wait, paused, meta, id, completed, events, marker
        let keys: Vec<String> = [
            QueueKeys::Wait,
            QueueKeys::Paused,
            QueueKeys::Meta,
            QueueKeys::Custom("id".into()),
            QueueKeys::Custom("completed".into()),
            QueueKeys::Events,
            QueueKeys::Marker,
        ]
        .iter()
        .map(|s| s.with_prefix(prefix))
        .collect();

        for key in keys {
            script = script.key(key)
        }

        let argv1 = build_argv1(prefix, job_name, &timestamp, custom_id);

        // ARGV[1] = msgpacked args array
        script = script.arg(argv1);
        // ARGV[2] = JSON stringified job data
        script = script.arg(data);
        // ARGV[3] = msgpacked options
        script = script.arg(opts);

        let job_id: String = script.invoke(client)?;

        Ok(job_id)
    }
}
