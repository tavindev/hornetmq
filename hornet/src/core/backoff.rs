use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq)]
pub enum BackoffStrategy {
    Fixed(u64),
    Exponential { base: u64, max: u64 },
}

/// Serialize to BullMQ format: `{"type":"fixed","delay":N}` or
/// `{"type":"exponential","delay":N,"max":N}`.
impl Serialize for BackoffStrategy {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            BackoffStrategy::Fixed(delay) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "fixed")?;
                map.serialize_entry("delay", delay)?;
                map.end()
            }
            BackoffStrategy::Exponential { base, max } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "exponential")?;
                map.serialize_entry("delay", base)?;
                map.serialize_entry("max", max)?;
                map.end()
            }
        }
    }
}

/// Deserialize from BullMQ format.
impl<'de> Deserialize<'de> for BackoffStrategy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            #[serde(rename = "type")]
            kind: String,
            delay: u64,
            #[serde(default = "default_max")]
            max: u64,
        }

        fn default_max() -> u64 {
            u64::MAX
        }

        let h = Helper::deserialize(deserializer)?;
        match h.kind.as_str() {
            "fixed" => Ok(BackoffStrategy::Fixed(h.delay)),
            "exponential" => Ok(BackoffStrategy::Exponential {
                base: h.delay,
                max: h.max,
            }),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &["fixed", "exponential"],
            )),
        }
    }
}

/// Compute retry delay in milliseconds for the given attempt number (1-indexed).
pub fn compute_delay(strategy: &BackoffStrategy, attempt: u32) -> u64 {
    match strategy {
        BackoffStrategy::Fixed(ms) => *ms,
        BackoffStrategy::Exponential { base, max } => {
            let delay = base.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
            delay.min(*max)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_returns_same_value_every_attempt() {
        let strategy = BackoffStrategy::Fixed(1000);
        assert_eq!(compute_delay(&strategy, 1), 1000);
        assert_eq!(compute_delay(&strategy, 2), 1000);
        assert_eq!(compute_delay(&strategy, 100), 1000);
    }

    #[test]
    fn exponential_doubles_each_attempt() {
        let strategy = BackoffStrategy::Exponential {
            base: 1000,
            max: 60_000,
        };
        assert_eq!(compute_delay(&strategy, 1), 1000); // 1000 * 2^0
        assert_eq!(compute_delay(&strategy, 2), 2000); // 1000 * 2^1
        assert_eq!(compute_delay(&strategy, 3), 4000); // 1000 * 2^2
        assert_eq!(compute_delay(&strategy, 4), 8000); // 1000 * 2^3
    }

    #[test]
    fn exponential_caps_at_max() {
        let strategy = BackoffStrategy::Exponential {
            base: 1000,
            max: 5000,
        };
        assert_eq!(compute_delay(&strategy, 1), 1000);
        assert_eq!(compute_delay(&strategy, 2), 2000);
        assert_eq!(compute_delay(&strategy, 3), 4000);
        assert_eq!(compute_delay(&strategy, 4), 5000); // capped
        assert_eq!(compute_delay(&strategy, 10), 5000); // still capped
    }

    #[test]
    fn exponential_saturates_instead_of_overflow() {
        let strategy = BackoffStrategy::Exponential {
            base: u64::MAX,
            max: u64::MAX,
        };
        // Should not panic
        let result = compute_delay(&strategy, 100);
        assert_eq!(result, u64::MAX);
    }

    #[test]
    fn attempt_zero_treated_as_first() {
        let strategy = BackoffStrategy::Exponential {
            base: 1000,
            max: 60_000,
        };
        // attempt 0 → saturating_sub(1) = 0 → 2^0 = 1 → 1000
        assert_eq!(compute_delay(&strategy, 0), 1000);
    }

    // BullMQ wire format tests

    #[test]
    fn backoff_fixed_serializes_to_bullmq_format() {
        let strategy = BackoffStrategy::Fixed(2000);
        let json = serde_json::to_value(&strategy).unwrap();
        assert_eq!(json, serde_json::json!({"type": "fixed", "delay": 2000}));
    }

    #[test]
    fn backoff_exponential_serializes_to_bullmq_format() {
        let strategy = BackoffStrategy::Exponential {
            base: 1000,
            max: 30000,
        };
        let json = serde_json::to_value(&strategy).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "exponential", "delay": 1000, "max": 30000})
        );
    }

    #[test]
    fn backoff_fixed_deserializes_from_bullmq_format() {
        let json = r#"{"type":"fixed","delay":2000}"#;
        let strategy: BackoffStrategy = serde_json::from_str(json).unwrap();
        assert_eq!(strategy, BackoffStrategy::Fixed(2000));
    }

    #[test]
    fn backoff_exponential_deserializes_from_bullmq_format() {
        let json = r#"{"type":"exponential","delay":1000}"#;
        let strategy: BackoffStrategy = serde_json::from_str(json).unwrap();
        assert_eq!(
            strategy,
            BackoffStrategy::Exponential {
                base: 1000,
                max: u64::MAX,
            }
        );
    }

    #[test]
    fn backoff_exponential_deserializes_with_max() {
        let json = r#"{"type":"exponential","delay":1000,"max":30000}"#;
        let strategy: BackoffStrategy = serde_json::from_str(json).unwrap();
        assert_eq!(
            strategy,
            BackoffStrategy::Exponential {
                base: 1000,
                max: 30000,
            }
        );
    }
}
