use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BackoffStrategy {
    Fixed(u64),
    Exponential { base: u64, max: u64 },
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
}
