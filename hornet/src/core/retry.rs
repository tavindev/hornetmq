use super::backoff::{self, BackoffStrategy};

pub fn should_retry(attempts_made: u32, max_attempts: u32) -> bool {
    attempts_made < max_attempts
}

/// Returns delay in ms before next retry. 0 means immediate.
pub fn next_retry_delay(strategy: Option<&BackoffStrategy>, attempt: u32) -> u64 {
    match strategy {
        Some(s) => backoff::compute_delay(s, attempt),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_retry_when_under_max() {
        assert!(should_retry(0, 3));
        assert!(should_retry(1, 3));
        assert!(should_retry(2, 3));
    }

    #[test]
    fn should_not_retry_at_max() {
        assert!(!should_retry(3, 3));
    }

    #[test]
    fn should_not_retry_over_max() {
        assert!(!should_retry(5, 3));
    }

    #[test]
    fn should_not_retry_when_max_is_zero() {
        assert!(!should_retry(0, 0));
    }

    #[test]
    fn no_strategy_means_immediate_retry() {
        assert_eq!(next_retry_delay(None, 1), 0);
        assert_eq!(next_retry_delay(None, 5), 0);
    }

    #[test]
    fn fixed_strategy_delay() {
        let strategy = Some(BackoffStrategy::Fixed(2000));
        assert_eq!(next_retry_delay(strategy.as_ref(), 1), 2000);
        assert_eq!(next_retry_delay(strategy.as_ref(), 3), 2000);
    }

    #[test]
    fn exponential_strategy_delay() {
        let strategy = Some(BackoffStrategy::Exponential {
            base: 500,
            max: 10_000,
        });
        assert_eq!(next_retry_delay(strategy.as_ref(), 1), 500);
        assert_eq!(next_retry_delay(strategy.as_ref(), 2), 1000);
        assert_eq!(next_retry_delay(strategy.as_ref(), 3), 2000);
    }
}
