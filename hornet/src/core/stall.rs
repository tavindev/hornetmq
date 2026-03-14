/// Returns true if a job's lock has expired (job is stalled).
/// `lock_expires_at` and `now` are both in milliseconds since epoch.
pub fn is_stalled(lock_expires_at: u64, now: u64) -> bool {
    now > lock_expires_at
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_stalled_when_lock_still_valid() {
        assert!(!is_stalled(1000, 500));
    }

    #[test]
    fn not_stalled_at_exact_expiry() {
        assert!(!is_stalled(1000, 1000));
    }

    #[test]
    fn stalled_after_expiry() {
        assert!(is_stalled(1000, 1001));
    }

    #[test]
    fn stalled_well_past_expiry() {
        assert!(is_stalled(1000, 999_999));
    }
}
