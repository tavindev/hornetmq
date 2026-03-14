#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEvent {
    Added,
    Active,
    Completed,
    Failed,
    Retrying,
    Stalled,
}

impl JobEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobEvent::Added => "added",
            JobEvent::Active => "active",
            JobEvent::Completed => "completed",
            JobEvent::Failed => "failed",
            JobEvent::Retrying => "retrying",
            JobEvent::Stalled => "stalled",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_as_str() {
        assert_eq!(JobEvent::Added.as_str(), "added");
        assert_eq!(JobEvent::Active.as_str(), "active");
        assert_eq!(JobEvent::Completed.as_str(), "completed");
        assert_eq!(JobEvent::Failed.as_str(), "failed");
        assert_eq!(JobEvent::Retrying.as_str(), "retrying");
        assert_eq!(JobEvent::Stalled.as_str(), "stalled");
    }

    #[test]
    fn event_clone_and_eq() {
        let event = JobEvent::Completed;
        let cloned = event.clone();
        assert_eq!(event, cloned);
    }
}
