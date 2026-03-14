pub enum QueueKeys {
    Wait,
    Active,
    Prioritized,
    Events,
    Stalled,
    Limiter,
    Delayed,
    Paused,
    Meta,
    Pc,
    Marker,
    Metrics,
    Custom(String),
}

impl QueueKeys {
    pub fn as_str(&self) -> String {
        match self {
            QueueKeys::Wait => "wait",
            QueueKeys::Active => "active",
            QueueKeys::Prioritized => "prioritized",
            QueueKeys::Events => "events",
            QueueKeys::Stalled => "stalled",
            QueueKeys::Limiter => "limiter",
            QueueKeys::Delayed => "delayed",
            QueueKeys::Paused => "paused",
            QueueKeys::Meta => "meta",
            QueueKeys::Pc => "pc",
            QueueKeys::Marker => "marker",
            QueueKeys::Metrics => "metrics",
            QueueKeys::Custom(s) => s,
        }
        .into()
    }

    pub fn with_prefix(&self, prefix: &str) -> String {
        format!("{}{}", prefix, self.as_str())
    }
}

impl From<QueueKeys> for String {
    fn from(val: QueueKeys) -> Self {
        val.to_string()
    }
}

impl std::fmt::Display for QueueKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
