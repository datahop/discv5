use super::params;
use std::time::Duration;

/// Configuration of the topic discovery subsystem. Defaults are the spec's parameters.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// How long an admitted advertisement stays in the ad cache. Default: `E`.
    pub ad_lifetime: Duration,
    /// Capacity of the ad cache across all topics. Default: `C`.
    pub ad_cache_size: usize,
    /// Maximum number of active registrars per registration bucket. Default: `K_REGISTER`.
    pub reg_bucket_size: usize,
    /// Maximum number of standby registrars per registration bucket. Default:
    /// `REG_STANDBY_LIMIT`.
    pub reg_bucket_standby_limit: usize,
    /// Maximum time spent on one registration attempt. Default: `None`, 1.5 times
    /// `ad_lifetime` (`REG_ATTEMPT_BUDGET` with the default lifetime). Zero is the
    /// default too, as in geth.
    pub reg_attempt_timeout: Option<Duration>,
    /// Number of nodes per search bucket. Default: `SEARCH_BUCKET_SIZE`.
    pub search_bucket_size: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        TopicConfig {
            ad_lifetime: params::E,
            ad_cache_size: params::C,
            reg_bucket_size: params::K_REGISTER,
            reg_bucket_standby_limit: params::REG_STANDBY_LIMIT,
            reg_attempt_timeout: None,
            search_bucket_size: params::SEARCH_BUCKET_SIZE,
        }
    }
}

impl TopicConfig {
    /// `reg_attempt_timeout`, or 1.5 times `ad_lifetime` when unset or zero.
    pub fn attempt_timeout(&self) -> Duration {
        self.reg_attempt_timeout
            .filter(|t| !t.is_zero())
            .unwrap_or(self.ad_lifetime + self.ad_lifetime / 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The attempt timeout follows the ad lifetime unless set to a non-zero value, as
    // geth's `RegAttemptTimeout` default.
    #[test]
    fn attempt_timeout_follows_lifetime() {
        let mut c = TopicConfig::default();
        assert_eq!(c.attempt_timeout(), params::REG_ATTEMPT_BUDGET);
        c.ad_lifetime = Duration::from_secs(60);
        assert_eq!(c.attempt_timeout(), Duration::from_secs(90));
        c.reg_attempt_timeout = Some(Duration::ZERO);
        assert_eq!(c.attempt_timeout(), Duration::from_secs(90));
        c.reg_attempt_timeout = Some(Duration::from_secs(5));
        assert_eq!(c.attempt_timeout(), Duration::from_secs(5));
    }
}
