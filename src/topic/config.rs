use super::params;
use std::time::Duration;

/// Configuration of the topic discovery subsystem. Defaults are the spec's parameters.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// Runs topic discovery: the `td` ENR entry, the registrar and the topic API.
    /// Default: false.
    pub enabled: bool,
    /// How long an admitted advertisement stays in the ad cache. Default: `E`.
    pub ad_lifetime: Duration,
    /// Capacity of the ad cache across all topics. Default: `C`.
    pub ad_cache_size: usize,
    /// Maximum number of active registrars per registration bucket. Default: `K_REGISTER`.
    pub reg_bucket_size: usize,
    /// Maximum number of standby registrars per registration bucket. Default:
    /// `REG_STANDBY_LIMIT`.
    pub reg_bucket_standby_limit: usize,
    /// Maximum time spent on one registration attempt. Default: `REG_ATTEMPT_BUDGET`.
    pub reg_attempt_timeout: Duration,
    /// Number of nodes per search bucket. Default: `SEARCH_BUCKET_SIZE`.
    pub search_bucket_size: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        TopicConfig {
            enabled: false,
            ad_lifetime: params::E,
            ad_cache_size: params::C,
            reg_bucket_size: params::K_REGISTER,
            reg_bucket_standby_limit: params::REG_STANDBY_LIMIT,
            reg_attempt_timeout: params::REG_ATTEMPT_BUDGET,
            search_bucket_size: params::SEARCH_BUCKET_SIZE,
        }
    }
}
