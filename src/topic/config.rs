use std::time::Duration;

/// Configuration of the topic discovery subsystem. Defaults match the Go implementation.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// How long an admitted advertisement stays in the ad cache. Default: 15 minutes.
    pub ad_lifetime: Duration,
    /// Capacity of the ad cache across all topics. Default: 5000.
    pub ad_cache_size: usize,
    /// Number of distance buckets kept in a registration table. Default: 10.
    pub reg_table_depth: usize,
    /// Number of distance buckets kept in a search table. Default: 10.
    pub search_table_depth: usize,
    /// Maximum number of active registrars per registration bucket. Default: 5.
    pub reg_bucket_size: usize,
    /// Maximum number of standby registrars per registration bucket. Default: 20.
    pub reg_bucket_standby_limit: usize,
    /// Maximum time spent on one registration attempt. Default: 1.5 x `ad_lifetime`.
    pub reg_attempt_timeout: Duration,
    /// Number of nodes per search bucket. Default: 16.
    pub search_bucket_size: usize,
    /// Number of ads per TOPICNODES reply below which a search moves closer to the
    /// topic. Zero turns the adaptive search distance off. Default: 8.
    pub search_yield_floor: usize,
    /// Number of buckets on each side of the active one that an adaptive search asks
    /// to have refilled (at least 1). `None` asks for every bucket with free space,
    /// the neighbourhood first. Default: 1.
    pub search_aux_radius: Option<usize>,
    /// Cap on the gap between search passes. The gap starts at two seconds and doubles
    /// after every pass that returned nothing new. `None` keeps it at two seconds.
    /// Default: one minute.
    pub search_pass_backoff: Option<Duration>,
    /// Maximum number of advertisers returned to a TOPICQUERY. Default: 16.
    pub topic_nodes_limit: usize,
    /// Maximum number of auxiliary routing nodes attached to a REGTOPIC or TOPICQUERY
    /// reply, one per requested distance. Default: 8.
    pub aux_nodes_limit: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        let ad_lifetime = Duration::from_secs(15 * 60);
        TopicConfig {
            ad_lifetime,
            ad_cache_size: 5000,
            reg_table_depth: 10,
            search_table_depth: 10,
            reg_bucket_size: 5,
            reg_bucket_standby_limit: 20,
            reg_attempt_timeout: ad_lifetime + ad_lifetime / 2,
            search_bucket_size: 16,
            search_yield_floor: 8,
            search_aux_radius: Some(1),
            search_pass_backoff: Some(Duration::from_secs(60)),
            topic_nodes_limit: 16,
            aux_nodes_limit: 8,
        }
    }
}
