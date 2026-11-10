use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

lazy_static! {
    pub(crate) static ref METRICS: InternalMetrics = InternalMetrics::default();
}

/// A collection of metrics used throughout the server.
pub(crate) struct InternalMetrics {
    /// The number of active UDP sessions that are currently established.
    pub active_sessions: AtomicUsize,
    /// The number of seconds to store received packets to taking a moving average over.
    pub moving_window: u64,
    /// The number of unsolicited requests received per moving window.
    pub unsolicited_requests_per_window: AtomicUsize,
    /// The number of bytes sent.
    pub bytes_sent: AtomicUsize,
    /// The number of bytes received.
    pub bytes_recv: AtomicUsize,
    /// Whether we consider ourselves contactable or not on ipv4.
    pub ipv4_contactable: AtomicBool,
    /// Whether we consider ourselves contactable or not on ipv6.
    pub ipv6_contactable: AtomicBool,
    /// Topic discovery message counters.
    #[cfg(feature = "topic-discovery")]
    pub topic: TopicCounters,
}

/// Counts of topic discovery messages, cumulative since start.
#[cfg(feature = "topic-discovery")]
#[derive(Default)]
pub(crate) struct TopicCounters {
    pub regtopic_sent: AtomicUsize,
    pub regtopic_recv: AtomicUsize,
    pub topicquery_sent: AtomicUsize,
    pub topicquery_recv: AtomicUsize,
    pub regconfirmation_recv: AtomicUsize,
    pub topicnodes_recv: AtomicUsize,
    pub regtopic_rejected: AtomicUsize,
    pub ticket_issued: AtomicUsize,
    pub ticket_early: AtomicUsize,
    pub ticket_late: AtomicUsize,
    pub ticket_expired: AtomicUsize,
    pub ticket_replayed: AtomicUsize,
    pub ticket_invalid: AtomicUsize,
    pub ticket_foreign: AtomicUsize,
}

#[cfg(feature = "topic-discovery")]
impl TopicCounters {
    pub(crate) fn snapshot(&self) -> TopicMetrics {
        TopicMetrics {
            regtopic_sent: self.regtopic_sent.load(Ordering::Relaxed),
            regtopic_recv: self.regtopic_recv.load(Ordering::Relaxed),
            topicquery_sent: self.topicquery_sent.load(Ordering::Relaxed),
            topicquery_recv: self.topicquery_recv.load(Ordering::Relaxed),
            regconfirmation_recv: self.regconfirmation_recv.load(Ordering::Relaxed),
            topicnodes_recv: self.topicnodes_recv.load(Ordering::Relaxed),
            regtopic_rejected: self.regtopic_rejected.load(Ordering::Relaxed),
            ticket_issued: self.ticket_issued.load(Ordering::Relaxed),
            ticket_early: self.ticket_early.load(Ordering::Relaxed),
            ticket_late: self.ticket_late.load(Ordering::Relaxed),
            ticket_expired: self.ticket_expired.load(Ordering::Relaxed),
            ticket_replayed: self.ticket_replayed.load(Ordering::Relaxed),
            ticket_invalid: self.ticket_invalid.load(Ordering::Relaxed),
            ticket_foreign: self.ticket_foreign.load(Ordering::Relaxed),
        }
    }
}

/// Counts of topic discovery messages, cumulative since start.
#[cfg(feature = "topic-discovery")]
#[derive(Clone, Debug, Default)]
pub struct TopicMetrics {
    pub regtopic_sent: usize,
    pub regtopic_recv: usize,
    pub topicquery_sent: usize,
    pub topicquery_recv: usize,
    pub regconfirmation_recv: usize,
    pub topicnodes_recv: usize,
    /// REGTOPIC requests rejected for their record.
    pub regtopic_rejected: usize,
    /// Tickets issued in REGCONFIRMATION.
    pub ticket_issued: usize,
    /// Tickets presented before their window and echoed.
    pub ticket_early: usize,
    /// Tickets credited late, within the grace.
    pub ticket_late: usize,
    /// Tickets presented past the grace.
    pub ticket_expired: usize,
    /// Tickets presented again after being credited.
    pub ticket_replayed: usize,
    /// Tickets that failed authentication or were for another topic.
    pub ticket_invalid: usize,
    /// Tickets presented by a node they were not issued to.
    pub ticket_foreign: usize,
}

impl Default for InternalMetrics {
    fn default() -> Self {
        InternalMetrics {
            moving_window: 5,
            active_sessions: AtomicUsize::new(0),
            unsolicited_requests_per_window: AtomicUsize::new(0),
            bytes_sent: AtomicUsize::new(0),
            bytes_recv: AtomicUsize::new(0),
            ipv4_contactable: AtomicBool::new(false),
            ipv6_contactable: AtomicBool::new(false),
            #[cfg(feature = "topic-discovery")]
            topic: TopicCounters::default(),
        }
    }
}

impl InternalMetrics {
    pub(crate) fn add_recv_bytes(&self, bytes: usize) {
        let current_bytes_recv = self.bytes_recv.load(Ordering::Relaxed);
        self.bytes_recv
            .store(current_bytes_recv.saturating_add(bytes), Ordering::Relaxed);
    }

    pub(crate) fn add_sent_bytes(&self, bytes: usize) {
        let current_bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
        self.bytes_sent
            .store(current_bytes_sent.saturating_add(bytes), Ordering::Relaxed);
    }
}

#[derive(Clone, Debug)]
/// The publicly accessible metrics that can be obtained from the Discv5 server.
pub struct Metrics {
    /// The number of active UDP sessions that are currently established.
    pub active_sessions: usize,
    /// The number of unsolicited requests received per second (averaged over a moving window).
    pub unsolicited_requests_per_second: f64,
    /// The number of bytes sent.
    pub bytes_sent: usize,
    /// The number of bytes received.
    pub bytes_recv: usize,
    /// Whether we consider ourselves contactable or not.
    pub ipv4_contactable: bool,
    /// Whether we consider ourselves contactable or not.
    pub ipv6_contactable: bool,
    /// Topic discovery message counters.
    #[cfg(feature = "topic-discovery")]
    pub topic: TopicMetrics,
}

impl From<&METRICS> for Metrics {
    fn from(internal_metrics: &METRICS) -> Self {
        Metrics {
            active_sessions: internal_metrics.active_sessions.load(Ordering::Relaxed),
            unsolicited_requests_per_second: internal_metrics
                .unsolicited_requests_per_window
                .load(Ordering::Relaxed) as f64
                / internal_metrics.moving_window as f64,
            bytes_sent: internal_metrics.bytes_sent.load(Ordering::Relaxed),
            bytes_recv: internal_metrics.bytes_recv.load(Ordering::Relaxed),
            ipv4_contactable: internal_metrics.ipv4_contactable.load(Ordering::Relaxed),
            ipv6_contactable: internal_metrics.ipv6_contactable.load(Ordering::Relaxed),
            #[cfg(feature = "topic-discovery")]
            topic: internal_metrics.topic.snapshot(),
        }
    }
}
