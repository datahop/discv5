use std::time::{Duration, Instant};

/// A lower bound on a waiting-time component: the largest value recently quoted,
/// decaying 1:1 with elapsed time so that a repeated request cannot obtain a quote
/// smaller than an earlier one by more than the time that has passed. The default
/// value is an already-expired bound.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct LowerBound {
    value: Duration,
    since: Option<Instant>,
}

impl LowerBound {
    /// The still-effective floor at `now`.
    pub(super) fn remaining(&self, now: Instant) -> Duration {
        match self.since {
            None => Duration::ZERO,
            Some(since) => self
                .value
                .saturating_sub(now.saturating_duration_since(since)),
        }
    }

    /// Raises the floor to `value` when it exceeds the remaining floor and returns the
    /// value that is now charged.
    pub(super) fn bump(&mut self, value: Duration, now: Instant) -> Duration {
        let remaining = self.remaining(now);
        if value > remaining {
            self.value = value;
            self.since = Some(now);
            value
        } else {
            remaining
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decay_and_max_aggregation() {
        let t0 = Instant::now();
        let mut lb = LowerBound::default();
        assert_eq!(lb.remaining(t0), Duration::ZERO);
        assert_eq!(
            lb.bump(Duration::from_secs(10), t0),
            Duration::from_secs(10)
        );

        let t1 = t0 + Duration::from_secs(3);
        assert_eq!(lb.remaining(t1), Duration::from_secs(7));
        // A smaller bump must not lower the floor.
        assert_eq!(lb.bump(Duration::from_secs(2), t1), Duration::from_secs(7));
        // A larger bump raises it and resets the decay origin.
        assert_eq!(
            lb.bump(Duration::from_secs(20), t1),
            Duration::from_secs(20)
        );
        assert_eq!(
            lb.remaining(t1 + Duration::from_secs(5)),
            Duration::from_secs(15)
        );
        assert_eq!(lb.remaining(t1 + Duration::from_secs(30)), Duration::ZERO);
    }
}
