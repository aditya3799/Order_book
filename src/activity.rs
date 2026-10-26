use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::Instant;

type ActivityKey = (usize, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityAlert {
    pub replica: usize,
    pub symbol: String,
    pub observed: u64,
    pub expected: u64,
}

#[derive(Debug)]
struct ActivityState {
    window_started: Instant,
    events: u64,
    baseline: Option<f64>,
    consecutive_low: u32,
}

#[derive(Debug)]
pub struct ActivityMonitor {
    window: Duration,
    ratio: f64,
    minimum_expected: u64,
    required_low_windows: u32,
    states: HashMap<ActivityKey, ActivityState>,
}

impl ActivityMonitor {
    pub fn new(
        window: Duration,
        ratio: f64,
        minimum_expected: u64,
        required_low_windows: u32,
    ) -> Self {
        Self {
            window,
            ratio,
            minimum_expected,
            required_low_windows,
            states: HashMap::new(),
        }
    }

    pub fn reconcile(&mut self, symbols: &HashSet<String>, redundancy: usize, now: Instant) {
        self.states
            .retain(|(_, symbol), _| symbols.contains(symbol));
        for symbol in symbols {
            for replica in 0..redundancy {
                self.states
                    .entry((replica, symbol.clone()))
                    .or_insert_with(|| ActivityState::new(now));
            }
        }
    }

    pub fn record(&mut self, replica: usize, symbol: &str, now: Instant) {
        self.states
            .entry((replica, symbol.to_owned()))
            .or_insert_with(|| ActivityState::new(now))
            .events += 1;
    }

    pub fn evaluate(&mut self, now: Instant) -> Vec<ActivityAlert> {
        let samples: HashMap<_, _> = self
            .states
            .iter_mut()
            .filter_map(|(key, state)| {
                state
                    .take_sample(now, self.window)
                    .map(|sample| (key.clone(), sample))
            })
            .collect();
        let mut symbol_maximum = HashMap::<String, u64>::new();
        for ((_, symbol), sample) in &samples {
            symbol_maximum
                .entry(symbol.clone())
                .and_modify(|maximum| *maximum = (*maximum).max(sample.observed))
                .or_insert(sample.observed);
        }

        let mut alerts = Vec::new();
        for ((replica, symbol), sample) in samples {
            let peer_maximum = symbol_maximum.get(&symbol).copied().unwrap_or_default();
            let expected = sample
                .baseline
                .map(|baseline| baseline.round() as u64)
                .unwrap_or_default()
                .max(peer_maximum);
            let low = expected >= self.minimum_expected
                && (sample.observed as f64) < expected as f64 * self.ratio;
            let state = self.states.get_mut(&(replica, symbol.clone())).unwrap();
            if low {
                state.consecutive_low += 1;
            } else {
                state.consecutive_low = 0;
                state.baseline = Some(match sample.baseline {
                    Some(baseline) => baseline * 0.8 + sample.observed as f64 * 0.2,
                    None => sample.observed as f64,
                });
            }
            if state.consecutive_low >= self.required_low_windows {
                alerts.push(ActivityAlert {
                    replica,
                    symbol,
                    observed: sample.observed,
                    expected,
                });
                *state = ActivityState::new(now);
            }
        }
        alerts
    }
}

impl ActivityState {
    fn new(now: Instant) -> Self {
        Self {
            window_started: now,
            events: 0,
            baseline: None,
            consecutive_low: 0,
        }
    }

    fn take_sample(&mut self, now: Instant, window: Duration) -> Option<ActivitySample> {
        if now.duration_since(self.window_started) < window {
            return None;
        }
        let sample = ActivitySample {
            observed: self.events,
            baseline: self.baseline,
        };
        self.window_started = now;
        self.events = 0;
        Some(sample)
    }
}

#[derive(Debug, Clone, Copy)]
struct ActivitySample {
    observed: u64,
    baseline: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_a_replica_after_sustained_rate_drop() {
        let start = Instant::now();
        let mut monitor = ActivityMonitor::new(Duration::from_secs(60), 0.25, 10, 2);
        monitor.reconcile(&HashSet::from(["asset".to_owned()]), 2, start);

        record_many(&mut monitor, 0, "asset", 100, start);
        record_many(&mut monitor, 1, "asset", 100, start);
        assert!(monitor.evaluate(start + Duration::from_secs(60)).is_empty());

        record_many(&mut monitor, 0, "asset", 100, start);
        record_many(&mut monitor, 1, "asset", 5, start);
        assert!(
            monitor
                .evaluate(start + Duration::from_secs(120))
                .is_empty()
        );

        record_many(&mut monitor, 0, "asset", 100, start);
        record_many(&mut monitor, 1, "asset", 4, start);
        let alerts = monitor.evaluate(start + Duration::from_secs(180));
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].replica, 1);
        assert_eq!(alerts[0].observed, 4);
        assert_eq!(alerts[0].expected, 100);
    }

    #[test]
    fn ignores_low_volume_symbols_without_a_meaningful_baseline() {
        let start = Instant::now();
        let mut monitor = ActivityMonitor::new(Duration::from_secs(60), 0.25, 10, 2);
        monitor.reconcile(&HashSet::from(["quiet".to_owned()]), 2, start);
        record_many(&mut monitor, 0, "quiet", 2, start);
        record_many(&mut monitor, 1, "quiet", 1, start);
        assert!(monitor.evaluate(start + Duration::from_secs(60)).is_empty());
        assert!(
            monitor
                .evaluate(start + Duration::from_secs(120))
                .is_empty()
        );
    }

    fn record_many(
        monitor: &mut ActivityMonitor,
        replica: usize,
        symbol: &str,
        count: usize,
        now: Instant,
    ) {
        for _ in 0..count {
            monitor.record(replica, symbol, now);
        }
    }
}
