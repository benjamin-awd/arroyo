//! Utilization tracking for pipeline components.
//!
//! Tracks the ratio of working time vs. waiting time for each component
//! using EWMA smoothing for stable readings.

use metrics::gauge;
use std::time::{Duration, Instant};

const EMISSION_INTERVAL: Duration = Duration::from_secs(5);

/// EWMA calculator for smoothing utilization values.
pub struct Ewma {
    average: Option<f64>,
    alpha: f64,
}

impl Ewma {
    pub fn new(alpha: f64) -> Self {
        Self {
            average: None,
            alpha,
        }
    }

    pub fn update(&mut self, point: f64) -> f64 {
        let average = match self.average {
            None => point,
            Some(avg) => point.mul_add(self.alpha, avg * (1.0 - self.alpha)),
        };
        self.average = Some(average);
        average
    }
}

/// Timer that tracks utilization for a component.
pub struct UtilizationTimer {
    overall_start: Instant,
    span_start: Instant,
    waiting: bool,
    total_wait: Duration,
    ewma: Ewma,
    gauge: metrics::Gauge,
}

impl UtilizationTimer {
    pub fn new(stage: &'static str) -> Self {
        let now = Instant::now();
        Self {
            overall_start: now,
            span_start: now,
            waiting: true, // Start in waiting state
            total_wait: Duration::ZERO,
            ewma: Ewma::new(0.9),
            gauge: gauge!("blizzard_utilization", "stage" => stage),
        }
    }

    /// Mark transition to waiting state.
    pub fn start_wait(&mut self) {
        if !self.waiting {
            self.waiting = true;
            self.span_start = Instant::now();
        }
    }

    /// Mark transition to working state.
    pub fn stop_wait(&mut self) {
        if self.waiting {
            self.total_wait += self.span_start.elapsed();
            self.waiting = false;
            self.span_start = Instant::now();
        }
    }

    /// Update and emit utilization metric (call periodically).
    pub fn maybe_update(&mut self) {
        if self.overall_start.elapsed() < EMISSION_INTERVAL {
            return;
        }
        self.update();
    }

    fn update(&mut self) {
        let now = Instant::now();

        // Account for current span
        let mut total_wait = self.total_wait;
        if self.waiting {
            total_wait += now.duration_since(self.span_start);
        }

        let total_duration = now.duration_since(self.overall_start);
        let wait_ratio = total_wait.as_secs_f64() / total_duration.as_secs_f64();
        let utilization = (1.0 - wait_ratio).clamp(0.0, 1.0);

        // Apply EWMA and emit
        let avg = self.ewma.update(utilization);
        let rounded = (avg * 10000.0).round() / 10000.0;
        self.gauge.set(rounded);

        // Reset for next period
        self.overall_start = now;
        self.span_start = now;
        self.total_wait = Duration::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ewma_first_point() {
        let mut ewma = Ewma::new(0.9);
        assert_eq!(ewma.update(0.5), 0.5);
    }

    #[test]
    fn test_ewma_smoothing() {
        let mut ewma = Ewma::new(0.9);
        ewma.update(1.0);
        // Second point: 0.0 * 0.9 + 1.0 * 0.1 = 0.1
        let result = ewma.update(0.0);
        assert!((result - 0.1).abs() < 0.001);
    }
}
