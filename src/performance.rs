use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub struct PerformanceTracker {
    start_time: Instant,
    item_count: AtomicUsize,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            item_count: AtomicUsize::new(0),
        }
    }

    pub fn create(enabled: bool) -> Option<Self> {
        if enabled { Some(Self::new()) } else { None }
    }

    pub fn track_item(&self) {
        self.item_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn print_stats(&self) {
        let duration = self.start_time.elapsed();
        let count = self.item_count.load(Ordering::Relaxed);
        let seconds = duration.as_secs_f64();
        let rate = if seconds > 0.0 {
            count as f64 / seconds
        } else {
            0.0
        };

        eprintln!(
            "Performance: {} values in {:.3} seconds ({:.0}/s)",
            count, seconds, rate
        );
    }

    pub fn report_if_needed(tracker: &Option<Self>) {
        if let Some(t) = tracker {
            t.print_stats();
        }
    }
}
