use std::time::{Duration, Instant};

pub struct Timer {
    duration: Duration,
    start_time: Option<Instant>,
}

impl Timer {
    pub fn new() -> Self {
        Timer {
            duration: Duration::new(0, 0),
            start_time: None,
        }
    }

    /// Offsets the running time of the timer by the given duration.
    pub fn add(&mut self, duration: Duration) {
        self.duration += duration;
    }

    /// Starts the timer. If the timer is already running, this is a no-op.
    pub fn start(&mut self) {
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }
    }

    /// Pauses the timer. If the timer is not running, this is a no-op.
    pub fn pause(&mut self) {
        if let Some(start_time) = self.start_time {
            self.duration += start_time.elapsed();
            self.start_time = None;
        }
    }

    /// Returns the elapsed time in which the timer has been running.
    pub fn elapsed(&self) -> Duration {
        if let Some(start_time) = self.start_time {
            self.duration + start_time.elapsed()
        } else {
            self.duration
        }
    }
}
