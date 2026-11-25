//! Where the server reads the time: the system's clock in production, a
//! hand-moved one in tests.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};

/// The current time.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

/// The system's clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock that stays where it is put.
#[derive(Debug, Clone)]
pub struct FixedClock(Arc<Mutex<DateTime<Utc>>>);

impl FixedClock {
    #[must_use]
    pub fn at(now: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }

    /// Moves it forward by `by`.
    pub fn advance(&self, by: Duration) {
        if let Ok(mut now) = self.0.lock() {
            *now += by;
        }
    }
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
            .lock()
            .map(|now| *now)
            .unwrap_or_else(|e| *e.into_inner())
    }
}
