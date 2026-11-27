//! What the core's drain asks before an update (quality-sdk decision
//! 0002, `drain`): the runs running now, and the work due to start within
//! a horizon: queued runs and the fires of enabled schedules of flows that
//! are not paused or disabled. Counts only.

use chrono::{DateTime, Duration, Utc};
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// The orchestrator's work, in counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DrainCounts {
    pub running: u32,
    pub scheduled_soon: u32,
}

impl PgStore {
    /// The runs running at `now` and the work due by `now + horizon`.
    pub async fn drain_counts(
        &self,
        now: DateTime<Utc>,
        horizon: Duration,
    ) -> Result<DrainCounts, StoreError> {
        let r = sqlx::query(
            "SELECT (SELECT count(*) FROM runs WHERE state = 'running') AS running,
                    (SELECT count(*) FROM runs WHERE state = 'queued')
                  + (SELECT count(*) FROM schedules s JOIN flows f ON f.id = s.flow_id
                      WHERE s.enabled AND s.next_fire_at <= $1
                        AND f.enabled AND f.paused_at IS NULL) AS soon",
        )
        .bind(now + horizon)
        .fetch_one(&self.pool)
        .await?;
        let count = |name: &str| -> Result<u32, StoreError> {
            u32::try_from(r.try_get::<i64, _>(name)?).map_err(|_| corrupt(name))
        };
        Ok(DrainCounts {
            running: count("running")?,
            scheduled_soon: count("soon")?,
        })
    }
}
