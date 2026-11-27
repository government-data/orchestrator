//! The outbox (decision 0011, step 6): events written with what they tell
//! (a run's end, by the trigger of `0011_event_outbox.sql`), read in order
//! by the server that sends them to the core, then marked sent or refused.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;

use crate::{PgStore, StoreError};

/// An event waiting to be sent.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxEvent {
    pub seq: i64,
    pub id: String,
    pub topic: String,
    pub version: u32,
    pub occurred_at: DateTime<Utc>,
    pub data: Value,
}

impl PgStore {
    /// The oldest `limit` events neither sent nor refused, in order.
    pub async fn pending_events(&self, limit: u32) -> Result<Vec<OutboxEvent>, StoreError> {
        let rows = sqlx::query(
            "SELECT seq, id::text AS id, topic, version, occurred_at, data FROM event_outbox
              WHERE sent_at IS NULL AND refused IS NULL ORDER BY seq LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let version: i32 = r.try_get("version")?;
                Ok(OutboxEvent {
                    seq: r.try_get("seq")?,
                    id: r.try_get("id")?,
                    topic: r.try_get("topic")?,
                    version: u32::try_from(version).map_err(|e| crate::corrupt(&e.to_string()))?,
                    occurred_at: r.try_get("occurred_at")?,
                    data: r.try_get("data")?,
                })
            })
            .collect()
    }

    /// The core stored event `seq`.
    pub async fn event_sent(&self, seq: i64, now: DateTime<Utc>) -> Result<(), StoreError> {
        sqlx::query("UPDATE event_outbox SET sent_at = $2 WHERE seq = $1 AND sent_at IS NULL")
            .bind(seq)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// The core refused event `seq` with `status`: kept, never sent again.
    pub async fn event_refused(&self, seq: i64, status: u16) -> Result<(), StoreError> {
        sqlx::query("UPDATE event_outbox SET refused = $2 WHERE seq = $1 AND sent_at IS NULL")
            .bind(seq)
            .bind(i32::from(status))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Deletes the events sent before `before`; the refused stay.
    pub async fn forget_sent_events(&self, before: DateTime<Utc>) -> Result<u64, StoreError> {
        let done = sqlx::query("DELETE FROM event_outbox WHERE sent_at < $1")
            .bind(before)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected())
    }
}
