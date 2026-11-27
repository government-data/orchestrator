//! What the orchestrator does to runs by itself: the reaper marks `lost`
//! a run whose lease expired, interrupted runs are repeated by their rule,
//! and a run is cancelled when asked.

use chrono::{DateTime, Duration, Utc};
use orchestration_model::{Interrupted, Interruption, OnInterrupt, Retry, RETRY_MAX_AGE_HOURS};
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// What a cancel did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancelled {
    /// It was queued: it is cancelled now.
    Now,
    /// It runs: its runner is told at its next renewal.
    Requested,
    /// It had ended, or does not exist.
    Nothing,
}

impl PgStore {
    /// Marks `lost` every running run whose lease ended before `now`, and
    /// returns their ids.
    pub async fn reap(&self, now: DateTime<Utc>) -> Result<Vec<String>, StoreError> {
        let ids = sqlx::query_scalar(
            "UPDATE runs SET state = 'lost', finished_at = $1, interruption = 'runner_lost',
                    lease = NULL, lease_until = NULL
              WHERE state = 'running' AND lease_until < $1
              RETURNING id::text",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(ids)
    }

    /// The interrupted runs that are not repeated yet and young enough to
    /// be, with what the retry rule reads; `runner_fences` is whether the
    /// runners stop a run they can no longer renew.
    pub async fn interrupted(
        &self,
        now: DateTime<Utc>,
        runner_fences: bool,
    ) -> Result<Vec<(String, Interrupted)>, StoreError> {
        let rows = sqlx::query(
            "SELECT r.id::text, r.interruption, r.attempt, r.finished_at, f.enabled,
                    COALESCE(f.on_interrupt, (SELECT s.on_interrupt FROM settings s)) AS rule
               FROM runs r JOIN flows f ON f.id = r.flow_id
              WHERE r.state IN ('failed', 'lost') AND r.interruption IS NOT NULL
                AND r.finished_at > $1
                AND NOT EXISTS (SELECT 1 FROM runs x WHERE x.retry_of = r.id)
              ORDER BY r.finished_at LIMIT 500",
        )
        .bind(now - Duration::hours(RETRY_MAX_AGE_HOURS))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let interruption = Interruption::parse(r.try_get("interruption")?)
                    .ok_or_else(|| corrupt("interruption"))?;
                let on_interrupt =
                    OnInterrupt::parse(r.try_get("rule")?).ok_or_else(|| corrupt("rule"))?;
                let attempt = u32::try_from(r.try_get::<i32, _>("attempt")?)
                    .map_err(|_| corrupt("attempt"))?;
                Ok((
                    r.try_get("id")?,
                    Interrupted {
                        interruption,
                        attempt,
                        finished_at: r.try_get("finished_at")?,
                        on_interrupt,
                        flow_enabled: r.try_get("enabled")?,
                        runner_fences,
                    },
                ))
            })
            .collect()
    }

    /// Queues the repetition of `run`; `None` if it was repeated already.
    /// It copies the run (its engine, line, parameters, fire and chain
    /// step), not leased, on any cluster the flow allows.
    pub async fn queue_retry(
        &self,
        run: &str,
        retry: &Retry,
        now: DateTime<Utc>,
    ) -> Result<Option<String>, StoreError> {
        let attempt = i32::try_from(retry.attempt).map_err(|_| corrupt("attempt"))?;
        let id = sqlx::query_scalar(
            "INSERT INTO runs (flow_id, engine, line, kind, cluster, trigger, triggered_by,
                               schedule_id, fire_at, parameters, timeout_secs, lock_name,
                               slot_pool, attempt, retry_of, resume, not_before, queued_at,
                               chain_id, chain_step)
             SELECT r.flow_id, r.engine, r.line, r.kind, f.cluster, r.trigger, r.triggered_by,
                    r.schedule_id, r.fire_at, r.parameters, r.timeout_secs, r.lock_name,
                    r.slot_pool, $2, r.id, $3, $4, $5, r.chain_id, r.chain_step
               FROM runs r JOIN flows f ON f.id = r.flow_id
              WHERE r.id = $1::uuid
             ON CONFLICT (retry_of) WHERE retry_of IS NOT NULL DO NOTHING
             RETURNING id::text",
        )
        .bind(run)
        .bind(attempt)
        .bind(retry.resume)
        .bind(retry.not_before)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        Ok(id)
    }

    /// Cancels a run: a queued one at once, a running one at its runner's
    /// next renewal.
    pub async fn cancel(&self, run: &str, now: DateTime<Utc>) -> Result<Cancelled, StoreError> {
        let row = sqlx::query(
            "UPDATE runs
                SET state = CASE WHEN state = 'queued' THEN 'cancelled' ELSE state END,
                    finished_at = CASE WHEN state = 'queued' THEN $2 ELSE finished_at END,
                    cancel_requested = true
              WHERE id = $1::uuid AND state IN ('queued', 'running')
              RETURNING state",
        )
        .bind(run)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            None => Cancelled::Nothing,
            Some(r) if r.try_get::<String, _>("state")? == "cancelled" => Cancelled::Now,
            Some(_) => Cancelled::Requested,
        })
    }

    /// Cancels the queued runs waiting since before `queued_before` (the
    /// installation's wait), and returns their ids.
    pub async fn cancel_stale(
        &self,
        queued_before: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Vec<String>, StoreError> {
        let ids = sqlx::query_scalar(
            "UPDATE runs SET state = 'cancelled', finished_at = $2, error_class = 'not_claimed'
              WHERE state = 'queued' AND not_before < $1
              RETURNING id::text",
        )
        .bind(queued_before)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(ids)
    }
}
