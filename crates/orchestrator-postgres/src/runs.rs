//! Runs: queued, claimed under a lease with `FOR UPDATE SKIP LOCKED`,
//! renewed, logged and finished. Every call about a claimed run names its
//! lease, so a holder whose lease expired changes nothing.

use chrono::{DateTime, Utc};
use engine_port::{Claimed, Finish, LogLine, Progress, Resume};
use orchestration_model::{EngineLine, EngineName, Trigger};
use serde_json::Value;
use sqlx::Row;

use crate::{PgStore, StoreError};

/// A run to queue.
#[derive(Debug, Clone, PartialEq)]
pub struct NewRun {
    pub flow_id: String,
    pub engine: EngineLine,
    pub kind: String,
    pub cluster: Option<String>,
    pub trigger: Trigger,
    /// The schedule's fire it runs: a fire queues one run however often it
    /// is seen.
    pub fire_at: Option<DateTime<Utc>>,
    pub parameters: Value,
    pub timeout_secs: u32,
    /// At most one running run per lock (a table's writer).
    pub lock_name: Option<String>,
    /// At most the pool's size of running runs naming it.
    pub slot_pool: Option<String>,
    pub queued_at: DateTime<Utc>,
}

/// What a claim asks for: a run of this engine and line, for this runner on
/// this cluster. A runner claims only when it has a free slot: its
/// concurrency is its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimFor<'a> {
    pub engine: &'a EngineLine,
    pub runner: &'a str,
    pub cluster: &'a str,
    /// The new lease's token and end.
    pub lease: &'a str,
    pub lease_until: DateTime<Utc>,
    pub now: DateTime<Utc>,
}

/// What a renewal tells the runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Renewal {
    pub cancel_requested: bool,
    /// A scan was asked of the run's runner and its found report has not
    /// arrived yet.
    pub scan_requested: bool,
    /// The run's engine, whose hold the answer reads.
    pub engine: EngineName,
}

fn u32_of(raw: i32) -> Result<u32, StoreError> {
    u32::try_from(raw).map_err(|_| crate::corrupt("a negative number"))
}

impl PgStore {
    /// Queues a run; `None` if its schedule's fire was queued already.
    pub async fn enqueue(&self, run: &NewRun) -> Result<Option<String>, StoreError> {
        let (trigger, by, schedule) = match &run.trigger {
            Trigger::Manual(person) => ("manual", Some(person.as_str()), None),
            Trigger::Schedule(id) => ("schedule", None, Some(id.as_str())),
        };
        let id = sqlx::query_scalar(
            "INSERT INTO runs (flow_id, engine, line, kind, cluster, trigger, triggered_by,
                               schedule_id, fire_at, parameters, timeout_secs, lock_name,
                               slot_pool, queued_at, not_before)
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::uuid, $9, $10, $11, $12, $13, $14, $14)
             ON CONFLICT (schedule_id, fire_at)
                WHERE schedule_id IS NOT NULL AND fire_at IS NOT NULL AND retry_of IS NULL
                DO NOTHING
             RETURNING id::text",
        )
        .bind(&run.flow_id)
        .bind(run.engine.engine.as_str())
        .bind(run.engine.line.as_str())
        .bind(&run.kind)
        .bind(&run.cluster)
        .bind(trigger)
        .bind(by)
        .bind(schedule)
        .bind(run.fire_at)
        .bind(&run.parameters)
        .bind(i32::try_from(run.timeout_secs).map_err(|_| StoreError::Invalid("timeout".into()))?)
        .bind(&run.lock_name)
        .bind(&run.slot_pool)
        .bind(run.queued_at)
        .fetch_optional(&self.pool)
        .await?;
        Ok(id)
    }

    /// Hands out the oldest queued run this runner may take, under a new
    /// lease: of its engine and line, due, for any cluster or its own,
    /// with the engine neither paused nor held, the flow not running when
    /// it does not allow overlap, its lock free and its pool not full. Claims are serialised by an advisory
    /// lock, so two never pass the same lock or pool at once.
    pub async fn claim(&self, c: &ClaimFor<'_>) -> Result<Option<Claimed>, StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('orchestrator-claim'))")
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query(CLAIM)
            .bind(c.engine.engine.as_str())
            .bind(c.engine.line.as_str())
            .bind(c.runner)
            .bind(c.cluster)
            .bind(c.lease)
            .bind(c.lease_until)
            .bind(c.now)
            .fetch_optional(&mut *tx)
            .await?;
        tx.commit().await?;
        let Some(r) = row else { return Ok(None) };
        let resume = match r.try_get::<Option<String>, _>("previous_run")? {
            Some(previous_run) => Some(Resume {
                previous_run,
                engine_ref: r.try_get("previous_ref")?,
                last_detail: r.try_get("previous_detail")?,
            }),
            None => None,
        };
        Ok(Some(Claimed {
            run: r.try_get("id")?,
            flow: r.try_get("flow_id")?,
            kind: r.try_get("kind")?,
            parameters: r.try_get("parameters")?,
            attempt: u32_of(r.try_get("attempt")?)?,
            timeout_secs: u32_of(r.try_get("timeout_secs")?)?,
            lease: c.lease.to_string(),
            lease_until: c.lease_until,
            resume,
            variables: Default::default(),
        }))
    }

    /// Extends a running run's lease, with its progress and the engine's
    /// reference; refused when `lease` is not its lease any more.
    pub async fn renew(
        &self,
        run: &str,
        lease: &str,
        progress: Option<&Progress>,
        engine_ref: Option<&str>,
        lease_until: DateTime<Utc>,
    ) -> Result<Renewal, StoreError> {
        // A run's runner is its runner's instance: the runner's name, `/`
        // and the instance's own.
        let row = sqlx::query(
            "WITH r AS (
               UPDATE runs SET lease_until = $3,
                      engine_ref = COALESCE($4, engine_ref),
                      progress_fraction = COALESCE($5, progress_fraction),
                      progress_detail = COALESCE($6, progress_detail)
                WHERE id = $1::uuid AND state = 'running' AND lease = $2
                RETURNING cancel_requested, engine, line, runner)
             SELECT r.cancel_requested, r.engine, EXISTS (
                      SELECT 1 FROM runners n
                       WHERE n.engine = r.engine AND n.line = r.line
                         AND starts_with(r.runner, n.runner || '/')
                         AND n.scan_requested_at IS NOT NULL) AS scan
               FROM r",
        )
        .bind(run)
        .bind(lease)
        .bind(lease_until)
        .bind(engine_ref)
        .bind(progress.and_then(|p| p.fraction))
        .bind(progress.and_then(|p| p.detail.as_deref()))
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or(StoreError::Lease)?;
        Ok(Renewal {
            cancel_requested: row.try_get("cancel_requested")?,
            scan_requested: row.try_get("scan")?,
            engine: EngineName::new(row.try_get("engine")?).map_err(|e| crate::corrupt(&e.0))?,
        })
    }

    /// Appends log lines; a line sent twice is kept once.
    pub async fn append_logs(
        &self,
        run: &str,
        lease: &str,
        lines: &[LogLine],
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        self.holds_lease(&mut tx, run, lease).await?;
        for l in lines {
            let seq = i64::try_from(l.seq).map_err(|_| StoreError::Invalid("seq".into()))?;
            let stream =
                serde_json::to_value(l.stream).map_err(|e| StoreError::Invalid(e.to_string()))?;
            sqlx::query(
                "INSERT INTO run_logs (run_id, seq, stream, line, logged_at)
                 VALUES ($1::uuid, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
            )
            .bind(run)
            .bind(seq)
            .bind(stream.as_str())
            .bind(&l.line)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Ends a running run as its runner reports it.
    pub async fn finish(
        &self,
        run: &str,
        f: &Finish,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let counts =
            serde_json::to_value(&f.counts).map_err(|e| StoreError::Invalid(e.to_string()))?;
        let done = sqlx::query(
            "UPDATE runs SET state = $3, finished_at = $4, counts = $5, error_class = $6,
                    interruption = $7, lease = NULL, lease_until = NULL
              WHERE id = $1::uuid AND state = 'running' AND lease = $2",
        )
        .bind(run)
        .bind(&f.lease)
        .bind(f.state.state().as_str())
        .bind(now)
        .bind(counts)
        .bind(&f.error_class)
        .bind(f.interruption.map(|i| i.as_str()))
        .execute(&self.pool)
        .await?;
        if done.rows_affected() == 1 {
            Ok(())
        } else {
            Err(StoreError::Lease)
        }
    }

    async fn holds_lease(
        &self,
        tx: &mut sqlx::PgConnection,
        run: &str,
        lease: &str,
    ) -> Result<(), StoreError> {
        let held: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM runs WHERE id = $1::uuid AND state = 'running' AND lease = $2",
        )
        .bind(run)
        .bind(lease)
        .fetch_optional(&mut *tx)
        .await?;
        held.map(drop).ok_or(StoreError::Lease)
    }
}

const CLAIM: &str = "
WITH pick AS (
    SELECT r.id
      FROM runs r
      JOIN flows f ON f.id = r.flow_id
     WHERE r.state = 'queued' AND r.engine = $1 AND r.line = $2 AND r.not_before <= $7
       AND (r.cluster IS NULL OR r.cluster = $4)
       AND NOT EXISTS (SELECT 1 FROM engine_pauses p WHERE p.engine = r.engine)
       AND NOT EXISTS (SELECT 1 FROM engine_holds h WHERE h.engine = r.engine AND h.until > $7)
       AND (f.allow_overlap OR NOT EXISTS (
            SELECT 1 FROM runs o WHERE o.flow_id = r.flow_id AND o.state = 'running'))
       AND (r.lock_name IS NULL OR NOT EXISTS (
            SELECT 1 FROM runs o WHERE o.lock_name = r.lock_name AND o.state = 'running'))
       AND (r.slot_pool IS NULL OR (
            SELECT count(*) FROM runs o WHERE o.slot_pool = r.slot_pool AND o.state = 'running')
            < COALESCE((SELECT s.size FROM slot_pools s WHERE s.name = r.slot_pool), 1))
     ORDER BY r.queued_at, r.id
     LIMIT 1
       FOR UPDATE OF r SKIP LOCKED
)
UPDATE runs r
   SET state = 'running', started_at = $7, runner = $3, lease = $5, lease_until = $6
  FROM pick
 WHERE r.id = pick.id
RETURNING r.id::text, r.flow_id::text, r.kind, r.parameters, r.attempt, r.timeout_secs,
          CASE WHEN r.resume THEN r.retry_of::text END AS previous_run,
          (SELECT p.engine_ref FROM runs p WHERE r.resume AND p.id = r.retry_of) AS previous_ref,
          (SELECT p.progress_detail FROM runs p WHERE r.resume AND p.id = r.retry_of)
              AS previous_detail";
