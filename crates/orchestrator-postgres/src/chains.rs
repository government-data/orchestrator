//! Chains: one fire's ordered steps on one flow (core decision 0090, point
//! 2). Each pass moves every open chain by [`next_move`]: a step's run is
//! queued once, the next only after it succeeded; a setup step whose
//! fingerprint already succeeded is skipped. A chain is moved under its row
//! lock, so two servers never move one twice.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use orchestration_model::{
    next_move, validate_steps, ChainMove, ChainStep, RunState, Trigger, RETRY_MAX_AGE_HOURS,
};
use sqlx::types::Json;
use sqlx::{PgConnection, Row};

use crate::{corrupt, PgStore, StoreError};

/// A chain to start.
#[derive(Debug, Clone, PartialEq)]
pub struct NewChain {
    pub flow_id: String,
    /// What fired it: a second chain of the same origin is not made.
    pub origin: String,
    pub trigger: Trigger,
    pub steps: Vec<ChainStep>,
    pub now: DateTime<Utc>,
}

/// What one pass over the chains did, in counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainsMoved {
    pub queued: u32,
    pub succeeded: u32,
    pub failed: u32,
}

impl PgStore {
    /// Starts a chain; `None` if one of that origin exists. Its first run
    /// is queued by the next [`PgStore::move_chains`].
    pub async fn start_chain(&self, c: &NewChain) -> Result<Option<String>, StoreError> {
        validate_steps(&c.steps).map_err(StoreError::Invalid)?;
        let (trigger, by, schedule) = match &c.trigger {
            Trigger::Manual(person) => ("manual", Some(person.as_str()), None),
            Trigger::Schedule(id) => ("schedule", None, Some(id.as_str())),
        };
        let id = sqlx::query_scalar(
            "INSERT INTO chains (flow_id, origin, trigger, triggered_by, schedule_id, steps,
                                 created_at)
             VALUES ($1::uuid, $2, $3, $4, $5::uuid, $6, $7)
             ON CONFLICT (origin) DO NOTHING RETURNING id::text",
        )
        .bind(&c.flow_id)
        .bind(&c.origin)
        .bind(trigger)
        .bind(by)
        .bind(schedule)
        .bind(Json(&c.steps))
        .bind(c.now)
        .fetch_optional(&self.pool)
        .await?;
        Ok(id)
    }

    /// Moves every open chain one decision on: running ones, and failed
    /// ones young enough that a repetition of their step may still end
    /// well.
    pub async fn move_chains(&self, now: DateTime<Utc>) -> Result<ChainsMoved, StoreError> {
        let open: Vec<String> = sqlx::query_scalar(
            "SELECT id::text FROM chains
              WHERE state = 'running' OR (state = 'failed' AND finished_at > $1)
              ORDER BY created_at LIMIT 500",
        )
        .bind(now - Duration::hours(RETRY_MAX_AGE_HOURS))
        .fetch_all(&self.pool)
        .await?;
        let mut moved = ChainsMoved::default();
        for id in open {
            let mut tx = self.pool.begin().await?;
            let took = move_one(&mut tx, &id, now).await?;
            tx.commit().await?;
            match took {
                Some(ChainMove::Queue(_)) => moved.queued += 1,
                Some(ChainMove::Succeeded) => moved.succeeded += 1,
                Some(ChainMove::Failed) => moved.failed += 1,
                Some(ChainMove::Wait) | None => {}
            }
        }
        Ok(moved)
    }

    /// The id of the chain of `origin`, if one was started.
    pub async fn chain_of_origin(&self, origin: &str) -> Result<Option<String>, StoreError> {
        let id: Option<String> =
            sqlx::query_scalar("SELECT id::text FROM chains WHERE origin = $1")
                .bind(origin)
                .fetch_optional(&self.pool)
                .await?;
        Ok(id)
    }

    /// A chain's state and the step it is at.
    pub async fn chain_state(&self, id: &str) -> Result<Option<(String, u32)>, StoreError> {
        let row = sqlx::query("SELECT state, step FROM chains WHERE id = $1::uuid")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| {
            let step = u32::try_from(r.try_get::<i32, _>("step")?).map_err(|_| corrupt("step"))?;
            Ok((r.try_get("state")?, step))
        })
        .transpose()
    }
}

/// One chain's move, under its row lock; `None` when another server holds
/// it, or the move changes nothing stored.
async fn move_one(
    tx: &mut PgConnection,
    id: &str,
    now: DateTime<Utc>,
) -> Result<Option<ChainMove>, StoreError> {
    let Some(row) = sqlx::query(
        "SELECT steps, step, state FROM chains WHERE id = $1::uuid FOR UPDATE SKIP LOCKED",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(None);
    };
    let Json(steps): Json<Vec<ChainStep>> = row.try_get("steps")?;
    let at = usize::try_from(row.try_get::<i32, _>("step")?).map_err(|_| corrupt("step"))?;
    let state: String = row.try_get("state")?;
    let newest = newest_run(tx, id, at).await?;
    if newest == Some(RunState::Succeeded) {
        if let Some(setup) = steps.get(at).and_then(|s| s.setup.as_ref()) {
            sqlx::query(
                "INSERT INTO setups (key, fingerprint, done_at) VALUES ($1, $2, $3)
                 ON CONFLICT (key) DO UPDATE SET fingerprint = $2, done_at = $3",
            )
            .bind(&setup.key)
            .bind(&setup.fingerprint)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
    }
    let done = setups_of(tx, &steps).await?;
    let (at, step_move) = next_move(&steps, at, newest, |s| {
        done.get(&s.key) == Some(&s.fingerprint)
    });
    let (new_state, finished) = match step_move {
        // A repetition of a failed chain's step runs: the chain is open again.
        ChainMove::Wait if state == "failed" => ("running", None),
        ChainMove::Wait => return Ok(Some(ChainMove::Wait)),
        ChainMove::Queue(step) => {
            queue_step(tx, id, step, &steps[step], now).await?;
            ("running", None)
        }
        ChainMove::Succeeded => ("succeeded", Some(now)),
        ChainMove::Failed if state == "failed" => return Ok(None),
        ChainMove::Failed => ("failed", Some(now)),
    };
    let at = i32::try_from(at).map_err(|_| corrupt("step"))?;
    sqlx::query("UPDATE chains SET step = $2, state = $3, finished_at = $4 WHERE id = $1::uuid")
        .bind(id)
        .bind(at)
        .bind(new_state)
        .bind(finished)
        .execute(&mut *tx)
        .await?;
    Ok(Some(step_move))
}

/// The state of the newest run of the chain's step, its repetitions
/// included.
async fn newest_run(
    tx: &mut PgConnection,
    id: &str,
    step: usize,
) -> Result<Option<RunState>, StoreError> {
    let step = i32::try_from(step).map_err(|_| corrupt("step"))?;
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM runs WHERE chain_id = $1::uuid AND chain_step = $2
          ORDER BY attempt DESC, queued_at DESC LIMIT 1",
    )
    .bind(id)
    .bind(step)
    .fetch_optional(&mut *tx)
    .await?;
    state
        .map(|s| RunState::parse(&s).map_err(|e| corrupt(&e.0)))
        .transpose()
}

/// The fingerprints last done for the chain's setup keys.
async fn setups_of(
    tx: &mut PgConnection,
    steps: &[ChainStep],
) -> Result<HashMap<String, String>, StoreError> {
    let keys: Vec<&str> = steps
        .iter()
        .filter_map(|s| s.setup.as_ref().map(|s| s.key.as_str()))
        .collect();
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT key, fingerprint FROM setups WHERE key = ANY($1)")
            .bind(&keys)
            .fetch_all(&mut *tx)
            .await?;
    Ok(rows.into_iter().collect())
}

/// Queues the step's first run on the chain's flow: its engine, line,
/// cluster and timeout, the step's kind, and the step's parameters over the
/// flow's. A step queued already is not queued again.
async fn queue_step(
    tx: &mut PgConnection,
    id: &str,
    step: usize,
    s: &ChainStep,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let step = i32::try_from(step).map_err(|_| corrupt("step"))?;
    let over = if s.parameters.is_null() {
        serde_json::json!({})
    } else {
        s.parameters.clone()
    };
    sqlx::query(
        "INSERT INTO runs (flow_id, engine, line, kind, cluster, trigger, triggered_by,
                           schedule_id, parameters, timeout_secs, chain_id, chain_step,
                           queued_at, not_before)
         SELECT f.id, f.engine, f.line, $3, f.cluster, c.trigger, c.triggered_by,
                c.schedule_id, f.parameters || $4, f.timeout_secs, c.id, $2, $5, $5
           FROM chains c JOIN flows f ON f.id = c.flow_id
          WHERE c.id = $1::uuid
         ON CONFLICT (chain_id, chain_step) WHERE chain_id IS NOT NULL AND retry_of IS NULL
            DO NOTHING",
    )
    .bind(id)
    .bind(step)
    .bind(&s.kind)
    .bind(over)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    Ok(())
}
