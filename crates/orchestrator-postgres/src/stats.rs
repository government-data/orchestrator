//! What the metrics read: the queue per engine and line, the engines held,
//! and the engine, kind and duration of runs that ended. Counts and names
//! of engines only, never a flow or an id.

use chrono::{DateTime, Utc};
use orchestration_model::{EngineLine, EngineName, Line, RunState};
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// The ready runs of an engine and line.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueStat {
    pub engine: EngineLine,
    pub ready: i64,
    /// How long the oldest ready run has waited.
    pub oldest_secs: f64,
}

/// A run that ended, as the metrics count it.
#[derive(Debug, Clone, PartialEq)]
pub struct EndedRun {
    pub engine: EngineLine,
    pub kind: String,
    pub state: RunState,
    /// From its start to its end; 0 when it never started.
    pub seconds: f64,
}

fn engine_line(engine: &str, line: &str) -> Result<EngineLine, StoreError> {
    Ok(EngineLine {
        engine: EngineName::new(engine).map_err(|e| corrupt(&e.0))?,
        line: Line::new(line).map_err(|e| corrupt(&e.0))?,
    })
}

impl PgStore {
    /// The runs ready at `now` (queued and due) per engine and line.
    pub async fn queue_stats(&self, now: DateTime<Utc>) -> Result<Vec<QueueStat>, StoreError> {
        let rows = sqlx::query(
            "SELECT engine, line, count(*) AS ready,
                    EXTRACT(EPOCH FROM ($1 - min(not_before)))::float8 AS oldest
               FROM runs WHERE state = 'queued' AND not_before <= $1
              GROUP BY engine, line ORDER BY engine, line",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(QueueStat {
                    engine: engine_line(r.try_get("engine")?, r.try_get("line")?)?,
                    ready: r.try_get("ready")?,
                    oldest_secs: r.try_get::<f64, _>("oldest")?.max(0.0),
                })
            })
            .collect()
    }

    /// The engines with a hold in force at `now`, and every engine that
    /// ever had one (to set them back to 0).
    pub async fn holds(&self, now: DateTime<Utc>) -> Result<Vec<(EngineName, bool)>, StoreError> {
        let rows =
            sqlx::query("SELECT engine, until > $1 AS held FROM engine_holds ORDER BY engine")
                .bind(now)
                .fetch_all(&self.pool)
                .await?;
        rows.iter()
            .map(|r| {
                let engine = EngineName::new(r.try_get("engine")?).map_err(|e| corrupt(&e.0))?;
                Ok((engine, r.try_get("held")?))
            })
            .collect()
    }

    /// Runs that ended, by id, as the metrics count them.
    pub async fn ended_runs(&self, ids: &[String]) -> Result<Vec<EndedRun>, StoreError> {
        let rows = sqlx::query(
            "SELECT engine, line, kind, state,
                    COALESCE(EXTRACT(EPOCH FROM (finished_at - started_at)), 0)::float8 AS seconds
               FROM runs WHERE id::text = ANY($1) AND finished_at IS NOT NULL",
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(EndedRun {
                    engine: engine_line(r.try_get("engine")?, r.try_get("line")?)?,
                    kind: r.try_get("kind")?,
                    state: RunState::parse(r.try_get("state")?).map_err(|e| corrupt(&e.0))?,
                    seconds: r.try_get("seconds")?,
                })
            })
            .collect()
    }
}
