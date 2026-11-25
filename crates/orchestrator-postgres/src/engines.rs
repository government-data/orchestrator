//! The engines and lines registered by their runners, the runners seen,
//! engine pauses and the engines' drain holds.

use chrono::{DateTime, Utc};
use engine_port::{Descriptor, RunKind};
use orchestration_model::{EngineLine, EngineName, Line};
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// An engine and line as its runners registered it last.
#[derive(Debug, Clone, PartialEq)]
pub struct RegisteredEngine {
    pub engine: EngineLine,
    pub engine_version: String,
    pub port: u32,
    pub kinds: Vec<RunKind>,
    pub registered_at: DateTime<Utc>,
}

/// How many runners of an engine and line were seen since an instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRunners {
    pub engine: EngineLine,
    pub live: u32,
}

fn engine_line(engine: String, line: String) -> Result<EngineLine, StoreError> {
    Ok(EngineLine {
        engine: EngineName::new(&engine).map_err(|e| corrupt(&e.0))?,
        line: Line::new(&line).map_err(|e| corrupt(&e.0))?,
    })
}

impl PgStore {
    /// Records a runner's descriptor (already validated) and the runner
    /// itself; registering again changes only what changed.
    pub async fn register(
        &self,
        d: &Descriptor,
        runner: &str,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let kinds =
            serde_json::to_value(&d.kinds).map_err(|e| StoreError::Invalid(e.to_string()))?;
        let port = i32::try_from(d.port).map_err(|_| StoreError::Invalid("port".into()))?;
        let concurrency =
            i32::try_from(d.concurrency).map_err(|_| StoreError::Invalid("concurrency".into()))?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO engines (engine, line, engine_version, port, kinds, registered_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (engine, line) DO UPDATE
                SET engine_version = EXCLUDED.engine_version, port = EXCLUDED.port,
                    kinds = EXCLUDED.kinds, registered_at = EXCLUDED.registered_at",
        )
        .bind(d.engine.as_str())
        .bind(d.line.as_str())
        .bind(&d.engine_version)
        .bind(port)
        .bind(kinds)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO runners (runner, engine, line, concurrency, last_seen_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (runner) DO UPDATE
                SET engine = EXCLUDED.engine, line = EXCLUDED.line,
                    concurrency = EXCLUDED.concurrency, last_seen_at = EXCLUDED.last_seen_at",
        )
        .bind(runner)
        .bind(d.engine.as_str())
        .bind(d.line.as_str())
        .bind(concurrency)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Notes that a runner is alive (each claim and renewal), and why it
    /// does not claim when it says so.
    pub async fn seen(
        &self,
        runner: &str,
        paused_reason: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE runners SET last_seen_at = $2, paused_reason = $3 WHERE runner = $1")
            .bind(runner)
            .bind(now)
            .bind(paused_reason)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Every engine and line registered.
    pub async fn engines(&self) -> Result<Vec<RegisteredEngine>, StoreError> {
        let rows = sqlx::query(
            "SELECT engine, line, engine_version, port, kinds, registered_at
               FROM engines ORDER BY engine, line",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let kinds = serde_json::from_value(r.try_get("kinds")?)
                    .map_err(|e| corrupt(&e.to_string()))?;
                Ok(RegisteredEngine {
                    engine: engine_line(r.try_get("engine")?, r.try_get("line")?)?,
                    engine_version: r.try_get("engine_version")?,
                    port: u32::try_from(r.try_get::<i32, _>("port")?)
                        .map_err(|_| corrupt("port"))?,
                    kinds,
                    registered_at: r.try_get("registered_at")?,
                })
            })
            .collect()
    }

    /// The runners of each engine and line seen at or after `since`.
    pub async fn live_runners(&self, since: DateTime<Utc>) -> Result<Vec<LiveRunners>, StoreError> {
        let rows = sqlx::query(
            "SELECT engine, line, count(*) FILTER (WHERE last_seen_at >= $1) AS live
               FROM runners GROUP BY engine, line ORDER BY engine, line",
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(LiveRunners {
                    engine: engine_line(r.try_get("engine")?, r.try_get("line")?)?,
                    live: u32::try_from(r.try_get::<i64, _>("live")?)
                        .map_err(|_| corrupt("live"))?,
                })
            })
            .collect()
    }

    /// Pauses an engine: nothing new of it is handed out.
    pub async fn pause_engine(
        &self,
        engine: &EngineName,
        reason: &str,
        by: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO engine_pauses (engine, reason, paused_by, paused_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT (engine) DO UPDATE SET reason = EXCLUDED.reason",
        )
        .bind(engine.as_str())
        .bind(reason)
        .bind(by)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The engines on pause now.
    pub async fn paused_engines(&self) -> Result<Vec<EngineName>, StoreError> {
        let names: Vec<String> =
            sqlx::query_scalar("SELECT engine FROM engine_pauses ORDER BY engine")
                .fetch_all(&self.pool)
                .await?;
        names
            .iter()
            .map(|n| EngineName::new(n).map_err(|e| corrupt(&e.0)))
            .collect()
    }

    pub async fn resume_engine(&self, engine: &EngineName) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM engine_pauses WHERE engine = $1")
            .bind(engine.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Holds an engine until `until` (or extends the same hold) and answers
    /// how many of its runs still run. A different hold in force is
    /// replaced: the holder is the engine's own agent.
    pub async fn hold(
        &self,
        engine: &EngineName,
        hold: &str,
        until: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<u32, StoreError> {
        sqlx::query(
            "INSERT INTO engine_holds (engine, hold, until, held_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT (engine) DO UPDATE SET hold = EXCLUDED.hold, until = EXCLUDED.until",
        )
        .bind(engine.as_str())
        .bind(hold)
        .bind(until)
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.running(engine).await
    }

    /// Ends a hold; false if `hold` is not the one in force.
    pub async fn release(&self, engine: &EngineName, hold: &str) -> Result<bool, StoreError> {
        let done = sqlx::query("DELETE FROM engine_holds WHERE engine = $1 AND hold = $2")
            .bind(engine.as_str())
            .bind(hold)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Whether an engine is held at `now`.
    pub async fn held(&self, engine: &EngineName, now: DateTime<Utc>) -> Result<bool, StoreError> {
        let held: Option<i32> =
            sqlx::query_scalar("SELECT 1 FROM engine_holds WHERE engine = $1 AND until > $2")
                .bind(engine.as_str())
                .bind(now)
                .fetch_optional(&self.pool)
                .await?;
        Ok(held.is_some())
    }

    /// How many runs of an engine run now.
    pub async fn running(&self, engine: &EngineName) -> Result<u32, StoreError> {
        let n: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runs WHERE engine = $1 AND state = 'running'")
                .bind(engine.as_str())
                .fetch_one(&self.pool)
                .await?;
        u32::try_from(n).map_err(|_| corrupt("count"))
    }
}
