//! What the check before a module's removal counts (decision 0015): the
//! lines a module registered, the flows pinned to them (paused and disabled
//! ones too) and their queued or running runs.

use orchestration_model::EngineLine;
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// What is on the lines a module registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OnLines {
    pub lines: u32,
    pub flows: u32,
    pub runs: u32,
}

fn count(r: &sqlx::postgres::PgRow, name: &str) -> Result<u32, StoreError> {
    u32::try_from(r.try_get::<i64, _>(name)?).map_err(|_| corrupt(name))
}

impl PgStore {
    /// Records the module whose machine client registered `engine` and the
    /// runner `runner` (after [`PgStore::register`]).
    pub async fn record_module(
        &self,
        engine: &EngineLine,
        runner: &str,
        module: &str,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE engines SET module = $3 WHERE engine = $1 AND line = $2")
            .bind(engine.engine.as_str())
            .bind(engine.line.as_str())
            .bind(module)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE runners SET module = $2 WHERE runner = $1")
            .bind(runner)
            .bind(module)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The lines `module` registered and what is on them.
    pub async fn on_lines_of(&self, module: &str) -> Result<OnLines, StoreError> {
        let r = sqlx::query(
            "WITH l AS (SELECT engine, line FROM engines WHERE module = $1)
             SELECT (SELECT count(*) FROM l) AS lines,
                    (SELECT count(*) FROM flows f JOIN l USING (engine, line)) AS flows,
                    (SELECT count(*) FROM runs r JOIN l USING (engine, line)
                      WHERE r.state IN ('queued', 'running')) AS runs",
        )
        .bind(module)
        .fetch_one(&self.pool)
        .await?;
        Ok(OnLines {
            lines: count(&r, "lines")?,
            flows: count(&r, "flows")?,
            runs: count(&r, "runs")?,
        })
    }

    /// How many engine modules registered a line.
    pub async fn engine_modules(&self) -> Result<u32, StoreError> {
        let r = sqlx::query(
            "SELECT count(DISTINCT module) AS modules FROM engines WHERE module IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await?;
        count(&r, "modules")
    }
}
