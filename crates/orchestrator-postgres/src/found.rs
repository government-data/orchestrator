//! What runners found on their clusters that no deploy placed (decision
//! 0011, import): each report replaces the runner's previous one.

use engine_port::{Flag, FoundArtefact, FoundSchedule};
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

impl PgStore {
    /// Replaces what `runner` found with `artefacts` and the `schedules`
    /// that start them, in one transaction; `Invalid` for a runner never
    /// registered.
    pub async fn replace_found(
        &self,
        runner: &str,
        artefacts: &[FoundArtefact],
        schedules: &[FoundSchedule],
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        let known: Option<String> =
            sqlx::query_scalar("SELECT runner FROM runners WHERE runner = $1 FOR UPDATE")
                .bind(runner)
                .fetch_optional(&mut *tx)
                .await?;
        if known.is_none() {
            return Err(StoreError::Invalid("no such runner".into()));
        }
        for table in ["found_schedules", "found_artefacts"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE runner = $1"))
                .bind(runner)
                .execute(&mut *tx)
                .await?;
        }
        for a in artefacts {
            let flags = serde_json::to_value(&a.flags).map_err(|_| corrupt("flags"))?;
            sqlx::query(
                "INSERT INTO found_artefacts (runner, path, segments, sha256, size_bytes, flags)
                 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (runner, path) DO NOTHING",
            )
            .bind(runner)
            .bind(&a.path)
            .bind(&a.segments)
            .bind(&a.sha256)
            .bind(i64::try_from(a.size_bytes).unwrap_or(i64::MAX))
            .bind(flags)
            .execute(&mut *tx)
            .await?;
        }
        for s in schedules {
            let schedule = serde_json::to_value(s).map_err(|_| corrupt("schedule"))?;
            sqlx::query(
                "INSERT INTO found_schedules (runner, artefact, schedule) VALUES ($1, $2, $3)",
            )
            .bind(runner)
            .bind(&s.artefact)
            .bind(schedule)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// What `runner` found, by path.
    pub async fn found_of(&self, runner: &str) -> Result<Vec<FoundArtefact>, StoreError> {
        let rows = sqlx::query(
            "SELECT path, segments, sha256, size_bytes, flags FROM found_artefacts
              WHERE runner = $1 ORDER BY path",
        )
        .bind(runner)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let flags: serde_json::Value = r.try_get("flags")?;
                Ok(FoundArtefact {
                    path: r.try_get("path")?,
                    segments: r.try_get("segments")?,
                    sha256: r.try_get("sha256")?,
                    size_bytes: u64::try_from(r.try_get::<i64, _>("size_bytes")?)
                        .map_err(|_| corrupt("size_bytes"))?,
                    flags: serde_json::from_value::<Vec<Flag>>(flags)
                        .map_err(|_| corrupt("flags"))?,
                })
            })
            .collect()
    }

    /// The external schedules `runner` found that start `artefact`.
    pub async fn found_schedules_of(
        &self,
        runner: &str,
        artefact: &str,
    ) -> Result<Vec<FoundSchedule>, StoreError> {
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT schedule FROM found_schedules WHERE runner = $1 AND artefact = $2
              ORDER BY schedule::text",
        )
        .bind(runner)
        .bind(artefact)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|v| serde_json::from_value(v).map_err(|_| corrupt("schedule")))
            .collect()
    }
}
