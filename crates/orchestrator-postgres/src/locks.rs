//! A run's locks (core decision 0090, points 4 and 6.2.4): a running run
//! takes a named lock under its lease when no other running run holds it,
//! by a taken lock or by its run-level `lock_name`. A lock whose run is no
//! longer running is free, so a run that ends or is lost frees its locks
//! without a call. Taking is serialised with claims by the claim's lock.

use chrono::{DateTime, Utc};

use crate::{PgStore, StoreError};

impl PgStore {
    /// Takes `name` for `run`, held under `lease`: `true` if the run holds
    /// it now (taken again, it stays the run's), `false` while another
    /// running run holds it.
    pub async fn take_lock(
        &self,
        run: &str,
        lease: &str,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('orchestrator-claim'))")
            .execute(&mut *tx)
            .await?;
        self.holds_lease(&mut tx, run, lease).await?;
        let busy: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM run_locks l JOIN runs o ON o.id = l.run_id
                             WHERE l.name = $2 AND o.state = 'running' AND o.id <> $1::uuid)
                 OR EXISTS (SELECT 1 FROM runs o
                             WHERE o.lock_name = $2 AND o.state = 'running' AND o.id <> $1::uuid)",
        )
        .bind(run)
        .bind(name)
        .fetch_one(&mut *tx)
        .await?;
        if !busy {
            sqlx::query(
                "INSERT INTO run_locks (name, run_id, taken_at) VALUES ($1, $2::uuid, $3)
                 ON CONFLICT (name) DO UPDATE SET run_id = $2::uuid, taken_at = $3
                  WHERE run_locks.run_id <> $2::uuid",
            )
            .bind(name)
            .bind(run)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(!busy)
    }

    /// Gives `name` back if `run` holds it; anything else changes nothing.
    pub async fn release_lock(&self, run: &str, lease: &str, name: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        self.holds_lease(&mut tx, run, lease).await?;
        sqlx::query("DELETE FROM run_locks WHERE name = $1 AND run_id = $2::uuid")
            .bind(name)
            .bind(run)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
