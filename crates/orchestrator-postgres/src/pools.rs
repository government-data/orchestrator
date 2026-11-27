//! An engine's slot pools (core decision 0090, point 6.3): the sizes are a
//! live setting, replaced whenever the engine module sends its pools; a
//! claim stamps a run's pool from its kind and hands out, among the runs
//! of a pool, the one whose `fair_by` value was served least recently.

use engine_port::SlotPool;
use orchestration_model::EngineLine;

use crate::{PgStore, StoreError};

impl PgStore {
    /// Replaces the pools of `engine`'s kinds with `pools`, and sets each
    /// pool's size. A run already running keeps its pool.
    pub async fn set_pools(
        &self,
        engine: &EngineLine,
        pools: &[SlotPool],
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM kind_pools WHERE engine = $1 AND line = $2")
            .bind(engine.engine.as_str())
            .bind(engine.line.as_str())
            .execute(&mut *tx)
            .await?;
        for p in pools {
            let size = i32::try_from(p.size).map_err(|_| StoreError::Invalid("size".into()))?;
            sqlx::query(
                "INSERT INTO slot_pools (name, size) VALUES ($1, $2)
                 ON CONFLICT (name) DO UPDATE SET size = EXCLUDED.size",
            )
            .bind(&p.name)
            .bind(size)
            .execute(&mut *tx)
            .await?;
            for kind in &p.kinds {
                sqlx::query(
                    "INSERT INTO kind_pools (engine, line, kind, pool, fair_by)
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(engine.engine.as_str())
                .bind(engine.line.as_str())
                .bind(kind)
                .bind(&p.name)
                .bind(&p.fair_by)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
}
