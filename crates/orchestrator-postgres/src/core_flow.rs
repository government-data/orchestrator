//! The core's flow (decision 0011, ruling 97): the one flow the core's
//! schedules fire on and its chains start on, made once at the core's
//! request and kept in the settings.

use orchestration_model::{EngineLine, Flow};
use serde_json::json;

use crate::flows::{flow, signed, FLOW_COLUMNS};
use crate::{PgStore, StoreError};

/// The folder and the name of the core's flow.
pub const CORE_FOLDER: &str = "core";
pub const CORE_FLOW_NAME: &str = "chains";

/// The core's flow, as the core asks for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCoreFlow {
    pub engine: EngineLine,
    pub timeout_secs: u32,
}

impl PgStore {
    /// The core's flow's id, when there is one.
    pub async fn core_flow(&self) -> Result<Option<String>, StoreError> {
        let id = sqlx::query_scalar("SELECT core_flow_id::text FROM settings")
            .fetch_one(&self.pool)
            .await?;
        Ok(id)
    }

    /// Makes `id` the core's flow; [`StoreError::Invalid`] if no such flow.
    pub async fn set_core_flow(&self, id: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE settings SET core_flow_id = $1::uuid, updated_at = now()")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// The core's flow and `true` when this call made it: in folder
    /// [`CORE_FOLDER`], named [`CORE_FLOW_NAME`], on `new`'s engine and
    /// line, with no parameters, its runs allowed to overlap (the engine's
    /// pools bound them). The flow there is already is returned as it is.
    /// [`StoreError::Conflict`] if a person's flow has that name.
    pub async fn make_core_flow(&self, new: &NewCoreFlow) -> Result<(Flow, bool), StoreError> {
        let mut tx = self.pool.begin().await?;
        let kept: Option<String> =
            sqlx::query_scalar("SELECT core_flow_id::text FROM settings FOR UPDATE")
                .fetch_one(&mut *tx)
                .await?;
        let sql = format!("SELECT {FLOW_COLUMNS} FROM flows WHERE id = $1::uuid");
        if let Some(id) = kept {
            let row = sqlx::query(&sql).bind(&id).fetch_one(&mut *tx).await?;
            return Ok((flow(&row)?, false));
        }
        let folder: String = sqlx::query_scalar(
            "INSERT INTO folders (name) VALUES ($1)
             ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id::text",
        )
        .bind(CORE_FOLDER)
        .fetch_one(&mut *tx)
        .await?;
        let insert = format!(
            "INSERT INTO flows (folder_id, name, description, engine, line, kind,
                                parameters, timeout_secs, allow_overlap)
             VALUES ($1::uuid, $2, $3, $4, $5, 'chain', $6, $7, true)
             RETURNING {FLOW_COLUMNS}"
        );
        let row = sqlx::query(&insert)
            .bind(&folder)
            .bind(CORE_FLOW_NAME)
            .bind("The core's schedules and jobs, as chains")
            .bind(new.engine.engine.as_str())
            .bind(new.engine.line.as_str())
            .bind(json!({}))
            .bind(signed(new.timeout_secs)?)
            .fetch_one(&mut *tx)
            .await?;
        let made = flow(&row)?;
        sqlx::query("UPDATE settings SET core_flow_id = $1::uuid, updated_at = now()")
            .bind(&made.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((made, true))
    }
}
