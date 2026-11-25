//! Folders and flows.

use chrono::{DateTime, Utc};
use orchestration_model::{EngineLine, EngineName, Flow, Folder, Line, OnInterrupt};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// A flow to create.
#[derive(Debug, Clone, PartialEq)]
pub struct NewFlow {
    pub folder_id: String,
    pub name: String,
    pub description: String,
    pub engine: EngineLine,
    pub kind: String,
    pub cluster: Option<String>,
    pub parameters: Value,
    pub timeout_secs: u32,
    pub allow_overlap: bool,
    pub on_interrupt: Option<OnInterrupt>,
}

const FLOW_COLUMNS: &str = "id::text, folder_id::text, name, description, engine, line, kind,
    cluster, parameters, timeout_secs, allow_overlap, on_interrupt, paused_at, enabled";

pub(crate) fn secs(raw: i32) -> Result<u32, StoreError> {
    u32::try_from(raw).map_err(|_| corrupt("a negative number of seconds"))
}

fn flow(r: &PgRow) -> Result<Flow, StoreError> {
    let on_interrupt: Option<String> = r.try_get("on_interrupt")?;
    Ok(Flow {
        id: r.try_get("id")?,
        folder_id: r.try_get("folder_id")?,
        name: r.try_get("name")?,
        description: r.try_get("description")?,
        engine: EngineLine {
            engine: EngineName::new(r.try_get("engine")?).map_err(|e| corrupt(&e.0))?,
            line: Line::new(r.try_get("line")?).map_err(|e| corrupt(&e.0))?,
        },
        kind: r.try_get("kind")?,
        cluster: r.try_get("cluster")?,
        parameters: r.try_get("parameters")?,
        timeout_secs: secs(r.try_get("timeout_secs")?)?,
        allow_overlap: r.try_get("allow_overlap")?,
        on_interrupt: match on_interrupt {
            Some(raw) => Some(OnInterrupt::parse(&raw).ok_or_else(|| corrupt("on_interrupt"))?),
            None => None,
        },
        paused_at: r.try_get("paused_at")?,
        enabled: r.try_get("enabled")?,
    })
}

pub(crate) fn signed(n: u32) -> Result<i32, StoreError> {
    i32::try_from(n).map_err(|_| StoreError::Invalid("a number out of range".into()))
}

impl PgStore {
    /// Creates a folder; its name is unique.
    pub async fn create_folder(&self, name: &str) -> Result<Folder, StoreError> {
        let id: String =
            sqlx::query_scalar("INSERT INTO folders (name) VALUES ($1) RETURNING id::text")
                .bind(name)
                .fetch_one(&self.pool)
                .await?;
        Ok(Folder {
            id,
            name: name.to_string(),
        })
    }

    pub async fn folders(&self) -> Result<Vec<Folder>, StoreError> {
        let rows = sqlx::query("SELECT id::text, name FROM folders ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| {
                Ok(Folder {
                    id: r.try_get("id")?,
                    name: r.try_get("name")?,
                })
            })
            .collect()
    }

    /// Creates a flow; its name is unique in its folder.
    pub async fn create_flow(&self, new: &NewFlow) -> Result<Flow, StoreError> {
        let sql = format!(
            "INSERT INTO flows (folder_id, name, description, engine, line, kind, cluster,
                                parameters, timeout_secs, allow_overlap, on_interrupt)
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             RETURNING {FLOW_COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(&new.folder_id)
            .bind(&new.name)
            .bind(&new.description)
            .bind(new.engine.engine.as_str())
            .bind(new.engine.line.as_str())
            .bind(&new.kind)
            .bind(&new.cluster)
            .bind(&new.parameters)
            .bind(signed(new.timeout_secs)?)
            .bind(new.allow_overlap)
            .bind(new.on_interrupt.map(OnInterrupt::as_str))
            .fetch_one(&self.pool)
            .await?;
        flow(&row)
    }

    pub async fn flow(&self, id: &str) -> Result<Option<Flow>, StoreError> {
        let sql = format!("SELECT {FLOW_COLUMNS} FROM flows WHERE id = $1::uuid");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(flow).transpose()
    }

    /// The flows of a folder, or every flow, by name.
    pub async fn flows(&self, folder_id: Option<&str>) -> Result<Vec<Flow>, StoreError> {
        let sql = format!(
            "SELECT {FLOW_COLUMNS} FROM flows
              WHERE $1::uuid IS NULL OR folder_id = $1::uuid ORDER BY name, id"
        );
        let rows = sqlx::query(&sql)
            .bind(folder_id)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(flow).collect()
    }

    /// Pauses a flow at `at`, or resumes it with `None`; false if it does
    /// not exist.
    pub async fn pause_flow(
        &self,
        id: &str,
        at: Option<DateTime<Utc>>,
    ) -> Result<bool, StoreError> {
        let done =
            sqlx::query("UPDATE flows SET paused_at = $2, updated_at = now() WHERE id = $1::uuid")
                .bind(id)
                .bind(at)
                .execute(&self.pool)
                .await?;
        Ok(done.rows_affected() == 1)
    }
}
