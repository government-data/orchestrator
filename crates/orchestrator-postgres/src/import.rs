//! An import (decision 0011): an artefact already on a cluster becomes a
//! flow of that cluster, in the folder a person chose (found by name or
//! created), and a deploy with no upload that the cluster's runner runs.
//! One transaction: the flow never exists without its deploy. An artefact
//! already imported (a deploy not failed, or a flow at its path on that
//! cluster) is refused, so an import repeated makes no second flow.

use chrono::{DateTime, Utc};
use engine_port::ScheduleSource;
use orchestration_model::EngineLine;

use crate::flows::signed;
use crate::{corrupt, PgStore, StoreError};

/// What to import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewImport {
    pub folder: String,
    pub name: String,
    pub engine: EngineLine,
    pub kind: String,
    pub timeout_secs: u32,
    /// The machine the artefact is on.
    pub cluster: String,
    pub source_path: String,
    pub source_sha256: String,
    /// The external schedules that start it, for the runner to disable.
    pub schedules: Vec<ScheduleSource>,
    pub queued_at: DateTime<Utc>,
}

/// An import done: the new flow and its deploy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Imported {
    pub flow_id: String,
    pub deploy_id: String,
}

/// Why an import was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportRefused {
    /// That path on that cluster is a flow already, or is being deployed.
    AlreadyImported,
    /// The folder has a flow of that name.
    NameTaken,
}

impl PgStore {
    /// Imports `new` in one transaction.
    pub async fn import_flow(
        &self,
        new: &NewImport,
    ) -> Result<Result<Imported, ImportRefused>, StoreError> {
        let schedules = serde_json::to_value(&new.schedules).map_err(|_| corrupt("schedules"))?;
        let mut tx = self.pool.begin().await?;
        // Two imports of one artefact at once: the second waits, then sees
        // the first.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('import:' || $1 || ':' || $2))")
            .bind(&new.cluster)
            .bind(&new.source_path)
            .execute(&mut *tx)
            .await?;
        let taken: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM deploys WHERE cluster = $1 AND source_path = $2
                                                    AND state <> 'failed')
                 OR EXISTS (SELECT 1 FROM flows WHERE cluster = $1 AND path = $2)",
        )
        .bind(&new.cluster)
        .bind(&new.source_path)
        .fetch_one(&mut *tx)
        .await?;
        if taken {
            return Ok(Err(ImportRefused::AlreadyImported));
        }
        let folder: String = sqlx::query_scalar(
            "INSERT INTO folders (name) VALUES ($1)
             ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id::text",
        )
        .bind(&new.folder)
        .fetch_one(&mut *tx)
        .await?;
        let flow: Option<String> = sqlx::query_scalar(
            "INSERT INTO flows (folder_id, name, engine, line, kind, cluster, timeout_secs)
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (folder_id, name) DO NOTHING RETURNING id::text",
        )
        .bind(&folder)
        .bind(&new.name)
        .bind(new.engine.engine.as_str())
        .bind(new.engine.line.as_str())
        .bind(&new.kind)
        .bind(&new.cluster)
        .bind(signed(new.timeout_secs)?)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(flow_id) = flow else {
            return Ok(Err(ImportRefused::NameTaken));
        };
        let deploy_id: String = sqlx::query_scalar(
            "INSERT INTO deploys (flow_id, engine, line, cluster, source_path, source_sha256,
                                  schedules, queued_at)
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8) RETURNING id::text",
        )
        .bind(&flow_id)
        .bind(new.engine.engine.as_str())
        .bind(new.engine.line.as_str())
        .bind(&new.cluster)
        .bind(&new.source_path)
        .bind(&new.source_sha256)
        .bind(schedules)
        .bind(new.queued_at)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Ok(Imported { flow_id, deploy_id }))
    }
}
