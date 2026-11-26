//! Deploys (decision 0011, import): queued for the cluster their artefact
//! is on, claimed with `FOR UPDATE SKIP LOCKED` by a runner of their engine
//! and line, and finished once: the same finish again changes nothing.

use chrono::{DateTime, Utc};
use engine_port::{DeployClaimed, DeployFinish, FolderRecord, ScheduleSource};
use orchestration_model::EngineLine;
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// A deploy to queue: an import of an artefact already on `cluster`. Its
/// engine and line are its flow's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDeploy {
    pub flow_id: String,
    pub cluster: String,
    pub source_path: String,
    pub source_sha256: String,
    pub schedules: Vec<ScheduleSource>,
    pub queued_at: DateTime<Utc>,
}

/// What a finish did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployEnd {
    /// The end is recorded now.
    Recorded,
    /// The same end was recorded before: nothing changed.
    Unchanged,
}

/// A deploy as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deploy {
    pub id: String,
    pub flow_id: String,
    pub cluster: String,
    pub state: String,
    pub runner: Option<String>,
    pub path: Option<String>,
    pub version: Option<String>,
    pub sha256: Option<String>,
    pub external_disabled: Option<bool>,
}

impl PgStore {
    /// Queues a deploy of `new.flow_id`'s engine and line; its id.
    pub async fn create_deploy(&self, new: &NewDeploy) -> Result<String, StoreError> {
        let schedules = serde_json::to_value(&new.schedules).map_err(|_| corrupt("schedules"))?;
        let id: Option<String> = sqlx::query_scalar(
            "INSERT INTO deploys (flow_id, engine, line, cluster, source_path, source_sha256,
                                  schedules, queued_at)
             SELECT f.id, f.engine, f.line, $2, $3, $4, $5, $6 FROM flows f
              WHERE f.id = $1::uuid
             RETURNING id::text",
        )
        .bind(&new.flow_id)
        .bind(&new.cluster)
        .bind(&new.source_path)
        .bind(&new.source_sha256)
        .bind(schedules)
        .bind(new.queued_at)
        .fetch_optional(&self.pool)
        .await?;
        id.ok_or_else(|| StoreError::Invalid("no such flow".into()))
    }

    /// Hands `runner` the deploy it claimed and did not finish, else the
    /// oldest queued one of `engine` on `cluster`.
    pub async fn claim_deploy(
        &self,
        engine: &EngineLine,
        cluster: &str,
        runner: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<DeployClaimed>, StoreError> {
        let row = sqlx::query(CLAIM)
            .bind(engine.engine.as_str())
            .bind(engine.line.as_str())
            .bind(cluster)
            .bind(runner)
            .bind(now)
            .fetch_optional(&self.pool)
            .await?;
        let Some(r) = row else { return Ok(None) };
        let parameters = match r.try_get::<serde_json::Value, _>("parameters")? {
            serde_json::Value::Object(m) => m,
            _ => return Err(corrupt("parameters")),
        };
        Ok(Some(DeployClaimed {
            deploy: r.try_get("id")?,
            flow: r.try_get("flow_id")?,
            source_path: r.try_get("source_path")?,
            source_sha256: r.try_get("source_sha256")?,
            schedules: serde_json::from_value(r.try_get("schedules")?)
                .map_err(|_| corrupt("schedules"))?,
            folder: FolderRecord {
                id: r.try_get("folder_id")?,
                name: r.try_get("folder_name")?,
            },
            name: r.try_get("name")?,
            description: r.try_get("description")?,
            kind: r.try_get("kind")?,
            parameters,
            timeout_secs: u32::try_from(r.try_get::<i32, _>("timeout_secs")?)
                .map_err(|_| corrupt("timeout_secs"))?,
            allow_overlap: r.try_get("allow_overlap")?,
            on_interrupt: r.try_get("on_interrupt")?,
        }))
    }

    /// Records a claimed deploy's end, reported from `cluster`. The same end
    /// again is [`DeployEnd::Unchanged`]; `Conflict` for a deploy of another
    /// cluster, not claimed, or ended otherwise.
    pub async fn finish_deploy(
        &self,
        id: &str,
        cluster: &str,
        f: &DeployFinish,
        now: DateTime<Utc>,
    ) -> Result<DeployEnd, StoreError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(&format!("{SELECT} WHERE id = $1::uuid FOR UPDATE"))
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        let d = row
            .as_ref()
            .map(deploy_of)
            .transpose()?
            .filter(|d| d.cluster == cluster)
            .ok_or_else(|| StoreError::Conflict("deploy".into()))?;
        if d.state != "claimed" {
            let same = d.state == f.state.as_str()
                && d.path == f.path
                && d.version == f.version
                && d.sha256 == f.sha256
                && d.external_disabled == Some(f.external_disabled);
            return if same {
                Ok(DeployEnd::Unchanged)
            } else {
                Err(StoreError::Conflict("deploy".into()))
            };
        }
        sqlx::query(
            "UPDATE deploys SET state = $2, finished_at = $3, path = $4, version = $5,
                    sha256 = $6, external_disabled = $7
              WHERE id = $1::uuid",
        )
        .bind(id)
        .bind(f.state.as_str())
        .bind(now)
        .bind(&f.path)
        .bind(&f.version)
        .bind(&f.sha256)
        .bind(f.external_disabled)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(DeployEnd::Recorded)
    }

    /// A deploy as stored, if it exists.
    pub async fn deploy(&self, id: &str) -> Result<Option<Deploy>, StoreError> {
        let row = sqlx::query(&format!("{SELECT} WHERE id = $1::uuid"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(deploy_of).transpose()
    }
}

const SELECT: &str = "SELECT id::text, flow_id::text, cluster, state, runner, path, version,
                             sha256, external_disabled FROM deploys";

fn deploy_of(r: &sqlx::postgres::PgRow) -> Result<Deploy, StoreError> {
    Ok(Deploy {
        id: r.try_get("id")?,
        flow_id: r.try_get("flow_id")?,
        cluster: r.try_get("cluster")?,
        state: r.try_get("state")?,
        runner: r.try_get("runner")?,
        path: r.try_get("path")?,
        version: r.try_get("version")?,
        sha256: r.try_get("sha256")?,
        external_disabled: r.try_get("external_disabled")?,
    })
}

const CLAIM: &str = "
WITH pick AS (
    SELECT d.id
      FROM deploys d
     WHERE d.engine = $1 AND d.line = $2 AND d.cluster = $3
       AND (d.state = 'queued' OR (d.state = 'claimed' AND d.runner = $4))
     ORDER BY d.state = 'claimed' DESC, d.queued_at, d.id
     LIMIT 1
       FOR UPDATE OF d SKIP LOCKED
)
UPDATE deploys d
   SET state = 'claimed', runner = $4, claimed_at = COALESCE(d.claimed_at, $5)
  FROM pick, flows f, folders o
 WHERE d.id = pick.id AND f.id = d.flow_id AND o.id = f.folder_id
RETURNING d.id::text, d.flow_id::text, d.source_path, d.source_sha256, d.schedules,
          o.id::text AS folder_id, o.name AS folder_name, f.name, f.description, f.kind,
          f.parameters, f.timeout_secs, f.allow_overlap, f.on_interrupt";
