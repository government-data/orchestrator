//! A new cluster adopts its inventory once (decision 0011, re-attach): per
//! flow, the latest version, a known flow on a gone machine moves to this
//! cluster with its runs, schedules, variables and folder; an unknown one
//! is registered again with its same id. All in one transaction, so a
//! crash adopts nothing and the next report adopts it all.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use engine_port::{ArtefactRecord, InventoryAnswer, InventoryItem, SkipReason, Skipped};
use orchestration_model::EngineLine;
use serde_json::Value;
use sqlx::{PgConnection, Row};

use crate::{PgStore, StoreError};

/// Who adopts: the runner, the machine its cluster is, its engine and
/// line, and the machines the core lists as live.
#[derive(Debug, Clone, Copy)]
pub struct Adoption<'a> {
    pub runner: &'a str,
    pub cluster: &'a str,
    pub engine: &'a EngineLine,
    pub live: &'a HashSet<String>,
    pub now: DateTime<Utc>,
}

/// What to do with one item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    Reattach,
    Register,
    Skip(SkipReason),
}

/// A known flow as adoption reads it.
struct Known {
    cluster: Option<String>,
    engine: String,
    line: String,
}

fn placement(a: &Adoption<'_>, known: Option<&Known>) -> Placement {
    let Some(k) = known else {
        return Placement::Register;
    };
    if k.engine != a.engine.engine.as_str() || k.line != a.engine.line.as_str() {
        // Another engine or line: moving it is a line move (decision 0015).
        return Placement::Skip(SkipReason::Invalid);
    }
    match k.cluster.as_deref() {
        // Already attached: the same machine name.
        Some(c) if c == a.cluster => Placement::Reattach,
        Some(c) if !a.live.contains(c) => Placement::Reattach,
        // On a live machine, or on any runner of its line.
        _ => Placement::Skip(SkipReason::OnLiveCluster),
    }
}

impl PgStore {
    /// Whether `runner` still adopts its inventory; `Invalid` for a runner
    /// never registered.
    pub async fn adopts_inventory(&self, runner: &str) -> Result<bool, StoreError> {
        let adopts: Option<bool> =
            sqlx::query_scalar("SELECT adopts_inventory FROM runners WHERE runner = $1")
                .bind(runner)
                .fetch_optional(&self.pool)
                .await?;
        adopts.ok_or_else(|| StoreError::Invalid("no such runner".into()))
    }

    /// Adopts `items` (the latest version of each flow) for `a.runner`;
    /// `None` when it adopted before, and then nothing changes.
    pub async fn adopt_inventory(
        &self,
        a: &Adoption<'_>,
        items: &[InventoryItem],
    ) -> Result<Option<InventoryAnswer>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let adopts: Option<bool> =
            sqlx::query_scalar("SELECT adopts_inventory FROM runners WHERE runner = $1 FOR UPDATE")
                .bind(a.runner)
                .fetch_optional(&mut *tx)
                .await?;
        match adopts {
            None => return Err(StoreError::Invalid("no such runner".into())),
            Some(false) => return Ok(None),
            Some(true) => {}
        }
        let mut answer = InventoryAnswer::default();
        for item in items {
            adopt_item(&mut tx, a, item, &mut answer).await?;
        }
        sqlx::query(
            "UPDATE runners SET adopts_inventory = false, adopted_at = $2 WHERE runner = $1",
        )
        .bind(a.runner)
        .bind(a.now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(answer))
    }
}

async fn adopt_item(
    tx: &mut PgConnection,
    a: &Adoption<'_>,
    item: &InventoryItem,
    answer: &mut InventoryAnswer,
) -> Result<(), StoreError> {
    let flow = &item.record.flow;
    let skip = |answer: &mut InventoryAnswer, reason| {
        answer.skipped.push(Skipped {
            path: item.path.clone(),
            reason,
        });
    };
    if i32::try_from(item.record.timeout_secs).is_err() {
        skip(answer, SkipReason::Invalid);
        return Ok(());
    }
    if path_taken(tx, a.cluster, &item.path, flow).await? {
        skip(answer, SkipReason::PathRegistered);
        return Ok(());
    }
    let known = known(tx, flow).await?;
    match placement(a, known.as_ref()) {
        Placement::Skip(reason) => skip(answer, reason),
        Placement::Reattach => {
            reattach(tx, a.cluster, &item.path, flow).await?;
            answer.reattached.push(flow.clone());
        }
        Placement::Register => {
            let (folder, created) = folder_of(tx, &item.record).await?;
            if name_taken(tx, &folder, &item.record.name).await? {
                // Another flow has its name in its folder: it cannot be
                // registered as it is.
                skip(answer, SkipReason::Invalid);
                return Ok(());
            }
            register(tx, a, item, &folder).await?;
            answer.folders_created += u32::from(created);
            answer.registered.push(flow.clone());
        }
    }
    Ok(())
}

/// Whether another flow is registered at `path` on `cluster`: placed there
/// by an inventory, or by a deploy that succeeded.
async fn path_taken(
    tx: &mut PgConnection,
    cluster: &str,
    path: &str,
    flow: &str,
) -> Result<bool, StoreError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM flows
                         WHERE cluster = $1 AND path = $2 AND id <> $3::uuid)
             OR EXISTS (SELECT 1 FROM deploys
                         WHERE cluster = $1 AND path = $2 AND state = 'succeeded'
                           AND flow_id <> $3::uuid)",
    )
    .bind(cluster)
    .bind(path)
    .bind(flow)
    .fetch_one(&mut *tx)
    .await?)
}

async fn known(tx: &mut PgConnection, flow: &str) -> Result<Option<Known>, StoreError> {
    let row = sqlx::query("SELECT cluster, engine, line FROM flows WHERE id = $1::uuid FOR UPDATE")
        .bind(flow)
        .fetch_optional(&mut *tx)
        .await?;
    row.map(|r| {
        Ok(Known {
            cluster: r.try_get("cluster")?,
            engine: r.try_get("engine")?,
            line: r.try_get("line")?,
        })
    })
    .transpose()
}

/// Moves the flow, and its runs still queued for its old cluster, to
/// `cluster`; runs leased to the old one expire and the reaper retries
/// them on the flow's cluster.
async fn reattach(
    tx: &mut PgConnection,
    cluster: &str,
    path: &str,
    flow: &str,
) -> Result<(), StoreError> {
    sqlx::query("UPDATE flows SET cluster = $2, path = $3, updated_at = now() WHERE id = $1::uuid")
        .bind(flow)
        .bind(cluster)
        .bind(path)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE runs SET cluster = $2
          WHERE flow_id = $1::uuid AND state = 'queued' AND cluster IS NOT NULL",
    )
    .bind(flow)
    .bind(cluster)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// The record's folder found by id, else by name, else created with its
/// id; whether it was created.
async fn folder_of(
    tx: &mut PgConnection,
    record: &ArtefactRecord,
) -> Result<(String, bool), StoreError> {
    let found: Option<String> = sqlx::query_scalar(
        "SELECT id::text FROM folders WHERE id = $1::uuid OR name = $2
          ORDER BY id = $1::uuid DESC LIMIT 1",
    )
    .bind(&record.folder.id)
    .bind(&record.folder.name)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(id) = found {
        return Ok((id, false));
    }
    sqlx::query("INSERT INTO folders (id, name) VALUES ($1::uuid, $2)")
        .bind(&record.folder.id)
        .bind(&record.folder.name)
        .execute(&mut *tx)
        .await?;
    Ok((record.folder.id.clone(), true))
}

async fn name_taken(tx: &mut PgConnection, folder: &str, name: &str) -> Result<bool, StoreError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM flows WHERE folder_id = $1::uuid AND name = $2)",
    )
    .bind(folder)
    .bind(name)
    .fetch_one(&mut *tx)
    .await?)
}

/// Registers the record's flow again with its same id, on this cluster.
async fn register(
    tx: &mut PgConnection,
    a: &Adoption<'_>,
    item: &InventoryItem,
    folder: &str,
) -> Result<(), StoreError> {
    let r = &item.record;
    sqlx::query(
        "INSERT INTO flows (id, folder_id, name, description, engine, line, kind, cluster, path,
                            parameters, timeout_secs, allow_overlap, on_interrupt)
         VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(&r.flow)
    .bind(folder)
    .bind(&r.name)
    .bind(&r.description)
    .bind(a.engine.engine.as_str())
    .bind(a.engine.line.as_str())
    .bind(&r.kind)
    .bind(a.cluster)
    .bind(&item.path)
    .bind(Value::Object(r.parameters.clone()))
    .bind(i32::try_from(r.timeout_secs).unwrap_or(i32::MAX))
    .bind(r.allow_overlap)
    .bind(r.on_interrupt.as_deref())
    .execute(&mut *tx)
    .await?;
    Ok(())
}
