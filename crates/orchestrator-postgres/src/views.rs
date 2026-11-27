//! What people read of runs (decision 0011, step 6): a page of runs by
//! filters, one run, and its log. Neither a run's parameters nor who
//! started it is read here.

use chrono::{DateTime, Utc};
use orchestration_model::RunState;
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

/// Which runs, newest queued first; every filter is optional.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunFilter {
    pub flow_id: Option<String>,
    pub folder_id: Option<String>,
    pub state: Option<RunState>,
    pub engine: Option<String>,
    /// Queued at or after.
    pub from: Option<DateTime<Utc>>,
    /// Queued before.
    pub to: Option<DateTime<Utc>>,
    pub limit: u32,
    pub offset: u32,
}

/// A run as people see it.
#[derive(Debug, Clone, PartialEq)]
pub struct RunView {
    pub id: String,
    pub flow_id: String,
    pub flow_name: String,
    pub folder_id: String,
    pub engine: String,
    pub line: String,
    pub kind: String,
    pub cluster: Option<String>,
    pub state: RunState,
    /// `manual` or `schedule`.
    pub trigger: String,
    pub schedule_id: Option<String>,
    pub chain_id: Option<String>,
    pub attempt: u32,
    pub retry_of: Option<String>,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub runner: Option<String>,
    pub progress_fraction: Option<f64>,
    pub cancel_requested: bool,
    pub interruption: Option<String>,
    pub error_class: Option<String>,
    /// Named counts, as the runner reported them.
    pub counts: Value,
}

/// A page of runs, and how many match in all.
#[derive(Debug, Clone, PartialEq)]
pub struct RunPage {
    pub total: u64,
    pub rows: Vec<RunView>,
}

/// One line of a run's log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLogLine {
    pub seq: u64,
    pub stream: String,
    pub line: String,
    pub logged_at: DateTime<Utc>,
}

const RUN_COLUMNS: &str = "r.id::text, r.flow_id::text, f.name AS flow_name,
    f.folder_id::text, r.engine, r.line, r.kind, r.cluster, r.state, r.trigger,
    r.schedule_id::text, r.chain_id::text, r.attempt, r.retry_of::text, r.queued_at,
    r.started_at, r.finished_at, r.runner, r.progress_fraction, r.cancel_requested,
    r.interruption, r.error_class, r.counts";

const RUN_FILTER: &str = "FROM runs r JOIN flows f ON f.id = r.flow_id
    WHERE ($1::uuid IS NULL OR r.flow_id = $1::uuid)
      AND ($2::uuid IS NULL OR f.folder_id = $2::uuid)
      AND ($3::text IS NULL OR r.state = $3)
      AND ($4::text IS NULL OR r.engine = $4)
      AND ($5::timestamptz IS NULL OR r.queued_at >= $5)
      AND ($6::timestamptz IS NULL OR r.queued_at < $6)";

fn run_view(r: &PgRow) -> Result<RunView, StoreError> {
    let state: String = r.try_get("state")?;
    let attempt: i32 = r.try_get("attempt")?;
    Ok(RunView {
        id: r.try_get("id")?,
        flow_id: r.try_get("flow_id")?,
        flow_name: r.try_get("flow_name")?,
        folder_id: r.try_get("folder_id")?,
        engine: r.try_get("engine")?,
        line: r.try_get("line")?,
        kind: r.try_get("kind")?,
        cluster: r.try_get("cluster")?,
        state: RunState::parse(&state).map_err(|_| corrupt("a run's state"))?,
        trigger: r.try_get("trigger")?,
        schedule_id: r.try_get("schedule_id")?,
        chain_id: r.try_get("chain_id")?,
        attempt: u32::try_from(attempt).map_err(|_| corrupt("a run's attempt"))?,
        retry_of: r.try_get("retry_of")?,
        queued_at: r.try_get("queued_at")?,
        started_at: r.try_get("started_at")?,
        finished_at: r.try_get("finished_at")?,
        runner: r.try_get("runner")?,
        progress_fraction: r.try_get("progress_fraction")?,
        cancel_requested: r.try_get("cancel_requested")?,
        interruption: r.try_get("interruption")?,
        error_class: r.try_get("error_class")?,
        counts: r.try_get("counts")?,
    })
}

fn is_uuid(raw: &str) -> bool {
    raw.len() == 36
        && raw.chars().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

impl PgStore {
    /// The runs `f` names, newest queued first, and how many match in all.
    /// An id filter that is no id matches nothing.
    pub async fn runs_page(&self, f: &RunFilter) -> Result<RunPage, StoreError> {
        let ids = [&f.flow_id, &f.folder_id];
        if ids
            .iter()
            .any(|id| id.as_deref().is_some_and(|i| !is_uuid(i)))
        {
            return Ok(RunPage {
                total: 0,
                rows: Vec::new(),
            });
        }
        let total: i64 = sqlx::query_scalar(&format!("SELECT count(*) {RUN_FILTER}"))
            .bind(&f.flow_id)
            .bind(&f.folder_id)
            .bind(f.state.map(RunState::as_str))
            .bind(&f.engine)
            .bind(f.from)
            .bind(f.to)
            .fetch_one(&self.pool)
            .await?;
        let sql = format!(
            "SELECT {RUN_COLUMNS} {RUN_FILTER}
              ORDER BY r.queued_at DESC, r.id LIMIT $7 OFFSET $8"
        );
        let rows = sqlx::query(&sql)
            .bind(&f.flow_id)
            .bind(&f.folder_id)
            .bind(f.state.map(RunState::as_str))
            .bind(&f.engine)
            .bind(f.from)
            .bind(f.to)
            .bind(i64::from(f.limit))
            .bind(i64::from(f.offset))
            .fetch_all(&self.pool)
            .await?;
        Ok(RunPage {
            total: u64::try_from(total).unwrap_or(0),
            rows: rows.iter().map(run_view).collect::<Result<_, _>>()?,
        })
    }

    /// One run, if it exists.
    pub async fn run_view(&self, id: &str) -> Result<Option<RunView>, StoreError> {
        if !is_uuid(id) {
            return Ok(None);
        }
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM runs r JOIN flows f ON f.id = r.flow_id
              WHERE r.id = $1::uuid"
        );
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(run_view).transpose()
    }

    /// A run's log lines after `after` (a `seq`), at most `limit`, in order;
    /// `None` if the run does not exist.
    pub async fn run_log(
        &self,
        run: &str,
        after: u64,
        limit: u32,
    ) -> Result<Option<Vec<RunLogLine>>, StoreError> {
        if !is_uuid(run) {
            return Ok(None);
        }
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM runs WHERE id = $1::uuid)")
                .bind(run)
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            return Ok(None);
        }
        let rows = sqlx::query(
            "SELECT seq, stream, line, logged_at FROM run_logs
              WHERE run_id = $1::uuid AND seq > $2 ORDER BY seq LIMIT $3",
        )
        .bind(run)
        .bind(i64::try_from(after).unwrap_or(i64::MAX))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let lines = rows
            .iter()
            .map(|r| {
                let seq: i64 = r.try_get("seq")?;
                Ok(RunLogLine {
                    seq: u64::try_from(seq).map_err(|_| corrupt("a log line's seq"))?,
                    stream: r.try_get("stream")?,
                    line: r.try_get("line")?,
                    logged_at: r.try_get("logged_at")?,
                })
            })
            .collect::<Result<_, StoreError>>()?;
        Ok(Some(lines))
    }
}
