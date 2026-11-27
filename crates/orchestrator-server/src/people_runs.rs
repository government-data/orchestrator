//! What people read of runs (decision 0011, step 6), behind
//! `orchestrator:ViewFlows`: a run is shown without its parameters or who
//! started it, and its log is the run's technical log, as its runner sent
//! it.
//!
//! | Call                          | Does                                       |
//! | ----------------------------- | ------------------------------------------ |
//! | `GET /api/v1/runs`            | a page of runs, newest first, with filters |
//! | `GET /api/v1/runs/{id}`       | one run                                    |
//! | `GET /api/v1/runs/{id}/logs`  | a run's log lines after `after`, in order  |

use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use orchestration_model::RunState;
use orchestrator_postgres::{RunFilter, RunView};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::people_http::{error, failed, Shared};
use crate::people_views::one;
use crate::{Clock, PeopleState, VIEW_FLOWS};

/// Runs on a page by default, and at most.
const PAGE: u32 = 50;
const MAX_PAGE: u32 = 200;
/// Log lines in one answer by default, and at most.
const LOG_LINES: u32 = 1000;

pub(crate) fn routes<C: Clock + Clone>(state: Arc<PeopleState<C>>) -> Router {
    Router::new()
        .route("/api/v1/runs", get(runs::<C>))
        .route("/api/v1/runs/{id}", get(run::<C>))
        .route("/api/v1/runs/{id}/logs", get(logs::<C>))
        .with_state(state)
}

/// A run as people see it: no parameters, no one who started it.
pub(crate) fn run_json(r: &RunView) -> Value {
    json!({
        "id": r.id, "flow_id": r.flow_id, "flow_name": r.flow_name, "folder_id": r.folder_id,
        "engine": r.engine, "line": r.line, "kind": r.kind, "cluster": r.cluster,
        "state": r.state.as_str(), "trigger": r.trigger, "schedule_id": r.schedule_id,
        "chain_id": r.chain_id, "attempt": r.attempt, "retry_of": r.retry_of,
        "queued_at": r.queued_at, "started_at": r.started_at, "finished_at": r.finished_at,
        "runner": r.runner, "progress_fraction": r.progress_fraction,
        "cancel_requested": r.cancel_requested, "interruption": r.interruption,
        "error_class": r.error_class, "counts": r.counts,
    })
}

#[derive(Debug, Deserialize)]
struct RunsQuery {
    flow: Option<String>,
    folder: Option<String>,
    state: Option<String>,
    engine: Option<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl RunsQuery {
    fn filter(self) -> Result<RunFilter, String> {
        let state = match self.state.as_deref() {
            Some(raw) => Some(RunState::parse(raw).map_err(|_| "state: not a run's state")?),
            None => None,
        };
        let limit = self.limit.unwrap_or(PAGE);
        if !(1..=MAX_PAGE).contains(&limit) {
            return Err(format!("limit: 1 to {MAX_PAGE}"));
        }
        Ok(RunFilter {
            flow_id: self.flow,
            folder_id: self.folder,
            state,
            engine: self.engine,
            from: self.from,
            to: self.to,
            limit,
            offset: self.offset.unwrap_or(0),
        })
    }
}

async fn runs<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    query: Result<Query<RunsQuery>, QueryRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, VIEW_FLOWS).await {
        return refused.into_response();
    }
    let Ok(Query(q)) = query else {
        return error(StatusCode::BAD_REQUEST, "not a runs query");
    };
    let filter = match q.filter() {
        Ok(filter) => filter,
        Err(why) => return error(StatusCode::BAD_REQUEST, &why),
    };
    match s.port.store.runs_page(&filter).await {
        Ok(page) => {
            let runs: Vec<Value> = page.rows.iter().map(run_json).collect();
            Json(json!({ "total": page.total, "runs": runs })).into_response()
        }
        Err(e) => failed(&e),
    }
}

async fn run<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, VIEW_FLOWS).await {
        return refused.into_response();
    }
    one(s.port.store.run_view(&id).await, "run", |run| {
        Json(run_json(&run)).into_response()
    })
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    after: Option<u64>,
    limit: Option<u32>,
}

async fn logs<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
    query: Result<Query<LogsQuery>, QueryRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, VIEW_FLOWS).await {
        return refused.into_response();
    }
    let Ok(Query(q)) = query else {
        return error(StatusCode::BAD_REQUEST, "not a logs query");
    };
    let limit = q.limit.unwrap_or(LOG_LINES);
    if !(1..=LOG_LINES).contains(&limit) {
        return error(StatusCode::BAD_REQUEST, &format!("limit: 1 to {LOG_LINES}"));
    }
    let found = s.port.store.run_log(&id, q.after.unwrap_or(0), limit).await;
    one(found, "run", |lines| {
        let lines: Vec<Value> = lines
            .iter()
            .map(|l| json!({"seq": l.seq, "stream": l.stream, "line": l.line, "logged_at": l.logged_at}))
            .collect();
        Json(json!({ "lines": lines })).into_response()
    })
}
