//! The people's API over HTTP (decision 0011, step 6), behind the
//! orchestrator's route of the web entry point: each call names the action
//! it needs, and [`People::allow`] refuses it before anything is read.
//! The import's calls need `orchestrator:ImportFlows`.
//!
//! | Call                       | Does                                           |
//! | -------------------------- | ---------------------------------------------- |
//! | `GET /api/v1/found`        | what a runner last found, to choose from       |
//! | `POST /api/v1/imports`     | import chosen artefacts (at most 500)          |
//! | `POST /api/v1/scans`       | ask a runner to report what it finds now       |
//!
//! Flows, folders, runs and logs are read in [`crate::people_views`].

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use engine_port::{FoundCalendar, ScheduleSource};
use orchestration_model::{EngineLine, EngineName, Line};
use orchestrator_postgres::{ScanAsked, StoreError};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{runner_of, Clock, EnginePort, ImportError, ImportItem, Machine, People, IMPORT_FLOWS};

/// What the people's handlers share.
pub struct PeopleState<C> {
    pub port: EnginePort<C>,
    pub people: People,
}

impl<C> std::fmt::Debug for PeopleState<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeopleState").finish_non_exhaustive()
    }
}

pub(crate) type Shared<C> = State<Arc<PeopleState<C>>>;

/// The people's API's routes.
pub fn people_router<C: Clock + Clone>(state: Arc<PeopleState<C>>) -> Router {
    Router::new()
        .route("/api/v1/found", get(found::<C>))
        .route("/api/v1/imports", post(import::<C>))
        .route("/api/v1/scans", post(scan::<C>))
        .with_state(Arc::clone(&state))
        .merge(crate::people_views::routes(state))
}

pub(crate) fn error(status: StatusCode, why: &str) -> Response {
    (status, Json(json!({ "error": why }))).into_response()
}

pub(crate) fn failed(e: &StoreError) -> Response {
    tracing::error!(error = %e, "people's API: the store failed");
    error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

/// A runner, by its machine, engine and line.
#[derive(Debug, Deserialize)]
struct RunnerRef {
    machine: String,
    engine: String,
    line: String,
}

impl RunnerRef {
    /// The runner named, or why it cannot be one (a 404).
    fn parse(&self) -> Result<(Machine, EngineLine), String> {
        let bad = |e: orchestration_model::InvalidName| e.0;
        let engine = EngineLine {
            engine: EngineName::new(&self.engine).map_err(bad)?,
            line: Line::new(&self.line).map_err(bad)?,
        };
        if self.machine.is_empty() || self.machine.len() > 253 {
            return Err("no such machine".into());
        }
        Ok((Machine(self.machine.clone()), engine))
    }
}

/// A found artefact as a person chooses from it: no schedule's account
/// or parameters.
#[derive(Debug, Serialize)]
struct Choice {
    path: String,
    segments: Vec<String>,
    sha256: String,
    size_bytes: u64,
    flags: Vec<engine_port::Flag>,
    schedules: Vec<ChoiceSchedule>,
}

#[derive(Debug, Serialize)]
struct ChoiceSchedule {
    source: ScheduleSource,
    calendar: FoundCalendar,
    timezone: Option<String>,
}

async fn found<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    query: Result<Query<RunnerRef>, axum::extract::rejection::QueryRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, IMPORT_FLOWS).await {
        return refused.into_response();
    }
    let Ok(Query(q)) = query else {
        return error(
            StatusCode::BAD_REQUEST,
            "name a machine, an engine and a line",
        );
    };
    let (machine, engine) = match q.parse() {
        Ok(parsed) => parsed,
        Err(why) => return error(StatusCode::NOT_FOUND, &why),
    };
    let runner = runner_of(&machine, &engine);
    let store = &s.port.store;
    let artefacts = match store.found_of(&runner).await {
        Ok(found) => found,
        Err(e) => return failed(&e),
    };
    let mut choices = Vec::with_capacity(artefacts.len());
    for a in artefacts {
        let schedules = match store.found_schedules_of(&runner, &a.path).await {
            Ok(schedules) => schedules,
            Err(e) => return failed(&e),
        };
        choices.push(Choice {
            schedules: schedules
                .into_iter()
                .map(|s| ChoiceSchedule {
                    source: s.source,
                    calendar: s.calendar,
                    timezone: s.timezone,
                })
                .collect(),
            path: a.path,
            segments: a.segments,
            sha256: a.sha256,
            size_bytes: a.size_bytes,
            flags: a.flags,
        });
    }
    Json(json!({ "artefacts": choices })).into_response()
}

/// An import request: one runner's artefacts.
#[derive(Debug, Deserialize)]
struct ImportRequest {
    #[serde(flatten)]
    runner: RunnerRef,
    items: Vec<ImportItem>,
}

async fn import<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    body: Result<Json<ImportRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, IMPORT_FLOWS).await {
        return refused.into_response();
    }
    let Ok(Json(req)) = body else {
        return error(StatusCode::BAD_REQUEST, "not an import");
    };
    let (machine, engine) = match req.runner.parse() {
        Ok(parsed) => parsed,
        Err(why) => return error(StatusCode::NOT_FOUND, &why),
    };
    match s.port.import(&machine, &engine, &req.items).await {
        Ok(answer) => Json(answer).into_response(),
        Err(ImportError::Count) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "an import takes 1 to 500 artefacts",
        ),
        Err(ImportError::UnknownEngine) => error(
            StatusCode::NOT_FOUND,
            "no such engine and line is registered",
        ),
        Err(ImportError::Store(e)) => failed(&e),
    }
}

async fn scan<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    body: Result<Json<RunnerRef>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, IMPORT_FLOWS).await {
        return refused.into_response();
    }
    let Ok(Json(runner)) = body else {
        return error(StatusCode::BAD_REQUEST, "not a runner");
    };
    let (machine, engine) = match runner.parse() {
        Ok(parsed) => parsed,
        Err(why) => return error(StatusCode::NOT_FOUND, &why),
    };
    match s.port.request_scan(&machine, &engine).await {
        Ok(ScanAsked::Asked) => StatusCode::ACCEPTED.into_response(),
        Ok(ScanAsked::NeverReported) => error(
            StatusCode::CONFLICT,
            "this runner reports nothing found: it cannot be asked",
        ),
        Ok(ScanAsked::UnknownRunner) => error(StatusCode::NOT_FOUND, "no such runner"),
        Err(crate::PortError::Store(e)) => failed(&e),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}
