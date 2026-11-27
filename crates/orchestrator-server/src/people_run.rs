//! A person runs a flow now or cancels a run (decision 0011, step 6),
//! behind `orchestrator:RunWorkflow`.
//!
//! | Call                            | Does                                     |
//! | ------------------------------- | ---------------------------------------- |
//! | `POST /api/v1/flows/{id}/runs`  | queue a run of the flow now, as it is    |
//! | `POST /api/v1/runs/{id}/cancel` | cancel a queued run, or ask a running one |

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use orchestration_model::{Flow, Trigger};
use orchestrator_postgres::{Cancelled, NewRun, StoreError};
use serde_json::json;

use crate::people_http::{error, failed, Shared};
use crate::people_views::one;
use crate::{Clock, EnginePort, PeopleState, Person, RUN_WORKFLOW};

pub(crate) fn routes<C: Clock + Clone>(state: Arc<PeopleState<C>>) -> Router {
    Router::new()
        .route("/api/v1/flows/{id}/runs", post(run_now::<C>))
        .route("/api/v1/runs/{id}/cancel", post(cancel::<C>))
        .with_state(state)
}

/// A manual run of `flow` by `person`, with the flow's own parameters.
fn manual_run<C: Clock>(port: &EnginePort<C>, flow: &Flow, person: Person) -> NewRun {
    NewRun {
        flow_id: flow.id.clone(),
        engine: flow.engine.clone(),
        kind: flow.kind.clone(),
        cluster: flow.cluster.clone(),
        trigger: Trigger::Manual(person.id),
        fire_at: None,
        parameters: flow.parameters.clone(),
        timeout_secs: flow.timeout_secs,
        lock_name: None,
        slot_pool: None,
        queued_at: port.clock.now(),
    }
}

async fn run_now<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let person = match s.people.allow(&headers, RUN_WORKFLOW).await {
        Ok(person) => person,
        Err(refused) => return refused.into_response(),
    };
    let flow = match s.port.store.flow(&id).await {
        Ok(Some(flow)) => flow,
        Ok(None) | Err(StoreError::Invalid(_)) => {
            return error(StatusCode::NOT_FOUND, "no such flow")
        }
        Err(e) => return failed(&e),
    };
    if !flow.enabled {
        return error(StatusCode::CONFLICT, "the flow is disabled");
    }
    let core_flow = match s.port.store.core_flow().await {
        Ok(id) => id,
        Err(e) => return failed(&e),
    };
    if core_flow.as_deref() == Some(flow.id.as_str()) {
        return error(StatusCode::CONFLICT, "the core starts this flow's runs");
    }
    match s
        .port
        .store
        .enqueue(&manual_run(&s.port, &flow, person))
        .await
    {
        Ok(Some(run)) => {
            tracing::info!(run = %run, "a person queued a run");
            (StatusCode::ACCEPTED, Json(json!({ "run": run }))).into_response()
        }
        Ok(None) => error(StatusCode::CONFLICT, "the run was not queued"),
        Err(e) => failed(&e),
    }
}

async fn cancel<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, RUN_WORKFLOW).await {
        return refused.into_response();
    }
    let cancelled = match s.port.store.cancel(&id, s.port.clock.now()).await {
        Ok(cancelled) => cancelled,
        Err(StoreError::Invalid(_)) => return error(StatusCode::NOT_FOUND, "no such run"),
        Err(e) => return failed(&e),
    };
    match cancelled {
        Cancelled::Now => {
            tracing::info!(run = %id, "a person cancelled a queued run");
            Json(json!({ "state": "cancelled" })).into_response()
        }
        Cancelled::Requested => {
            tracing::info!(run = %id, "a person asked a running run to stop");
            let body = json!({ "state": "running", "cancel_requested": true });
            (StatusCode::ACCEPTED, Json(body)).into_response()
        }
        Cancelled::Nothing => one(s.port.store.run_view(&id).await, "run", |_| {
            error(StatusCode::CONFLICT, "the run has ended")
        }),
    }
}
