//! A person edits, pauses and resumes flows and engines (decision 0011,
//! step 6), behind `orchestrator:ManageFlows`. A paused flow's schedules
//! do not fire; a paused engine is handed nothing new.
//!
//! | Call                                | Does                              |
//! | ----------------------------------- | --------------------------------- |
//! | `PATCH /api/v1/flows/{id}`          | edit the fields given             |
//! | `PUT /api/v1/flows/{id}/pause`      | pause a flow                      |
//! | `DELETE /api/v1/flows/{id}/pause`   | resume it                         |
//! | `PUT /api/v1/engines/{name}/pause`  | pause an engine, with a reason    |
//! | `DELETE /api/v1/engines/{name}/pause` | resume it                       |

use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use orchestration_model::{EngineName, OnInterrupt};
use orchestrator_postgres::{FlowEdit, StoreError};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::people_http::{error, failed, Shared};
use crate::people_views::one;
use crate::{Clock, PeopleState, MANAGE_FLOWS};

pub(crate) fn routes<C: Clock + Clone>(state: Arc<PeopleState<C>>) -> Router {
    Router::new()
        .route(
            "/api/v1/flows/{id}/pause",
            put(pause_flow::<C>).delete(resume_flow::<C>),
        )
        .route(
            "/api/v1/engines/{name}/pause",
            put(pause_engine::<C>).delete(resume_engine::<C>),
        )
        .with_state(state)
}

/// A field given as `null` (`Some(None)`) apart from one left out (`None`).
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(d).map(Some)
}

/// A flow's edit as a person sends it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EditBody {
    folder_id: Option<String>,
    name: Option<String>,
    description: Option<String>,
    parameters: Option<Value>,
    timeout_secs: Option<u32>,
    allow_overlap: Option<bool>,
    /// `none`, `repeat` or `continue`; `null`: the installation's rule.
    #[serde(default, deserialize_with = "present")]
    on_interrupt: Option<Option<String>>,
    enabled: Option<bool>,
}

impl EditBody {
    fn edit(self) -> Result<FlowEdit, &'static str> {
        if self
            .name
            .as_ref()
            .is_some_and(|n| n.is_empty() || n.len() > 200)
        {
            return Err("name: 1 to 200 characters");
        }
        if self.parameters.as_ref().is_some_and(|p| !p.is_object()) {
            return Err("parameters: an object");
        }
        if self.timeout_secs == Some(0) {
            return Err("timeout_secs: at least 1");
        }
        let on_interrupt = match self.on_interrupt {
            Some(Some(raw)) => Some(Some(
                OnInterrupt::parse(&raw).ok_or("on_interrupt: none, repeat or continue")?,
            )),
            Some(None) => Some(None),
            None => None,
        };
        Ok(FlowEdit {
            folder_id: self.folder_id,
            name: self.name,
            description: self.description,
            parameters: self.parameters,
            timeout_secs: self.timeout_secs,
            allow_overlap: self.allow_overlap,
            on_interrupt,
            enabled: self.enabled,
        })
    }
}

/// `PATCH /api/v1/flows/{id}`, routed with the flow's read.
pub(crate) async fn edit_flow<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Result<Json<EditBody>, JsonRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, MANAGE_FLOWS).await {
        return refused.into_response();
    }
    let Ok(Json(body)) = body else {
        return error(StatusCode::BAD_REQUEST, "not a flow's edit");
    };
    let edit = match body.edit() {
        Ok(edit) => edit,
        Err(why) => return error(StatusCode::UNPROCESSABLE_ENTITY, why),
    };
    match s.port.store.flow(&id).await {
        Ok(Some(_)) => {}
        Ok(None) | Err(StoreError::Invalid(_)) => {
            return error(StatusCode::NOT_FOUND, "no such flow")
        }
        Err(e) => return failed(&e),
    }
    match s.port.store.update_flow(&id, &edit).await {
        Ok(Some(flow)) => Json(flow).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "no such flow"),
        Err(StoreError::Conflict(_)) => {
            error(StatusCode::CONFLICT, "a flow of that name is in the folder")
        }
        Err(StoreError::Invalid(_)) => error(StatusCode::UNPROCESSABLE_ENTITY, "no such folder"),
        Err(e) => failed(&e),
    }
}

async fn set_flow_pause<C: Clock>(
    s: &PeopleState<C>,
    headers: &HeaderMap,
    id: &str,
    pause: bool,
) -> Response {
    if let Err(refused) = s.people.allow(headers, MANAGE_FLOWS).await {
        return refused.into_response();
    }
    let at = pause.then(|| s.port.clock.now());
    let done = s
        .port
        .store
        .pause_flow(id, at)
        .await
        .map(|done| done.then_some(()));
    one(done, "flow", |()| StatusCode::NO_CONTENT.into_response())
}

async fn pause_flow<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    set_flow_pause(&s, &headers, &id, true).await
}

async fn resume_flow<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    set_flow_pause(&s, &headers, &id, false).await
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PauseBody {
    #[serde(default)]
    reason: String,
}

async fn pause_engine<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Result<Json<PauseBody>, JsonRejection>,
) -> Response {
    let person = match s.people.allow(&headers, MANAGE_FLOWS).await {
        Ok(person) => person,
        Err(refused) => return refused.into_response(),
    };
    let Ok(engine) = EngineName::new(&name) else {
        return error(StatusCode::NOT_FOUND, "no such engine");
    };
    let reason = match body {
        Ok(Json(b)) => b.reason,
        Err(JsonRejection::MissingJsonContentType(_)) => String::new(),
        Err(_) => return error(StatusCode::BAD_REQUEST, "not an engine's pause"),
    };
    if reason.len() > 500 {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "reason: at most 500 characters",
        );
    }
    let now = s.port.clock.now();
    match s
        .port
        .store
        .pause_engine(&engine, &reason, Some(&person.id), now)
        .await
    {
        Ok(()) => {
            tracing::info!(engine = %engine.as_str(), "a person paused an engine");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => failed(&e),
    }
}

async fn resume_engine<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, MANAGE_FLOWS).await {
        return refused.into_response();
    }
    let Ok(engine) = EngineName::new(&name) else {
        return error(StatusCode::NOT_FOUND, "no such engine");
    };
    match s.port.store.resume_engine(&engine).await {
        Ok(()) => {
            tracing::info!(engine = %engine.as_str(), "a person resumed an engine");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => failed(&e),
    }
}
