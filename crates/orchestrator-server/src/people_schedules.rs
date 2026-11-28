//! A flow's schedules for people (decision 0011, step 6): read behind
//! `orchestrator:ViewFlows`, created, enabled, disabled and deleted behind
//! `orchestrator:ManageSchedules`. A schedule the core keeps (core
//! decision 0090) is shown but changed only by the core.
//!
//! | Call                                | Does                              |
//! | ----------------------------------- | --------------------------------- |
//! | `GET /api/v1/flows/{id}/schedules`  | a flow's schedules                |
//! | `POST /api/v1/flows/{id}/schedules` | create one, enabled               |
//! | `PATCH /api/v1/schedules/{id}`      | enable or disable it              |
//! | `DELETE /api/v1/schedules/{id}`     | delete it (its runs stay)         |

use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use orchestrator_postgres::{NewSchedule, Schedule, StoreError};
use schedule::{ScheduleSpec, ScheduleTrigger};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::people_http::{error, failed, Shared};
use crate::people_views::one;
use crate::{Clock, PeopleState, MANAGE_SCHEDULES, VIEW_FLOWS};

pub(crate) fn routes<C: Clock + Clone>(state: Arc<PeopleState<C>>) -> Router {
    Router::new()
        .route(
            "/api/v1/flows/{id}/schedules",
            get(list::<C>).post(create::<C>),
        )
        .route(
            "/api/v1/schedules/{id}",
            axum::routing::patch(enable::<C>).delete(delete::<C>),
        )
        .with_state(state)
}

fn schedule_json(s: &Schedule) -> Value {
    json!({
        "id": s.id, "flow_id": s.flow_id, "cron": s.spec.cron, "timezone": s.spec.timezone,
        "trigger": s.spec.trigger, "parameters": s.parameters, "timeout_secs": s.timeout_secs,
        "enabled": s.enabled, "next_fire_at": s.next_fire_at, "last_fire_at": s.last_fire_at,
        "kept_by_core": s.core_id.is_some(),
    })
}

/// Why the flow `id` cannot be used, if it cannot: a value that is no id
/// is not found.
async fn no_flow<C: Clock>(s: &PeopleState<C>, id: &str) -> Option<Response> {
    match s.port.store.flow(id).await {
        Ok(Some(_)) => None,
        Ok(None) | Err(StoreError::Invalid(_)) => {
            Some(error(StatusCode::NOT_FOUND, "no such flow"))
        }
        Err(e) => Some(failed(&e)),
    }
}

async fn list<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, VIEW_FLOWS).await {
        return refused.into_response();
    }
    if let Some(response) = no_flow(&s, &id).await {
        return response;
    }
    match s.port.store.schedules(&id).await {
        Ok(all) => {
            let all: Vec<Value> = all.iter().map(schedule_json).collect();
            Json(json!({ "schedules": all })).into_response()
        }
        Err(e) => failed(&e),
    }
}

/// A schedule as a person creates it: a cron or a trigger, in a zone.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    #[serde(default)]
    cron: String,
    timezone: String,
    #[serde(default)]
    trigger: Option<ScheduleTrigger>,
    #[serde(default)]
    parameters: Option<Value>,
    timeout_secs: Option<u32>,
}

async fn create<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Result<Json<CreateBody>, JsonRejection>,
) -> Response {
    let person = match s.people.allow(&headers, MANAGE_SCHEDULES).await {
        Ok(person) => person,
        Err(refused) => return refused.into_response(),
    };
    let Ok(Json(body)) = body else {
        return error(StatusCode::BAD_REQUEST, "not a schedule");
    };
    if let Some(response) = no_flow(&s, &id).await {
        return response;
    }
    let parameters = body.parameters.unwrap_or_else(|| json!({}));
    if !parameters.is_object() || body.timeout_secs == Some(0) {
        let why = "parameters: an object; timeout_secs: at least 1";
        return error(StatusCode::UNPROCESSABLE_ENTITY, why);
    }
    let cron = match body.trigger {
        Some(_) => String::new(),
        None => body.cron.split_whitespace().collect::<Vec<_>>().join(" "),
    };
    let spec = ScheduleSpec {
        cron,
        timezone: body.timezone,
        trigger: body.trigger,
    };
    let first = match spec.validate(&id, s.port.clock.now()) {
        Ok(first) => first,
        Err(e) => return error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
    };
    let new = NewSchedule {
        flow_id: id,
        spec,
        parameters,
        timeout_secs: body.timeout_secs,
        next_fire_at: Some(first),
        created_by: Some(person.id),
    };
    match s.port.store.create_schedule(&new).await {
        Ok(created) => (StatusCode::CREATED, Json(schedule_json(&created))).into_response(),
        Err(e) => failed(&e),
    }
}

/// A person's schedule by id: not found, or kept by the core (a 409).
async fn own<C: Clock>(s: &PeopleState<C>, id: &str) -> Result<Schedule, Box<Response>> {
    let found = s.port.store.schedule(id).await;
    let mut kept = None;
    let response = one(found, "schedule", |schedule| {
        if schedule.core_id.is_some() {
            return error(StatusCode::CONFLICT, "the core keeps this schedule");
        }
        kept = Some(schedule);
        StatusCode::OK.into_response()
    });
    kept.ok_or_else(|| Box::new(response))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnableBody {
    enabled: bool,
}

async fn enable<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Result<Json<EnableBody>, JsonRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, MANAGE_SCHEDULES).await {
        return refused.into_response();
    }
    let Ok(Json(body)) = body else {
        return error(StatusCode::BAD_REQUEST, "not a schedule's change");
    };
    let schedule = match own(&s, &id).await {
        Ok(schedule) => schedule,
        Err(response) => return *response,
    };
    let next = if body.enabled {
        match schedule.spec.next_after(&schedule.id, s.port.clock.now()) {
            Ok(Some(next)) => Some(next),
            Ok(None) | Err(_) => return error(StatusCode::CONFLICT, "it fires no more"),
        }
    } else {
        None
    };
    let changed = s.port.store.set_schedule_next(&id, next).await;
    one(changed, "schedule", |changed| {
        Json(schedule_json(&changed)).into_response()
    })
}

async fn delete<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, MANAGE_SCHEDULES).await {
        return refused.into_response();
    }
    if let Err(response) = own(&s, &id).await {
        return *response;
    }
    match s.port.store.delete_schedule(&id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => error(StatusCode::NOT_FOUND, "no such schedule"),
        Err(e) => failed(&e),
    }
}
