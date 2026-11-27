//! What people read of flows (decision 0011, step 6): folders and flows,
//! behind `orchestrator:ViewFlows`.
//!
//! | Call                      | Does                                     |
//! | ------------------------- | ---------------------------------------- |
//! | `GET /api/v1/folders`     | the folders, by name                     |
//! | `GET /api/v1/flows`       | the flows, of a `folder` or all, by name |
//! | `GET /api/v1/flows/{id}`  | one flow                                 |

use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use orchestrator_postgres::StoreError;
use serde::Deserialize;
use serde_json::json;

use crate::people_http::{error, failed, Shared};
use crate::{Clock, PeopleState, VIEW_FLOWS};

pub(crate) fn routes<C: Clock + Clone>(state: Arc<PeopleState<C>>) -> Router {
    Router::new()
        .route("/api/v1/folders", get(folders::<C>))
        .route("/api/v1/flows", get(flows::<C>))
        .route("/api/v1/flows/{id}", get(flow::<C>))
        .with_state(state)
}

pub(crate) fn not_found(what: &str) -> Response {
    error(StatusCode::NOT_FOUND, &format!("no such {what}"))
}

/// A store's answer for one id, shown by `show`: a value that is no id is
/// not found.
pub(crate) fn one<T>(
    found: Result<Option<T>, StoreError>,
    what: &str,
    show: impl FnOnce(T) -> Response,
) -> Response {
    match found {
        Ok(Some(t)) => show(t),
        Ok(None) | Err(StoreError::Invalid(_)) => not_found(what),
        Err(e) => failed(&e),
    }
}

async fn folders<C: Clock>(State(s): Shared<C>, headers: HeaderMap) -> Response {
    if let Err(refused) = s.people.allow(&headers, VIEW_FLOWS).await {
        return refused.into_response();
    }
    match s.port.store.folders().await {
        Ok(folders) => Json(json!({ "folders": folders })).into_response(),
        Err(e) => failed(&e),
    }
}

#[derive(Debug, Deserialize)]
struct FlowsQuery {
    folder: Option<String>,
}

async fn flows<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    query: Result<Query<FlowsQuery>, QueryRejection>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, VIEW_FLOWS).await {
        return refused.into_response();
    }
    let Ok(Query(q)) = query else {
        return error(StatusCode::BAD_REQUEST, "not a flows query");
    };
    match s.port.store.flows(q.folder.as_deref()).await {
        Ok(flows) => Json(json!({ "flows": flows })).into_response(),
        Err(StoreError::Invalid(_)) => not_found("folder"),
        Err(e) => failed(&e),
    }
}

async fn flow<C: Clock>(
    State(s): Shared<C>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(refused) = s.people.allow(&headers, VIEW_FLOWS).await {
        return refused.into_response();
    }
    one(s.port.store.flow(&id).await, "flow", |flow| {
        Json(flow).into_response()
    })
}
