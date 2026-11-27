//! The core's flow (decision 0011, ruling 97): the core makes, once, the
//! flow its schedules fire on and its chains start on, and reads it back.
//! Called as `/internal/chains` is, with the core's machine and its own
//! token ([`crate::CoreCaller`]).
//!
//! | Call | Answers |
//! | --- | --- |
//! | `POST /internal/core-flow` `{"engine", "line", "timeout_secs"}` | `201` made; `200` the core's flow, made before on that engine and line; `409` made before on another engine or line, or a person's flow has its name |
//! | `GET /internal/core-flow` | `200`; `404` none yet |
//!
//! Each answer with a flow is `{"flow_id", "engine", "line",
//! "timeout_secs"}`. The engine and line are free names, as any flow's.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use orchestration_model::{EngineLine, EngineName, Flow, Line};
use orchestrator_postgres::{NewCoreFlow, StoreError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{ApiError, Caller, Clock, PortError, PortState};

/// The longest a core's run may take: a day.
const MAX_TIMEOUT_SECS: u32 = 86_400;

/// The core's request for its flow.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreFlowAsked {
    pub engine: String,
    pub line: String,
    pub timeout_secs: u32,
}

fn answer(status: StatusCode, flow: &Flow) -> Response {
    let body = json!({
        "flow_id": flow.id,
        "engine": flow.engine.engine.as_str(),
        "line": flow.engine.line.as_str(),
        "timeout_secs": flow.timeout_secs,
    });
    (status, Json(body)).into_response()
}

fn refused(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

fn checked(asked: &CoreFlowAsked) -> Result<NewCoreFlow, String> {
    if !(1..=MAX_TIMEOUT_SECS).contains(&asked.timeout_secs) {
        return Err(format!("timeout_secs: 1 to {MAX_TIMEOUT_SECS}"));
    }
    let engine = EngineLine {
        engine: EngineName::new(&asked.engine).map_err(|e| format!("engine: {}", e.0))?,
        line: Line::new(&asked.line).map_err(|e| format!("line: {}", e.0))?,
    };
    Ok(NewCoreFlow {
        engine,
        timeout_secs: asked.timeout_secs,
    })
}

pub(crate) async fn make<C: Clock>(
    State(s): State<Arc<PortState<C>>>,
    Caller(..): Caller,
    Json(asked): Json<CoreFlowAsked>,
) -> Result<Response, ApiError> {
    let new = match checked(&asked) {
        Ok(new) => new,
        Err(why) => {
            return Ok(refused(
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({"error": why}),
            ))
        }
    };
    let (flow, made) = match s.port.store.make_core_flow(&new).await {
        Ok(done) => done,
        Err(StoreError::Conflict(_)) => {
            let why = "a flow of that folder and name exists and is not the core's";
            return Ok(refused(StatusCode::CONFLICT, json!({"error": why})));
        }
        Err(e) => return Err(PortError::from(e).into()),
    };
    if made {
        tracing::info!(flow = %flow.id, "the core made its flow");
        return Ok(answer(StatusCode::CREATED, &flow));
    }
    if flow.engine != new.engine {
        let why = "the core's flow runs on another engine or line";
        let body = json!({"error": why, "flow_id": flow.id});
        return Ok(refused(StatusCode::CONFLICT, body));
    }
    Ok(answer(StatusCode::OK, &flow))
}

pub(crate) async fn read<C: Clock>(
    State(s): State<Arc<PortState<C>>>,
    Caller(..): Caller,
) -> Result<Response, ApiError> {
    let store = &s.port.store;
    let flow = match store.core_flow().await.map_err(PortError::from)? {
        Some(id) => store.flow(&id).await.map_err(PortError::from)?,
        None => None,
    };
    Ok(match flow {
        Some(flow) => answer(StatusCode::OK, &flow),
        None => refused(
            StatusCode::NOT_FOUND,
            json!({"error": "the core has no flow yet"}),
        ),
    })
}
