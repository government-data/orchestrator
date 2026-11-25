//! The engine port over HTTP, on the orchestrator's route of the `internal`
//! entry point. Who calls is the caller's machine, as an [`Authenticate`]
//! adapter reads it; the body of every call is one of `engine-port`'s
//! messages.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use axum::{Json, Router};
use engine_port::{Claim, Descriptor, Finish, Hold, Logs, Release, Renew};
use orchestration_model::{EngineLine, EngineName, Line};
use serde_json::json;

use crate::{Clock, EnginePort, Machine, PortError};

/// Reads who calls the engine port from a request's headers: `None` when
/// the caller is not a machine the orchestrator serves.
#[async_trait]
pub trait Authenticate: Send + Sync + 'static {
    async fn machine(&self, headers: &HeaderMap) -> Option<Machine>;
}

/// What the engine port's handlers share.
pub struct PortState<C> {
    pub port: EnginePort<C>,
    pub auth: Arc<dyn Authenticate>,
}

impl<C> std::fmt::Debug for PortState<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortState").finish_non_exhaustive()
    }
}

type Shared<C> = State<Arc<PortState<C>>>;

/// The caller's machine, or 401.
#[derive(Debug)]
pub struct Caller(pub Machine);

impl<C: Clock> FromRequestParts<Arc<PortState<C>>> for Caller {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<PortState<C>>,
    ) -> Result<Self, Self::Rejection> {
        match state.auth.machine(&parts.headers).await {
            Some(m) => Ok(Self(m)),
            None => Err(ApiError::Unauthorized),
        }
    }
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// Why a call was not answered.
#[derive(Debug)]
pub enum ApiError {
    Port(Box<PortError>),
    /// The path names no engine or line that can exist.
    NotFound(String),
    Unauthorized,
}

impl From<PortError> for ApiError {
    fn from(e: PortError) -> Self {
        Self::Port(Box::new(e))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let port = match self {
            Self::Unauthorized => {
                return error(
                    StatusCode::UNAUTHORIZED,
                    "not a machine of this installation",
                )
            }
            Self::NotFound(why) => return error(StatusCode::NOT_FOUND, &why),
            Self::Port(e) => e,
        };
        match &*port {
            PortError::Refused(r) => error(StatusCode::UNPROCESSABLE_ENTITY, &r.to_string()),
            PortError::Lease => error(StatusCode::CONFLICT, &port.to_string()),
            PortError::UnknownEngine => error(StatusCode::NOT_FOUND, &port.to_string()),
            PortError::Store(e) => {
                tracing::error!(error = %e, "engine port: the store failed");
                error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
            }
        }
    }
}

fn engine_line(engine: &str, line: &str) -> Result<EngineLine, ApiError> {
    let bad = |e: orchestration_model::InvalidName| ApiError::NotFound(e.0);
    Ok(EngineLine {
        engine: EngineName::new(engine).map_err(bad)?,
        line: Line::new(line).map_err(bad)?,
    })
}

fn engine_name(engine: &str) -> Result<EngineName, ApiError> {
    EngineName::new(engine).map_err(|e| ApiError::NotFound(e.0))
}

/// The engine port's routes.
pub fn router<C: Clock + Clone>(state: Arc<PortState<C>>) -> Router {
    Router::new()
        .route("/internal/engines/{engine}/{line}", put(register::<C>))
        .route("/internal/engines/{engine}/{line}/claim", post(claim::<C>))
        .route("/internal/engines/{engine}/hold", post(hold::<C>))
        .route("/internal/engines/{engine}/release", post(release::<C>))
        .route("/internal/runs/{id}/renew", post(renew::<C>))
        .route("/internal/runs/{id}/logs", post(logs::<C>))
        .route("/internal/runs/{id}/finish", post(finish::<C>))
        .with_state(state)
}

async fn register<C: Clock>(
    State(s): Shared<C>,
    Caller(m): Caller,
    Path((engine, line)): Path<(String, String)>,
    Json(d): Json<Descriptor>,
) -> Result<Response, ApiError> {
    let e = engine_line(&engine, &line)?;
    let answer = s.port.register(&m, &e, &d).await?;
    Ok(Json(answer).into_response())
}

async fn claim<C: Clock>(
    State(s): Shared<C>,
    Caller(m): Caller,
    Path((engine, line)): Path<(String, String)>,
    Json(c): Json<Claim>,
) -> Result<Response, ApiError> {
    let e = engine_line(&engine, &line)?;
    match s.port.claim(&m, &e, &c).await? {
        Some(run) => Ok(Json(run).into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

async fn renew<C: Clock>(
    State(s): Shared<C>,
    Caller(_): Caller,
    Path(id): Path<String>,
    Json(r): Json<Renew>,
) -> Result<Response, ApiError> {
    Ok(Json(s.port.renew(&id, &r).await?).into_response())
}

async fn logs<C: Clock>(
    State(s): Shared<C>,
    Caller(_): Caller,
    Path(id): Path<String>,
    Json(l): Json<Logs>,
) -> Result<StatusCode, ApiError> {
    s.port.logs(&id, &l).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn finish<C: Clock>(
    State(s): Shared<C>,
    Caller(_): Caller,
    Path(id): Path<String>,
    Json(f): Json<Finish>,
) -> Result<StatusCode, ApiError> {
    s.port.finish(&id, &f).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn hold<C: Clock>(
    State(s): Shared<C>,
    Caller(_): Caller,
    Path(engine): Path<String>,
    Json(h): Json<Hold>,
) -> Result<Response, ApiError> {
    let e = engine_name(&engine)?;
    let answer = s.port.hold(&e, &h).await?;
    Ok(Json(answer).into_response())
}

async fn release<C: Clock>(
    State(s): Shared<C>,
    Caller(_): Caller,
    Path(engine): Path<String>,
    Json(r): Json<Release>,
) -> Result<StatusCode, ApiError> {
    let e = engine_name(&engine)?;
    s.port.release(&e, &r).await?;
    Ok(StatusCode::NO_CONTENT)
}
