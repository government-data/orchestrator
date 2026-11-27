//! The engine port over HTTP, on the orchestrator's route of the `internal`
//! entry point. Who calls is the caller's machine, as an [`Authenticate`]
//! adapter reads it; the body of every call is one of `engine-port`'s
//! messages.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use engine_port::{
    Claim, DeployFinish, Descriptor, Finish, Found, Hold, Inventory, Lock, Logs, Release, Renew,
};
use orchestration_model::{EngineLine, EngineName, Line};
use serde_json::json;

use crate::{Clock, EnginePort, Machine, PortError};

/// Reads who calls the engine port from a request's headers: `None` when
/// the caller is not a machine the orchestrator serves.
#[async_trait]
pub trait Authenticate: Send + Sync + 'static {
    async fn machine(&self, headers: &HeaderMap) -> Option<Machine>;

    /// The engine module the call comes from, when the adapter knows it
    /// (its machine client's token).
    async fn module(&self, _headers: &HeaderMap) -> Option<String> {
        None
    }
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
pub struct Caller(pub Machine, pub Option<String>);

impl<C: Clock> FromRequestParts<Arc<PortState<C>>> for Caller {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<PortState<C>>,
    ) -> Result<Self, Self::Rejection> {
        match state.auth.machine(&parts.headers).await {
            Some(m) => Ok(Self(m, state.auth.module(&parts.headers).await)),
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
            PortError::Lease | PortError::NotTheirs => {
                error(StatusCode::CONFLICT, &port.to_string())
            }
            PortError::UnknownEngine => error(StatusCode::NOT_FOUND, &port.to_string()),
            PortError::Unavailable => error(StatusCode::SERVICE_UNAVAILABLE, &port.to_string()),
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
        .route(
            "/internal/engines/{engine}/{line}/deploys/claim",
            post(claim_deploy::<C>),
        )
        .route("/internal/engines/{engine}/{line}/found", put(found::<C>))
        .route(
            "/internal/engines/{engine}/{line}/inventory",
            put(inventory::<C>),
        )
        .route("/internal/engines/{engine}/hold", post(hold::<C>))
        .route("/internal/engines/{engine}/release", post(release::<C>))
        .route("/internal/removal", get(removal::<C>))
        .route("/internal/runs/{id}/renew", post(renew::<C>))
        .route("/internal/runs/{id}/logs", post(logs::<C>))
        .route("/internal/runs/{id}/finish", post(finish::<C>))
        .route("/internal/runs/{id}/locks", post(take_lock::<C>))
        .route("/internal/runs/{id}/locks/release", post(release_lock::<C>))
        .route("/internal/deploys/{id}/finish", post(finish_deploy::<C>))
        .with_state(state)
}

async fn register<C: Clock>(
    State(s): Shared<C>,
    Caller(m, module): Caller,
    Path((engine, line)): Path<(String, String)>,
    Json(d): Json<Descriptor>,
) -> Result<Response, ApiError> {
    let e = engine_line(&engine, &line)?;
    let answer = s.port.register(&m, module.as_deref(), &e, &d).await?;
    Ok(Json(answer).into_response())
}

async fn claim<C: Clock>(
    State(s): Shared<C>,
    Caller(m, _): Caller,
    Path((engine, line)): Path<(String, String)>,
    Json(c): Json<Claim>,
) -> Result<Response, ApiError> {
    let e = engine_line(&engine, &line)?;
    match s.port.claim(&m, &e, &c).await? {
        Some(run) => Ok(Json(run).into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

async fn claim_deploy<C: Clock>(
    State(s): Shared<C>,
    Caller(m, _): Caller,
    Path((engine, line)): Path<(String, String)>,
    Json(c): Json<Claim>,
) -> Result<Response, ApiError> {
    let e = engine_line(&engine, &line)?;
    match s.port.claim_deploy(&m, &e, &c).await? {
        Some(deploy) => Ok(Json(deploy).into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

async fn finish_deploy<C: Clock>(
    State(s): Shared<C>,
    Caller(m, _): Caller,
    Path(id): Path<String>,
    Json(f): Json<DeployFinish>,
) -> Result<StatusCode, ApiError> {
    s.port.finish_deploy(&m, &id, &f).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn found<C: Clock>(
    State(s): Shared<C>,
    Caller(m, _): Caller,
    Path((engine, line)): Path<(String, String)>,
    Json(f): Json<Found>,
) -> Result<Response, ApiError> {
    let e = engine_line(&engine, &line)?;
    Ok(Json(s.port.found(&m, &e, f).await?).into_response())
}

async fn inventory<C: Clock>(
    State(s): Shared<C>,
    Caller(m, _): Caller,
    Path((engine, line)): Path<(String, String)>,
    Json(i): Json<Inventory>,
) -> Result<Response, ApiError> {
    let e = engine_line(&engine, &line)?;
    Ok(Json(s.port.inventory(&m, &e, i).await?).into_response())
}

async fn renew<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Path(id): Path<String>,
    Json(r): Json<Renew>,
) -> Result<Response, ApiError> {
    Ok(Json(s.port.renew(&id, &r).await?).into_response())
}

async fn logs<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Path(id): Path<String>,
    Json(l): Json<Logs>,
) -> Result<StatusCode, ApiError> {
    s.port.logs(&id, &l).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn finish<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Path(id): Path<String>,
    Json(f): Json<Finish>,
) -> Result<StatusCode, ApiError> {
    s.port.finish(&id, &f).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn take_lock<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Path(id): Path<String>,
    Json(l): Json<Lock>,
) -> Result<Response, ApiError> {
    Ok(Json(s.port.take_lock(&id, &l).await?).into_response())
}

async fn release_lock<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Path(id): Path<String>,
    Json(l): Json<Lock>,
) -> Result<StatusCode, ApiError> {
    s.port.release_lock(&id, &l).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn hold<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Path(engine): Path<String>,
    Json(h): Json<Hold>,
) -> Result<Response, ApiError> {
    let e = engine_name(&engine)?;
    let answer = s.port.hold(&e, &h).await?;
    Ok(Json(answer).into_response())
}

async fn release<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Path(engine): Path<String>,
    Json(r): Json<Release>,
) -> Result<StatusCode, ApiError> {
    let e = engine_name(&engine)?;
    s.port.release(&e, &r).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Counts every request of `router`'s routes by its template and status.
pub fn counted(router: Router, metrics: Arc<crate::Metrics>) -> Router {
    router.route_layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let metrics = Arc::clone(&metrics);
            async move {
                let route = req
                    .extensions()
                    .get::<axum::extract::MatchedPath>()
                    .map_or_else(|| "unmatched".to_string(), |p| p.as_str().to_string());
                let res = next.run(req).await;
                let labels = crate::metrics::HttpLabels {
                    route,
                    status: res.status().as_u16(),
                };
                metrics.http_requests_total.get_or_create(&labels).inc();
                res
            }
        },
    ))
}

/// The check before a module's removal: `?module=<name>`.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct RemovalQuery {
    module: String,
}

async fn removal<C: Clock>(
    State(s): Shared<C>,
    Caller(..): Caller,
    Query(q): Query<RemovalQuery>,
) -> Result<Response, ApiError> {
    Ok(Json(s.port.removal(&q.module).await?).into_response())
}
