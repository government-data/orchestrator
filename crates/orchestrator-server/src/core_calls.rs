//! What the core asks the orchestrator on its route of the `internal`
//! entry point, with its machine certificate and no token (quality-sdk
//! decisions 0002 and 0015): the drain before an update and the check
//! before a module's removal. Both answer counts only. They are served
//! apart from the engine port, whose calls also need an engine module's
//! machine token, so the state given here authenticates the machine
//! alone.
//!
//! | Call | Answers |
//! | --- | --- |
//! | `GET /internal/drain?horizon_secs=N` | `{"running": n, "scheduled_soon": n}` |
//! | `GET /internal/removal?module=<name>` | `{"refuse": false}` or a refusal with counts |

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::{ApiError, Caller, Clock, EnginePort, PortError, PortState};

/// The orchestrator's work for the core's drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DrainAnswer {
    pub running: u32,
    pub scheduled_soon: u32,
}

impl<C: Clock> EnginePort<C> {
    /// The runs running now, and the queued runs and schedule fires due
    /// within `horizon_secs`.
    pub async fn drain(&self, horizon_secs: u32) -> Result<DrainAnswer, PortError> {
        let horizon = Duration::seconds(i64::from(horizon_secs));
        let c = self.store.drain_counts(self.clock.now(), horizon).await?;
        Ok(DrainAnswer {
            running: c.running,
            scheduled_soon: c.scheduled_soon,
        })
    }
}

/// The core's calls; `state.auth` reads the calling machine only.
pub fn core_router<C: Clock + Clone>(state: Arc<PortState<C>>) -> Router {
    Router::new()
        .route("/internal/drain", get(drain::<C>))
        .route("/internal/removal", get(removal::<C>))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct DrainQuery {
    horizon_secs: u32,
}

async fn drain<C: Clock>(
    State(s): State<Arc<PortState<C>>>,
    Caller(..): Caller,
    Query(q): Query<DrainQuery>,
) -> Result<Response, ApiError> {
    Ok(Json(s.port.drain(q.horizon_secs).await?).into_response())
}

#[derive(Debug, Deserialize)]
struct RemovalQuery {
    module: String,
}

async fn removal<C: Clock>(
    State(s): State<Arc<PortState<C>>>,
    Caller(..): Caller,
    Query(q): Query<RemovalQuery>,
) -> Result<Response, ApiError> {
    Ok(Json(s.port.removal(&q.module).await?).into_response())
}
