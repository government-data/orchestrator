//! The core starts a chain on demand (core decision 0090, step 19): _Run
//! now_ and the kinds a person asks for (a source's listing, a test of a
//! rule, a console query) reach the orchestrator as a call of the core's
//! machine on the `internal` entry point, with no token. The core's job id
//! is the chain's origin, so the same job asked twice starts one chain.
//!
//! | Call | Answers |
//! | --- | --- |
//! | `POST /internal/chains` `{"job_id", "steps": [{"kind", "parameters", "setup"?: {"key", "fingerprint"}}]}` | `201 {"chain_id"}` started; `200 {"chain_id"}` that job's chain, started before |
//!
//! A call from any other machine is `401`; malformed steps are `422`; with
//! no flow set for the core's chains, `503`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use engine_port::Refused;
use orchestration_model::{validate_steps, ChainStep, Trigger};
use orchestrator_postgres::NewChain;
use serde::Deserialize;
use serde_json::json;

use crate::{ApiError, Caller, Clock, EnginePort, Machine, PortError, PortState};

/// The prefix of a chain's origin for a job the core asked for.
pub const CORE_JOB_ORIGIN: &str = "core-job:";

/// Whether `id` is a core job's id: `[A-Za-z0-9_-]`, 1 to 128 bytes.
fn is_job_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The core's chain request.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainAsked {
    pub job_id: String,
    pub steps: Vec<ChainStep>,
}

/// A chain the core asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainStarted {
    pub chain_id: String,
    /// `false`: the job's chain was started before.
    pub new: bool,
}

impl<C: Clock> EnginePort<C> {
    /// Starts the chain of the core's job `asked`, by `machine` (the
    /// core's), on the core's flow; the same job's chain if it exists.
    /// `None` when no flow is set for the core's chains.
    pub async fn start_core_chain(
        &self,
        machine: &Machine,
        asked: ChainAsked,
    ) -> Result<Option<ChainStarted>, PortError> {
        let Some(flow_id) = &self.core_flow else {
            return Ok(None);
        };
        let invalid = |why: String| PortError::Refused(Refused::Invalid(why));
        if !is_job_id(&asked.job_id) {
            return Err(invalid("job_id: [A-Za-z0-9_-], 1 to 128".into()));
        }
        validate_steps(&asked.steps).map_err(invalid)?;
        let origin = format!("{CORE_JOB_ORIGIN}{}", asked.job_id);
        let chain = NewChain {
            flow_id: flow_id.clone(),
            origin: origin.clone(),
            trigger: Trigger::Manual(machine.0.clone()),
            steps: asked.steps,
            now: self.clock.now(),
        };
        if let Some(chain_id) = self.store.start_chain(&chain).await? {
            tracing::info!(chain = %chain_id, "the core started a chain");
            return Ok(Some(ChainStarted {
                chain_id,
                new: true,
            }));
        }
        let chain_id = self.store.chain_of_origin(&origin).await?;
        Ok(chain_id.map(|chain_id| ChainStarted {
            chain_id,
            new: false,
        }))
    }
}

/// The core's chain requests; `state.auth` admits the core's machine
/// alone ([`crate::CoreMachine`]).
pub fn chains_router<C: Clock + Clone>(state: Arc<PortState<C>>) -> Router {
    Router::new()
        .route("/internal/chains", post(start::<C>))
        .with_state(state)
}

async fn start<C: Clock>(
    State(s): State<Arc<PortState<C>>>,
    Caller(machine, _): Caller,
    Json(asked): Json<ChainAsked>,
) -> Result<Response, ApiError> {
    let Some(started) = s.port.start_core_chain(&machine, asked).await? else {
        let body = Json(json!({"error": "no flow is set for the core's chains"}));
        return Ok((StatusCode::SERVICE_UNAVAILABLE, body).into_response());
    };
    let status = if started.new {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let body = Json(json!({ "chain_id": started.chain_id }));
    Ok((status, body).into_response())
}
