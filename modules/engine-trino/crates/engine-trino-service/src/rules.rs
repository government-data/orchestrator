//! The rules service (decision 0011; core decision 0090, point 7.2): the
//! coordinator reads `GET /rules.json` every refresh period; each read
//! asks the core for its grants and merges them with the engine modules'
//! own. When the core does not answer, the answer is 503 and Trino fails
//! every query until it does: no stale grant is served.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use trino_rules::{document, Grant};

use crate::HttpCore;

/// Where the core's grants come from.
#[async_trait]
pub trait Grants: Send + Sync + 'static {
    async fn grants(&self) -> Result<Vec<Grant>, String>;
}

#[async_trait]
impl Grants for HttpCore {
    async fn grants(&self) -> Result<Vec<Grant>, String> {
        HttpCore::grants(self).await.map_err(|e| e.0)
    }
}

struct Rules<G> {
    core: G,
    modules: Vec<Grant>,
}

/// The service's routes: the document, and `/health`.
pub fn router<G: Grants>(core: G, modules: Vec<Grant>) -> Router {
    let state = Arc::new(Rules { core, modules });
    Router::new()
        .route("/rules.json", get(rules::<G>))
        .route("/health", get(|| async { "ok" }))
        .with_state(state)
}

async fn rules<G: Grants>(State(s): State<Arc<Rules<G>>>) -> Response {
    let grants = match s.core.grants().await {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(error = %e, "the core's grants could not be read");
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    let (doc, invalid) = document(&grants, &s.modules);
    if !invalid.is_empty() {
        tracing::warn!(left_out = invalid.len(), "grants left out of Trino's rules");
    }
    (
        [(header::CONTENT_TYPE, "application/json")],
        doc.to_string(),
    )
        .into_response()
}
