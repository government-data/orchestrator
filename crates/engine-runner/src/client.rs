//! The orchestrator, as a runner calls it: the engine port over HTTP.

use async_trait::async_trait;
use engine_port::{Claim, Claimed, Descriptor, Finish, Logs, Registered, Renew, Renewed};
use reqwest::StatusCode;

/// Why a call to the orchestrator failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// The lease is not the run's any more: stop the run, report nothing.
    Lease,
    /// The orchestrator refused the call (and why).
    Refused(u16, String),
    /// It could not be reached, or answered with a server error.
    Unreachable(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lease => f.write_str("the lease is not this run's any more"),
            Self::Refused(status, why) => write!(f, "refused ({status}): {why}"),
            Self::Unreachable(why) => write!(f, "orchestrator unreachable: {why}"),
        }
    }
}

impl std::error::Error for CallError {}

/// The engine port's calls.
#[async_trait]
pub trait Orchestrator: Send + Sync + 'static {
    async fn register(&self, d: &Descriptor) -> Result<Registered, CallError>;
    async fn claim(&self, d: &Descriptor, c: &Claim) -> Result<Option<Claimed>, CallError>;
    async fn renew(&self, run: &str, r: &Renew) -> Result<Renewed, CallError>;
    async fn logs(&self, run: &str, l: &Logs) -> Result<(), CallError>;
    async fn finish(&self, run: &str, f: &Finish) -> Result<(), CallError>;
}

/// The engine port over HTTP. The client carries the machine's identity
/// (its certificate, its token's header); `base` is the orchestrator's
/// route on the `internal` entry point.
#[derive(Debug, Clone)]
pub struct HttpOrchestrator {
    client: reqwest::Client,
    base: String,
}

impl HttpOrchestrator {
    #[must_use]
    pub fn new(client: reqwest::Client, base: &str) -> Self {
        Self {
            client,
            base: base.trim_end_matches('/').to_string(),
        }
    }

    async fn send<T: serde::Serialize + Sync>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response, CallError> {
        let res = self
            .client
            .request(method, format!("{}{path}", self.base))
            .json(body)
            .send()
            .await
            .map_err(|e| CallError::Unreachable(e.without_url().to_string()))?;
        let status = res.status();
        if status.is_success() {
            return Ok(res);
        }
        let why = res.text().await.unwrap_or_default();
        Err(match status {
            StatusCode::CONFLICT => CallError::Lease,
            s if s.is_server_error() => CallError::Unreachable(s.to_string()),
            s => CallError::Refused(s.as_u16(), why),
        })
    }

    async fn json<R: serde::de::DeserializeOwned>(res: reqwest::Response) -> Result<R, CallError> {
        res.json()
            .await
            .map_err(|e| CallError::Unreachable(e.without_url().to_string()))
    }
}

fn engine_path(d: &Descriptor) -> String {
    format!("/internal/engines/{}/{}", d.engine, d.line)
}

#[async_trait]
impl Orchestrator for HttpOrchestrator {
    async fn register(&self, d: &Descriptor) -> Result<Registered, CallError> {
        Self::json(self.send(reqwest::Method::PUT, &engine_path(d), d).await?).await
    }

    async fn claim(&self, d: &Descriptor, c: &Claim) -> Result<Option<Claimed>, CallError> {
        let path = format!("{}/claim", engine_path(d));
        let res = self.send(reqwest::Method::POST, &path, c).await?;
        if res.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        Self::json(res).await.map(Some)
    }

    async fn renew(&self, run: &str, r: &Renew) -> Result<Renewed, CallError> {
        let path = format!("/internal/runs/{run}/renew");
        Self::json(self.send(reqwest::Method::POST, &path, r).await?).await
    }

    async fn logs(&self, run: &str, l: &Logs) -> Result<(), CallError> {
        let path = format!("/internal/runs/{run}/logs");
        self.send(reqwest::Method::POST, &path, l).await.map(drop)
    }

    async fn finish(&self, run: &str, f: &Finish) -> Result<(), CallError> {
        let path = format!("/internal/runs/{run}/finish");
        self.send(reqwest::Method::POST, &path, f).await.map(drop)
    }
}
