//! The orchestrator, as a runner calls it: the engine port over HTTP.

use std::sync::Arc;

use async_trait::async_trait;
use engine_port::{
    Claim, Claimed, DeployClaimed, DeployFinish, Descriptor, Finish, Found, FoundAnswer, Inventory,
    InventoryAnswer, Lock, LockAnswer, Logs, Pools, Registered, Renew, Renewed,
};
use reqwest::StatusCode;

/// Why a call to the orchestrator failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// The lease is not the run's any more: stop the run, report nothing.
    /// For a deploy: it is not this machine's, or it ended otherwise.
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
    /// What the runner found on its cluster; replaces its last report.
    async fn found(&self, d: &Descriptor, f: &Found) -> Result<FoundAnswer, CallError>;
    /// The records deploys left on the cluster, at start.
    async fn inventory(&self, d: &Descriptor, i: &Inventory) -> Result<InventoryAnswer, CallError>;
    /// A deploy of the runner's engine and line on its cluster, or none:
    /// the one this runner claimed and did not finish first.
    async fn claim_deploy(
        &self,
        d: &Descriptor,
        c: &Claim,
    ) -> Result<Option<DeployClaimed>, CallError>;
    /// A deploy's end; the same end again changes nothing.
    async fn finish_deploy(&self, deploy: &str, f: &DeployFinish) -> Result<(), CallError>;

    /// Takes a lock for a running run (core decision 0090, point 6.2.4).
    /// An orchestrator that does not serve locks refuses it.
    async fn take_lock(&self, run: &str, l: &Lock) -> Result<LockAnswer, CallError> {
        let _ = (run, l);
        Err(not_served())
    }

    /// Gives a run's lock back.
    async fn release_lock(&self, run: &str, l: &Lock) -> Result<(), CallError> {
        let _ = (run, l);
        Err(not_served())
    }

    /// The engine's slot pools (core decision 0090, point 6.3); replaces
    /// the last ones sent.
    async fn set_pools(&self, d: &Descriptor, p: &Pools) -> Result<(), CallError> {
        let _ = (d, p);
        Err(not_served())
    }
}

fn not_served() -> CallError {
    CallError::Refused(404, "not served by this orchestrator".into())
}

/// A running run's locks, under its lease, for its [`crate::RunContext`].
#[async_trait]
pub(crate) trait RunLocks: Send + Sync + std::fmt::Debug {
    async fn take(&self, name: &str) -> Result<bool, CallError>;
    async fn give_back(&self, name: &str) -> Result<(), CallError>;
}

/// [`RunLocks`] through the orchestrator, with the run's id and lease.
pub(crate) struct LeasedLocks<O> {
    pub(crate) orchestrator: Arc<O>,
    pub(crate) run: String,
    pub(crate) lease: String,
}

impl<O> std::fmt::Debug for LeasedLocks<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeasedLocks")
            .field("run", &self.run)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<O: Orchestrator> RunLocks for LeasedLocks<O> {
    async fn take(&self, name: &str) -> Result<bool, CallError> {
        let l = Lock {
            lease: self.lease.clone(),
            name: name.to_string(),
        };
        Ok(self.orchestrator.take_lock(&self.run, &l).await?.granted)
    }

    async fn give_back(&self, name: &str) -> Result<(), CallError> {
        let l = Lock {
            lease: self.lease.clone(),
            name: name.to_string(),
        };
        self.orchestrator.release_lock(&self.run, &l).await
    }
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

    async fn found(&self, d: &Descriptor, f: &Found) -> Result<FoundAnswer, CallError> {
        let path = format!("{}/found", engine_path(d));
        Self::json(self.send(reqwest::Method::PUT, &path, f).await?).await
    }

    async fn inventory(&self, d: &Descriptor, i: &Inventory) -> Result<InventoryAnswer, CallError> {
        let path = format!("{}/inventory", engine_path(d));
        Self::json(self.send(reqwest::Method::PUT, &path, i).await?).await
    }

    async fn claim_deploy(
        &self,
        d: &Descriptor,
        c: &Claim,
    ) -> Result<Option<DeployClaimed>, CallError> {
        let path = format!("{}/deploys/claim", engine_path(d));
        let res = self.send(reqwest::Method::POST, &path, c).await?;
        if res.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        Self::json(res).await.map(Some)
    }

    async fn finish_deploy(&self, deploy: &str, f: &DeployFinish) -> Result<(), CallError> {
        let path = format!("/internal/deploys/{deploy}/finish");
        self.send(reqwest::Method::POST, &path, f).await.map(drop)
    }

    async fn take_lock(&self, run: &str, l: &Lock) -> Result<LockAnswer, CallError> {
        let path = format!("/internal/runs/{run}/locks");
        Self::json(self.send(reqwest::Method::POST, &path, l).await?).await
    }

    async fn release_lock(&self, run: &str, l: &Lock) -> Result<(), CallError> {
        let path = format!("/internal/runs/{run}/locks/release");
        self.send(reqwest::Method::POST, &path, l).await.map(drop)
    }

    async fn set_pools(&self, d: &Descriptor, p: &Pools) -> Result<(), CallError> {
        let path = format!("{}/pools", engine_path(d));
        self.send(reqwest::Method::PUT, &path, p).await.map(drop)
    }
}
