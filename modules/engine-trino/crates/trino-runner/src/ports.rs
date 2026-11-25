//! What the job runner calls: the core (bundles, the job's tokens, a
//! source's secrets) and Trino. Adapters live in the runner's binary; the
//! tests use fakes.

use async_trait::async_trait;

use crate::bundle::{Keys, RunAs, SignedBundle};
use crate::check::Answer;

/// The core could not answer (a class, never a body).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreError(pub String);

/// A source's secret, sent as one extra credential (core decision 0090,
/// point 7.5): the current value and, for one retry on a refused login,
/// the previous one.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    pub name: String,
    pub current: String,
    pub previous: Option<String>,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// What the runner asks the core for a run's bundle: the job the operator
/// handed out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRequest {
    pub kind: String,
    pub dataset: Option<String>,
    pub cut: Option<String>,
    pub attempt: u32,
}

/// The core, over mTLS with the runner's machine identity.
#[async_trait]
pub trait Core: Send + Sync + 'static {
    /// The run's signed bundle: compiled at the first ask, the same one at
    /// every later ask for the run.
    async fn bundle(&self, run: &str, job: &JobRequest) -> Result<SignedBundle, CoreError>;
    /// The provider's public keys (its JWKS).
    async fn keys(&self) -> Result<Keys, CoreError>;
    /// A Trino token for the job, as its own user or its report user.
    async fn token(&self, job: &str, run_as: RunAs) -> Result<String, CoreError>;
    /// The secrets of the source the job reads (none for most kinds).
    async fn credentials(&self, job: &str) -> Result<Vec<Credential>, CoreError>;
}

/// Why Trino gave no answer: its error's kind (`trino_client::TrinoError::
/// kind`), never its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoFailure {
    pub kind: String,
}

/// Trino's coordinator.
#[async_trait]
pub trait Trino: Send + Sync + 'static {
    /// Runs `sql`; `started` gets the query id before the next page.
    async fn run(
        &self,
        token: &str,
        sql: &str,
        extra: &[(String, String)],
        started: &(dyn for<'q> Fn(&'q str) + Send + Sync),
    ) -> Result<Answer, TrinoFailure>;
    /// Ends the query `id`.
    async fn kill(&self, token: &str, id: &str) -> Result<(), TrinoFailure>;
    /// Whether the query `id` is gone or done.
    async fn ended(&self, token: &str, id: &str) -> Result<bool, TrinoFailure>;
}
