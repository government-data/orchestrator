//! What the job runner calls: the core (bundles, the job's tokens, a
//! source's secrets) and Trino. Adapters live in the runner's binary; the
//! tests use fakes.

use async_trait::async_trait;

use crate::bundle::{Keys, SignedBundle};
use crate::check::Answer;
use crate::report::RunReport;

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
    /// The source a source's job reads (a `tables` listing).
    pub source: Option<String>,
    pub attempt: u32,
}

/// A job's Trino user, as the core's token route names it
/// (`job_<user>_<dataset or source>`, core decision 0090, point 7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoUser {
    /// Reads its dataset's layers and writes gold and the scratch rows.
    Rules,
    /// Reads gold and its tables' `$refs`: a statement `run_as` report.
    Report,
    /// Prepares and maintains its dataset's bronze and silver.
    Maint,
    /// Reads its source and loads its dataset's bronze.
    Extract,
    /// Reads one source's catalog: a `tables` listing, a `map` profile or
    /// a page of `records`.
    Source,
    /// Reads its dataset's silver for samples (a `test_rule`).
    Sample,
}

impl TrinoUser {
    /// Its name on the core's token route.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rules => "rules",
            Self::Report => "report",
            Self::Maint => "maint",
            Self::Extract => "extract",
            Self::Source => "source",
            Self::Sample => "sample",
        }
    }

    /// The user a job of `kind` runs its own statements as; `None` for a
    /// kind the core compiles no Trino bundle for.
    #[must_use]
    pub fn of_kind(kind: &str) -> Option<Self> {
        match kind {
            "evaluate" | "backfill" | "rule_test" | "retract" | "slas" => Some(Self::Rules),
            "prepare" => Some(Self::Maint),
            "extract" => Some(Self::Extract),
            "tables" | "map" | "records" => Some(Self::Source),
            "test_rule" => Some(Self::Sample),
            _ => None,
        }
    }
}

/// What the core did with a run's report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// Kept now, or kept already by an earlier send.
    Kept,
    /// Refused for good (its error, as the core names it).
    Refused(String),
}

/// The core, over mTLS with the runner's machine identity.
#[async_trait]
pub trait Core: Send + Sync + 'static {
    /// The run's signed bundle: compiled at the first ask, the same one at
    /// every later ask for the run.
    async fn bundle(&self, run: &str, job: &JobRequest) -> Result<SignedBundle, CoreError>;
    /// The provider's public keys (its JWKS).
    async fn keys(&self) -> Result<Keys, CoreError>;
    /// A Trino token for the job as one of its users.
    async fn token(&self, job: &str, user: TrinoUser) -> Result<String, CoreError>;
    /// The secrets of the source the job reads (none for most kinds).
    async fn credentials(&self, job: &str) -> Result<Vec<Credential>, CoreError>;
    /// The rows of the values read `statement` (ruling 79), kept by the
    /// core before the run reports; an error is a core that did not answer.
    async fn rows(
        &self,
        job: &str,
        statement: &str,
        rows: &[Vec<serde_json::Value>],
    ) -> Result<Delivery, CoreError>;
    /// The run's report (ruling 75); an error is a core that did not
    /// answer, and the report is sent again.
    async fn report(&self, job: &str, report: &RunReport) -> Result<Delivery, CoreError>;
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
