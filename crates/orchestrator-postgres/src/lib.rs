//! The orchestrator's PostgreSQL adapter (decision 0011): its migrations,
//! and [`PgStore`], the store the server's use cases run on. The database is the
//! module's own, created by the agent at `module add` (quality-sdk decision
//! 0009); its migrations only add.

mod adoption;
mod chains;
mod deploys;
mod engines;
mod flows;
mod found;
mod locks;
mod reaper;
mod removal;
mod runs;
mod schedules;
mod stats;
mod store;
mod variables;
mod windows;

pub use adoption::Adoption;
pub use chains::{ChainsMoved, NewChain};
pub use deploys::{Deploy, DeployEnd, NewDeploy};
pub use engines::{LiveRunners, RegisteredEngine};
pub use flows::NewFlow;
pub use found::ScanAsked;
pub use reaper::Cancelled;
pub use removal::OnLines;
pub use runs::{ClaimFor, NewRun, Renewal};
pub use schedules::{NewSchedule, Schedule};
pub use stats::{EndedRun, QueueStat};
pub(crate) use store::corrupt;
pub use store::{PgStore, StoreError};
pub use variables::{SecretInfo, Variable, VariableValue};

use sqlx::migrate::{MigrateError, Migrator};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
pub use sqlx::PgPool;

/// The migrations, embedded in the binary.
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Applies the migrations not applied yet; running it twice changes
/// nothing.
pub async fn migrate(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}

/// A pool of at most `max` connections with `options`.
pub async fn connect(options: PgConnectOptions, max: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max)
        .connect_with(options)
        .await
}
