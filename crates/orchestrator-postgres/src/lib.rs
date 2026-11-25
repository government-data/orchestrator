//! The orchestrator's PostgreSQL adapter (decision 0011): its migrations,
//! and [`PgStore`], the store the server's use cases run on. The database is the
//! module's own, created by the agent at `module add` (quality-sdk decision
//! 0009); its migrations only add.

mod flows;
mod schedules;
mod store;
mod variables;

pub use flows::NewFlow;
pub use schedules::{NewSchedule, Schedule};
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
