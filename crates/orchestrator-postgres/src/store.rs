//! The store on PostgreSQL and its errors.

use std::fmt;

use sqlx::PgPool;

/// The orchestrator's store on its PostgreSQL database.
#[derive(Debug, Clone)]
pub struct PgStore {
    pub(crate) pool: PgPool,
}

impl PgStore {
    /// The store on a migrated database.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Why the store did not do what it was asked.
#[derive(Debug)]
pub enum StoreError {
    /// A name that must be unique is taken.
    Conflict(String),
    /// What was asked is not valid (a reference to nothing, a value out of
    /// range).
    Invalid(String),
    /// The lease named is not the run's any more: its holder lost it.
    Lease,
    /// A stored value the model does not read: a bug or a hand edit.
    Corrupt(String),
    /// The database failed.
    Database(sqlx::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict(what) => write!(f, "already exists: {what}"),
            Self::Invalid(why) => write!(f, "not valid: {why}"),
            Self::Lease => f.write_str("the lease is not this run's any more"),
            Self::Corrupt(what) => write!(f, "a stored value is not readable: {what}"),
            Self::Database(e) => write!(f, "database: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        let Some(db) = e.as_database_error() else {
            return Self::Database(e);
        };
        // Constraint names only: a constraint's message may quote a value.
        let constraint = db.constraint().unwrap_or("a constraint").to_string();
        match db.code().as_deref() {
            Some("23505") => Self::Conflict(constraint),
            Some("23503" | "23514" | "22P02") => Self::Invalid(constraint),
            _ => Self::Database(e),
        }
    }
}

pub(crate) fn corrupt(what: &str) -> StoreError {
    StoreError::Corrupt(what.to_string())
}
