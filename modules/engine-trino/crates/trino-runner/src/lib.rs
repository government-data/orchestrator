//! engine-trino's job runner (decision 0011; core decision 0090, point 2
//! and step 20): the core's signed statement bundle and its verification,
//! the check of each answer's shape, the class of Trino's errors, and
//! [`TrinoEngine`], engine-runner's `Engine` that runs a bundle through
//! the [`Core`] and [`Trino`] ports.

mod bundle;
mod check;
mod engine;
mod ports;
mod report;
mod run;
mod steps;

pub use bundle::{
    bundle_kid, digest_hex, Bundle, BundleRefused, Expects, Keys, OnlyIfRows, Placeholder,
    PlaceholderType, RunAs, SignedBundle, Source, Statement, BUNDLE_TYP, MAX_VALUES_ROWS,
};
pub use check::{class, fits, Answer, Class};
pub use engine::{parameters, TrinoEngine, KINDS};
pub use ports::{
    Core, CoreError, Credential, Delivery, JobRequest, Trino, TrinoFailure, TrinoUser,
};
pub use report::{ReportState, RunReport, SnapshotFilled, StatementReport, StatementState};
