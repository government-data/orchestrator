//! engine-trino's job runner (decision 0011; core decision 0090, point 2
//! and step 20). This part: the core's signed statement bundle and its
//! verification, the check of each answer's shape, and the class of
//! Trino's errors. The engine that runs a bundle comes on top of it.

mod bundle;
mod check;

pub use bundle::{
    digest_hex, Bundle, BundleKey, BundleRefused, Expects, RunAs, SignedBundle, Statement,
};
pub use check::{class, fits, Answer, Class};
