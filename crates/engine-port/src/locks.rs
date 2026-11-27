//! A run's locks (core decision 0090, points 4 and 6.2.4): one writer at a
//! time per named lock, taken by a running run just before the writes that
//! need it and released after them, or when the run ends. The orchestrator
//! names no lock: the engine's runner says which.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Refused;

/// The longest lock name, in bytes.
pub const MAX_LOCK_NAME_BYTES: usize = 200;

/// Whether `name` is a lock's name: `[a-z0-9_.:-]`, 1 to 200 bytes.
#[must_use]
pub fn is_lock_name(name: &str) -> bool {
    (1..=MAX_LOCK_NAME_BYTES).contains(&name.len())
        && name.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '.' | ':' | '-')
        })
}

/// A run asks for a lock, or gives it back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Lock {
    pub lease: String,
    pub name: String,
}

impl Lock {
    /// Checks the lock's name.
    pub fn validate(&self) -> Result<(), Refused> {
        if is_lock_name(&self.name) {
            Ok(())
        } else {
            Err(Refused::Invalid(format!(
                "name: [a-z0-9_.:-], 1 to {MAX_LOCK_NAME_BYTES} bytes"
            )))
        }
    }
}

/// Whether the run holds the lock now. Not granted: another running run
/// holds it; ask again later, the run's lease renewed meanwhile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LockAnswer {
    pub granted: bool,
}
