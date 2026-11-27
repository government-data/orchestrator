//! An engine's slot pools (core decision 0090, point 6.3: the operator
//! hands out at most `R` rule statements at once, round-robin across
//! datasets): which of its kinds share a pool, the pool's size, and the
//! run parameter whose values take turns. Sent by the engine module from
//! its own configuration, again whenever it changes; the orchestrator
//! names no pool.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{is_kind_name, Refused};

/// The most pools one engine and line declares.
pub const MAX_POOLS: usize = 16;

/// The largest pool.
pub const MAX_POOL_SIZE: u32 = 64;

/// One pool: at most `size` runs of `kinds` run at once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SlotPool {
    /// `[a-z][a-z0-9_-]`, at most 64 bytes.
    pub name: String,
    pub size: u32,
    pub kinds: Vec<String>,
    /// The run parameter (`[a-z_][a-z0-9_]`, at most 64) whose values take
    /// turns: the value served least recently goes first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fair_by: Option<String>,
}

/// Every pool of an engine and line; what is not listed is in no pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Pools {
    pub pools: Vec<SlotPool>,
}

fn is_pool_name(s: &str) -> bool {
    s.len() <= 64
        && s.starts_with(|c: char| c.is_ascii_lowercase())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn is_parameter(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

impl Pools {
    /// Checks the pools: at most [`MAX_POOLS`], names well formed and
    /// unique, sizes 1 to [`MAX_POOL_SIZE`], each kind well formed and in
    /// one pool only, `fair_by` a parameter's name.
    pub fn validate(&self) -> Result<(), Refused> {
        let invalid = |why: String| Err(Refused::Invalid(why));
        if self.pools.len() > MAX_POOLS {
            return invalid(format!("pools: at most {MAX_POOLS}"));
        }
        let (mut names, mut kinds) = (BTreeSet::new(), BTreeSet::new());
        for p in &self.pools {
            if !is_pool_name(&p.name) || !names.insert(p.name.as_str()) {
                return invalid(format!(
                    "pool {:?}: [a-z][a-z0-9_-], at most 64, once",
                    p.name
                ));
            }
            if !(1..=MAX_POOL_SIZE).contains(&p.size) {
                return invalid(format!("pool {:?}: size 1 to {MAX_POOL_SIZE}", p.name));
            }
            if p.kinds.is_empty() {
                return invalid(format!("pool {:?}: at least one kind", p.name));
            }
            for k in &p.kinds {
                if !is_kind_name(k) || !kinds.insert(k.as_str()) {
                    return invalid(format!("kind {k:?}: well formed, in one pool only"));
                }
            }
            if p.fair_by.as_deref().is_some_and(|f| !is_parameter(f)) {
                return invalid(format!("pool {:?}: fair_by is a parameter's name", p.name));
            }
        }
        Ok(())
    }
}
