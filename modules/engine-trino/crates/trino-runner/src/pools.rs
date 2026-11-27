//! engine-trino's slot pools (core decision 0090, point 6.3: the operator
//! hands out at most `R` rule statements at once, round-robin across
//! datasets). One pool, `rules`: the kinds that run as the job's `rules`
//! user, sized by the tier's `R` (`rules.hardConcurrencyLimit`, the
//! coordinator's `tiers.sh`), taking turns by `dataset`.

use engine_port::{Pools, SlotPool};

use crate::engine::KINDS;
use crate::ports::TrinoUser;

/// The pool of the kinds that run rule statements.
pub const RULES_POOL: &str = "rules";

/// The run parameter whose values take turns in the `rules` pool.
pub const FAIR_BY: &str = "dataset";

/// The tier's `R`: the rule statements Trino's `rules` group runs at once,
/// as the coordinator's `tiers.sh` renders it for `TRINO_TIER`.
pub fn rules_concurrency(tier: &str) -> Result<u32, String> {
    match tier {
        "xs" | "s" => Ok(1),
        "m" | "l" => Ok(2),
        _ => Err(format!("TRINO_TIER: xs, s, m or l, not {tier}")),
    }
}

/// The kinds whose statements run as the job's `rules` user.
#[must_use]
pub fn rules_kinds() -> Vec<String> {
    KINDS
        .iter()
        .filter(|k| TrinoUser::of_kind(k) == Some(TrinoUser::Rules))
        .map(|k| (*k).to_string())
        .collect()
}

/// The engine's pools for `tier`: the `rules` pool only.
pub fn pools(tier: &str) -> Result<Pools, String> {
    Ok(Pools {
        pools: vec![SlotPool {
            name: RULES_POOL.to_string(),
            size: rules_concurrency(tier)?,
            kinds: rules_kinds(),
            fair_by: Some(FAIR_BY.to_string()),
        }],
    })
}
