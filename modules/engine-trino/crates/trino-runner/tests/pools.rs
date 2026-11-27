//! engine-trino's `rules` pool (core decision 0090, point 6.3): its kinds
//! are those run as the `rules` user, its size the tier's `R` as the
//! coordinator's `tiers.sh` renders it, and the fake orchestrator takes it
//! from the runner, again after a restart.

mod common;

use std::time::Duration;

use common::{FakeCore, FakeTrino};
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{NoGuard, Runner};
use serde_json::json;
use trino_runner::{pools, rules_concurrency, TrinoEngine, KINDS};

const TIERS: &str = include_str!("../../../trino/tiers.sh");

/// Each tier's `rules=N` in `tiers.sh`'s `case`.
fn rendered() -> Vec<(String, u32)> {
    TIERS
        .lines()
        .filter_map(|l| {
            let (tier, rest) = l.trim().split_once(") heap=")?;
            let n = rest.split_once("rules=")?.1.split_once(' ')?.0;
            Some((tier.to_string(), n.parse().ok()?))
        })
        .collect()
}

#[test]
fn every_tier_sizes_the_pool_as_tiers_sh_renders_r() {
    let tiers = rendered();
    assert_eq!(tiers.len(), 4, "xs, s, m and l");
    for (tier, r) in tiers {
        assert_eq!(rules_concurrency(&tier), Ok(r), "tier {tier}");
        assert_eq!(pools(&tier).unwrap().pools[0].size, r);
    }
    assert!(pools("xl").is_err());
}

#[test]
fn the_rules_pool_holds_the_kinds_run_as_the_rules_user_fair_by_dataset() {
    let p = pools("s").unwrap();
    assert_eq!(p.pools.len(), 1);
    let rules = &p.pools[0];
    assert_eq!(rules.name, "rules");
    assert_eq!(rules.fair_by.as_deref(), Some("dataset"));
    assert_eq!(
        rules.kinds,
        ["evaluate", "backfill", "rule_test", "retract", "slas"]
    );
    assert!(rules.kinds.iter().all(|k| KINDS.contains(&k.as_str())));
    p.validate().unwrap();
}

#[tokio::test]
async fn a_runner_sends_the_rules_pool_and_sends_it_again_after_a_restart() {
    let fake = FakeOrchestrator::default();
    let runner = || {
        let core = FakeCore::new(json!({}), vec![]);
        let engine = TrinoEngine::new(core, FakeTrino::default(), "484", 2)
            .polling_every(Duration::from_millis(10));
        Runner::new(engine, fake.clone(), NoGuard, "r")
    };
    let first = runner();
    first.register().await.unwrap();
    first.send_pools(&pools("m").unwrap()).await.unwrap();
    assert_eq!(fake.pools(), Some(pools("m").unwrap()));
    let restarted = runner();
    restarted.register().await.unwrap();
    restarted.send_pools(&pools("m").unwrap()).await.unwrap();
    assert_eq!(fake.pools(), Some(pools("m").unwrap()), "the same pools");
}
