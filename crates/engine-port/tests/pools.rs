//! An engine's slot pools: checked, unknown fields refused.

use engine_port::{Pools, SlotPool, MAX_POOL_SIZE};
use serde_json::json;

fn rules() -> SlotPool {
    SlotPool {
        name: "rules".into(),
        size: 1,
        kinds: vec!["evaluate".into(), "retract".into()],
        fair_by: Some("dataset".into()),
    }
}

#[test]
fn pools_are_checked() {
    let ok = Pools {
        pools: vec![rules()],
    };
    assert!(ok.validate().is_ok());
    assert!(Pools { pools: vec![] }.validate().is_ok(), "no pool at all");
    let bad = [
        SlotPool {
            name: "Rules".into(),
            ..rules()
        },
        SlotPool { size: 0, ..rules() },
        SlotPool {
            size: MAX_POOL_SIZE + 1,
            ..rules()
        },
        SlotPool {
            kinds: vec![],
            ..rules()
        },
        SlotPool {
            kinds: vec!["Evaluate".into()],
            ..rules()
        },
        SlotPool {
            fair_by: Some("data set".into()),
            ..rules()
        },
    ];
    for p in bad {
        assert!(
            Pools {
                pools: vec![p.clone()]
            }
            .validate()
            .is_err(),
            "{p:?}"
        );
    }
    let twice = Pools {
        pools: vec![
            rules(),
            SlotPool {
                name: "other".into(),
                ..rules()
            },
        ],
    };
    assert!(twice.validate().is_err(), "a kind in two pools");
    let same = Pools {
        pools: vec![
            rules(),
            SlotPool {
                kinds: vec!["slas".into()],
                ..rules()
            },
        ],
    };
    assert!(same.validate().is_err(), "a name twice");
}

#[test]
fn pools_read_from_the_wire() {
    let p: Pools = serde_json::from_value(json!({"pools": [
        {"name": "rules", "size": 2, "kinds": ["evaluate"]}]}))
    .unwrap();
    assert_eq!(p.pools[0].fair_by, None);
    assert!(serde_json::from_value::<Pools>(json!({"pools": [], "x": 1})).is_err());
}
