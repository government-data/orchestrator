//! A run's lock messages: their names checked, unknown fields refused.

use engine_port::{is_lock_name, Lock, LockAnswer};
use serde_json::json;

#[test]
fn lock_names_are_checked() {
    for good in [
        "lake.quality_gold.rule_results",
        "gold",
        "lake:sales_bronze.t-1",
    ] {
        assert!(is_lock_name(good), "{good}");
    }
    for bad in ["", "Gold", "gold table", "gold;drop", &"a".repeat(201)] {
        assert!(!is_lock_name(bad), "{bad:?}");
        let lock = Lock {
            lease: "l".into(),
            name: bad.to_string(),
        };
        assert!(lock.validate().is_err());
    }
}

#[test]
fn locks_round_trip_and_refuse_unknown_fields() {
    let lock: Lock = serde_json::from_value(json!({"lease": "l-1", "name": "gold"})).unwrap();
    assert!(lock.validate().is_ok());
    assert!(serde_json::from_value::<Lock>(json!({"lease": "l", "name": "g", "x": 1})).is_err());
    let answer = serde_json::to_value(LockAnswer { granted: true }).unwrap();
    assert_eq!(answer, json!({"granted": true}));
}
