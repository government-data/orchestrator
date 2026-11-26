//! The deploy messages: each round-trips, a malformed finish is refused,
//! and unknown fields are refused on the wire.

use engine_port::*;
use serde_json::{json, Map};

fn claimed() -> DeployClaimed {
    DeployClaimed {
        deploy: "00000000-0000-4000-8000-0000000000d1".into(),
        flow: "00000000-0000-4000-8000-0000000000a1".into(),
        source_path: "/srv/flows/sales/close".into(),
        source_sha256: "ab".repeat(32),
        schedules: vec![
            ScheduleSource::Cron {
                file: "/etc/cron.d/flows".into(),
                line: 3,
            },
            ScheduleSource::Timer {
                unit: "close.timer".into(),
            },
        ],
        folder: FolderRecord {
            id: "00000000-0000-4000-8000-0000000000f1".into(),
            name: "Sales".into(),
        },
        name: "Close".into(),
        description: String::new(),
        kind: "flow".into(),
        parameters: Map::from_iter([("region".to_string(), json!("north"))]),
        timeout_secs: 3600,
        allow_overlap: false,
        on_interrupt: Some("repeat".into()),
    }
}

fn succeeded() -> DeployFinish {
    DeployFinish {
        state: DeployState::Succeeded,
        path: Some("/srv/managed/sales/close".into()),
        version: Some("v-abab".into()),
        sha256: Some("cd".repeat(32)),
        external_disabled: true,
    }
}

#[test]
fn deploy_messages_round_trip() {
    let c = claimed();
    let wire = serde_json::to_string(&c).unwrap();
    assert_eq!(serde_json::from_str::<DeployClaimed>(&wire).unwrap(), c);
    let f = succeeded();
    let wire = serde_json::to_value(&f).unwrap();
    assert_eq!(wire["state"], "succeeded");
    assert_eq!(serde_json::from_value::<DeployFinish>(wire).unwrap(), f);
    let failed = json!({
        "state": "failed", "path": null, "version": null, "sha256": null,
        "external_disabled": false
    });
    let failed: DeployFinish = serde_json::from_value(failed).unwrap();
    assert_eq!(failed.state.as_str(), "failed");
    assert!(failed.validate().is_ok());
}

#[test]
fn unknown_fields_and_states_are_refused_on_the_wire() {
    let mut wire = serde_json::to_value(succeeded()).unwrap();
    wire["counts"] = json!({});
    assert!(serde_json::from_value::<DeployFinish>(wire).is_err());
    let mut wire = serde_json::to_value(succeeded()).unwrap();
    wire["state"] = json!("cancelled");
    assert!(serde_json::from_value::<DeployFinish>(wire).is_err());
    let mut wire = serde_json::to_value(claimed()).unwrap();
    wire["content"] = json!("…");
    assert!(serde_json::from_value::<DeployClaimed>(wire).is_err());
}

#[test]
fn a_finish_names_what_it_placed_only_on_success() {
    assert!(succeeded().validate().is_ok());
    let without = |f: fn(&mut DeployFinish)| {
        let mut d = succeeded();
        f(&mut d);
        d.validate()
    };
    assert!(without(|d| d.path = None).is_err());
    assert!(without(|d| d.path = Some("relative/close".into())).is_err());
    assert!(without(|d| d.path = Some(format!("/{}", "x".repeat(MAX_PATH_BYTES)))).is_err());
    assert!(without(|d| d.version = None).is_err());
    assert!(without(|d| d.version = Some("x".repeat(MAX_VERSION_BYTES + 1))).is_err());
    assert!(without(|d| d.sha256 = Some("AB".repeat(32))).is_err());
    assert!(without(|d| d.sha256 = None).is_err());
    let failed_with_path = DeployFinish {
        state: DeployState::Failed,
        version: None,
        sha256: None,
        ..succeeded()
    };
    assert!(failed_with_path.validate().is_err());
}

#[test]
fn the_snapshot_has_the_deploy_messages() {
    let all = schemas();
    let finish = &all["messages"]["DeployFinish"];
    let states: Vec<&serde_json::Value> = finish["$defs"]["DeployState"]["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| &s["const"])
        .collect();
    assert_eq!(states, [&json!("succeeded"), &json!("failed")]);
    assert!(all["messages"]["DeployClaimed"].is_object());
}
