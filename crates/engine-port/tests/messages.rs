//! The engine port's run messages: each round-trips, the orchestrator
//! refuses what is malformed, and the schemas match the committed snapshot.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use engine_port::*;
use orchestration_model::{EngineName, FinalState, Interruption, Line};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn descriptor() -> Descriptor {
    Descriptor {
        engine: EngineName::new("engine-a").unwrap(),
        line: Line::new("-").unwrap(),
        engine_version: "1.2.3".into(),
        port: PORT,
        kinds: vec![RunKind {
            name: "load".into(),
            parameters: json!({"type": "object"}),
        }],
        concurrency: 2,
    }
}

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: &T) {
    let wire = serde_json::to_string(value).unwrap();
    let back: T = serde_json::from_str(&wire).unwrap();
    assert_eq!(&back, value, "{wire}");
}

#[test]
fn every_message_round_trips() {
    round_trip(&descriptor());
    round_trip(&Registered::for_port(PORT));
    round_trip(&Claim {
        runner: "r-1".into(),
    });
    round_trip(&Claimed {
        run: "run-1".into(),
        flow: "flow-1".into(),
        kind: "load".into(),
        parameters: json!({"day": "2026-11-25"}),
        attempt: 2,
        timeout_secs: 600,
        lease: "lease-1".into(),
        lease_until: at("2026-11-25T10:02:00Z"),
        resume: Some(Resume {
            previous_run: "run-0".into(),
            engine_ref: Some("ref-0".into()),
            last_detail: None,
        }),
    });
    round_trip(&Renew {
        lease: "lease-1".into(),
        progress: Some(Progress {
            fraction: Some(0.5),
            detail: Some("step 2".into()),
        }),
        engine_ref: Some("ref-1".into()),
    });
    for action in [
        RenewAction::Continue,
        RenewAction::Cancel,
        RenewAction::Held,
    ] {
        round_trip(&Renewed {
            action,
            lease_until: at("2026-11-25T10:02:30Z"),
        });
    }
    round_trip(&Logs {
        lease: "lease-1".into(),
        lines: vec![LogLine {
            seq: 1,
            stream: LogStream::System,
            line: "started".into(),
        }],
    });
    round_trip(&Finish {
        lease: "lease-1".into(),
        state: FinalState::Failed,
        counts: BTreeMap::from([("statements".into(), 3)]),
        error_class: Some("out_of_memory".into()),
        interruption: Some(Interruption::System),
    });
    round_trip(&Hold {
        hold: "h-1".into(),
        until: at("2026-11-25T10:30:00Z"),
    });
    round_trip(&HoldAnswer {
        state: HoldState::Draining,
        running: 1,
    });
    round_trip(&Release { hold: "h-1".into() });
}

#[test]
fn the_wire_names_are_snake_case() {
    let renewed = Renewed {
        action: RenewAction::Held,
        lease_until: at("2026-11-25T10:02:30Z"),
    };
    assert_eq!(serde_json::to_value(renewed).unwrap()["action"], "held");
    let answer = HoldAnswer {
        state: HoldState::Free,
        running: 0,
    };
    assert_eq!(
        serde_json::to_value(answer).unwrap(),
        json!({"state": "free", "running": 0})
    );
}

#[test]
fn unknown_fields_and_lost_are_refused_on_the_wire() {
    let mut wire = serde_json::to_value(Claim { runner: "r".into() }).unwrap();
    wire["extra"] = json!(1);
    assert!(serde_json::from_value::<Claim>(wire).is_err());
    let lost = json!({"lease": "l", "state": "lost", "error_class": null, "interruption": null});
    assert!(
        serde_json::from_value::<Finish>(lost).is_err(),
        "lost is the orchestrator's"
    );
}

#[test]
fn logs_and_finishes_are_checked() {
    let line = |seq| LogLine {
        seq,
        stream: LogStream::Stdout,
        line: "x".into(),
    };
    let logs = |lines| Logs {
        lease: "l".into(),
        lines,
    };
    assert!(logs(vec![line(1), line(2)]).validate().is_ok());
    assert!(logs(vec![line(2), line(2)]).validate().is_err());
    assert!(logs(vec![line(0)]).validate().is_err());
    let long = LogLine {
        line: "x".repeat(MAX_LOG_LINE_BYTES + 1),
        ..line(1)
    };
    assert!(logs(vec![long]).validate().is_err());
    assert!(logs((1..=MAX_LOG_LINES as u64 + 1).map(line).collect())
        .validate()
        .is_err());

    let finish = |state, class: Option<&str>| Finish {
        lease: "l".into(),
        state,
        counts: BTreeMap::new(),
        error_class: class.map(str::to_string),
        interruption: None,
    };
    assert!(finish(FinalState::Succeeded, None).validate().is_ok());
    assert!(finish(FinalState::Failed, Some("syntax"))
        .validate()
        .is_ok());
    assert!(finish(FinalState::Succeeded, Some("syntax"))
        .validate()
        .is_err());
    assert!(finish(FinalState::Failed, Some("Has a message"))
        .validate()
        .is_err());
    let mut counted = finish(FinalState::Succeeded, None);
    counted.counts.insert("Rows read".into(), 1);
    assert!(counted.validate().is_err());
}

#[test]
fn the_schemas_match_the_snapshot() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/schemas/engine-port-1.json");
    let now = serde_json::to_string_pretty(&schemas()).unwrap() + "\n";
    if std::env::var_os("UPDATE_SNAPSHOT").is_some() {
        std::fs::write(path, &now).unwrap();
    }
    let committed = std::fs::read_to_string(path).expect("run with UPDATE_SNAPSHOT=1 once");
    assert_eq!(
        committed, now,
        "the port's schemas changed: a breaking change is a new port"
    );
    let all: Value = serde_json::from_str(&committed).unwrap();
    let finish = &all["messages"]["Finish"]["properties"]["state"]["enum"];
    assert_eq!(
        finish,
        &json!(["succeeded", "failed", "timed_out", "cancelled"])
    );
}
