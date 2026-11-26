//! The import and re-attach messages: each round-trips, a bad item is
//! named, and an inventory keeps the latest version of each flow.

use chrono::{DateTime, Utc};
use engine_port::*;
use serde_json::{json, Map};

fn artefact(path: &str, segments: &[&str]) -> FoundArtefact {
    FoundArtefact {
        path: path.into(),
        segments: segments.iter().map(|s| (*s).to_string()).collect(),
        sha256: "ab".repeat(32),
        size_bytes: 10,
        flags: vec![Flag {
            kind: FlagKind::Code,
            subject: "Script (#3)".into(),
        }],
    }
}

fn schedule() -> FoundSchedule {
    FoundSchedule {
        source: ScheduleSource::Cron {
            file: "/etc/cron.d/flows".into(),
            line: 3,
        },
        user: "svc".into(),
        calendar: FoundCalendar::Cron("0 6 * * 1-5".into()),
        timezone: Some("America/Bogota".into()),
        artefact: "/srv/flows/sales/close".into(),
        parameters: Map::from_iter([("region".to_string(), json!("north"))]),
        persistent: false,
    }
}

fn record(flow: &str, at: i64) -> ArtefactRecord {
    ArtefactRecord {
        format: RECORD_FORMAT,
        flow: flow.into(),
        deploy: "00000000-0000-4000-8000-0000000000d1".into(),
        version: "v-abab".into(),
        sha256: "ab".repeat(32),
        folder: FolderRecord {
            id: "00000000-0000-4000-8000-0000000000f1".into(),
            name: "Sales".into(),
        },
        name: "Close".into(),
        description: String::new(),
        kind: "flow".into(),
        parameters: Map::new(),
        timeout_secs: 3600,
        allow_overlap: false,
        on_interrupt: None,
        deployed_at: DateTime::<Utc>::from_timestamp(at, 0).unwrap(),
    }
}

fn item(path: &str, flow: &str, at: i64) -> InventoryItem {
    InventoryItem {
        path: path.into(),
        record: record(flow, at),
    }
}

const FLOW_1: &str = "00000000-0000-4000-8000-000000000001";
const FLOW_2: &str = "00000000-0000-4000-8000-000000000002";

#[test]
fn the_messages_round_trip() {
    let found = Found {
        artefacts: vec![artefact("/srv/flows/sales/close", &["sales", "close"])],
        schedules: vec![
            schedule(),
            FoundSchedule {
                source: ScheduleSource::Timer {
                    unit: "close.timer".into(),
                },
                calendar: FoundCalendar::Unsupported("boot_relative".into()),
                persistent: true,
                ..schedule()
            },
        ],
    };
    let wire = serde_json::to_value(&found).unwrap();
    assert_eq!(wire["schedules"][0]["source"]["kind"], "cron");
    assert_eq!(wire["schedules"][1]["calendar"]["calendar"], "unsupported");
    assert_eq!(serde_json::from_value::<Found>(wire).unwrap(), found);
    let inventory = Inventory {
        items: vec![item("/srv/flows/sales/close_abab", FLOW_1, 10)],
    };
    let wire = serde_json::to_string(&inventory).unwrap();
    assert_eq!(serde_json::from_str::<Inventory>(&wire).unwrap(), inventory);
    let answer = InventoryAnswer {
        skipped: vec![Skipped {
            path: "/x".into(),
            reason: SkipReason::OnLiveCluster,
        }],
        ..InventoryAnswer::default()
    };
    let wire = serde_json::to_value(&answer).unwrap();
    assert_eq!(wire["skipped"][0]["reason"], "on_live_cluster");
}

#[test]
fn a_found_item_out_of_bounds_is_named() {
    assert_eq!(artefact("/srv/a", &["a"]).problem(), None);
    assert!(artefact("srv/a", &["a"]).problem().is_some(), "relative");
    assert!(artefact("/srv/a", &[]).problem().is_some());
    assert!(artefact("/srv/a", &["..", "a"]).problem().is_some());
    assert!(artefact("/srv/a", &["a/b"]).problem().is_some());
    let mut upper = artefact("/srv/a", &["a"]);
    upper.sha256 = "AB".repeat(32);
    assert!(upper.problem().is_some());
    assert_eq!(schedule().problem(), None);
    let mut six = schedule();
    six.calendar = FoundCalendar::Cron("0 0 6 * * 1".into());
    assert!(six.problem().is_some());
    let mut unit = schedule();
    unit.source = ScheduleSource::Timer {
        unit: "close.service".into(),
    };
    assert!(unit.problem().is_some());
    let too_many = Found {
        artefacts: vec![artefact("/a", &["a"]); MAX_FOUND + 1],
        ..Found::default()
    };
    assert!(too_many.validate().is_err());
}

#[test]
fn the_name_proposed_is_the_folders_after_the_first() {
    assert_eq!(artefact("/a", &["Close"]).proposed_name(), "Close");
    assert_eq!(artefact("/a", &["Sales", "Close"]).proposed_name(), "Close");
    let deep = artefact("/a", &["Sales", "North", "Close"]);
    assert_eq!(deep.proposed_name(), "North - Close");
}

#[test]
fn an_inventory_keeps_the_latest_version_of_each_flow() {
    let mut bad = item("/srv/bad", FLOW_2, 1);
    bad.record.format = RECORD_FORMAT + 1;
    let (latest, skipped) = Inventory {
        items: vec![
            item("/srv/close_a", FLOW_1, 10),
            item("/srv/close_b", FLOW_1, 30),
            item("/srv/other", FLOW_2, 5),
            item("/srv/close_c", FLOW_1, 20),
            bad,
        ],
    }
    .latest();
    let paths = |v: Vec<&str>| v.into_iter().map(str::to_string).collect::<Vec<_>>();
    let got: Vec<String> = latest.iter().map(|i| i.path.clone()).collect();
    assert_eq!(got, paths(vec!["/srv/close_b", "/srv/other"]));
    let reasons: Vec<(String, SkipReason)> =
        skipped.into_iter().map(|s| (s.path, s.reason)).collect();
    assert_eq!(
        reasons,
        vec![
            ("/srv/close_a".into(), SkipReason::OlderVersion),
            ("/srv/close_c".into(), SkipReason::OlderVersion),
            ("/srv/bad".into(), SkipReason::Invalid),
        ]
    );
}

#[test]
fn a_record_without_what_registers_its_flow_is_invalid() {
    assert!(record(FLOW_1, 1).is_valid());
    let cases: [fn(&mut ArtefactRecord); 6] = [
        |r| r.flow = "not-a-uuid".into(),
        |r| r.folder.name = " ".into(),
        |r| r.kind = "Load".into(),
        |r| r.timeout_secs = 0,
        |r| r.on_interrupt = Some("later".into()),
        |r| r.sha256 = "ab".into(),
    ];
    for (i, change) in cases.iter().enumerate() {
        let mut r = record(FLOW_1, 1);
        change(&mut r);
        assert!(!r.is_valid(), "case {i}");
    }
    let wire = json!({"format": 1, "flow": FLOW_1, "secret": "x"});
    assert!(serde_json::from_value::<ArtefactRecord>(wire).is_err());
}
