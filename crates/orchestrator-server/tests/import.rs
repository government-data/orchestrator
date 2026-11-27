//! The import: the chosen found artefacts become flows of their cluster,
//! in their folders, with a deploy that carries their external schedules;
//! an unconfirmed flag, an unknown path or kind, a duplicate and a second
//! import are skipped with their reason; never two flows for one artefact.

mod common;

use axum::http::{Method, StatusCode};
use common::{engine, harness};
use engine_port::{Flag, FlagKind, ScheduleSource};
use orchestrator_server::{
    Clock, EnginePort, ImportError, ImportItem, Machine, Skipped, SkippedItem, MAX_IMPORT,
};
use serde_json::json;

const RUNNER: &str = "machine-1/fake-engine/-";

fn code(subject: &str) -> Flag {
    Flag {
        kind: FlagKind::Code,
        subject: subject.into(),
    }
}

fn item(path: &str, folder: &str, name: &str, flags: Vec<Flag>) -> ImportItem {
    ImportItem {
        path: path.into(),
        folder: folder.into(),
        name: name.into(),
        kind: "job".into(),
        timeout_secs: 600,
        confirmed_flags: flags,
    }
}

async fn found(h: &common::Harness) {
    h.register().await;
    let artefact = |path: &str, flags: serde_json::Value| {
        json!({"path": path, "segments": ["x"], "sha256": "ab".repeat(32), "size_bytes": 1,
               "flags": flags})
    };
    let report = json!({
        "artefacts": [
            artefact("/srv/flows/close", json!([{"kind": "code", "subject": "Script (#3)"}])),
            artefact("/srv/flows/open", json!([])),
            artefact("/srv/flows/flagged", json!([{"kind": "path", "subject": "/tmp"}])),
        ],
        "schedules": [{
            "source": {"kind": "cron", "file": "/etc/cron.d/flows", "line": 2},
            "user": "svc", "calendar": {"calendar": "cron", "value": "0 6 * * 1-5"},
            "timezone": "UTC", "artefact": "/srv/flows/close"
        }]
    });
    let path = "/internal/engines/fake-engine/-/found";
    let (status, _) = h.call(Method::PUT, path, Some("machine-1"), report).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn an_import_loads_the_chosen_artefacts_with_their_folder_and_confirmed_flags() {
    let h = harness().await;
    found(&h).await;
    let port = EnginePort::new(h.store.clone(), h.clock.clone());
    let machine = Machine("machine-1".into());
    let mut unknown_kind = item("/srv/flows/open", "Ops", "Open", vec![]);
    unknown_kind.kind = "nothing".into();
    let items = [
        item(
            "/srv/flows/close",
            "Sales",
            "Close",
            vec![code("Script (#3)")],
        ),
        item("/srv/flows/flagged", "Ops", "Flagged", vec![]),
        item(
            "/srv/flows/flagged",
            "Ops",
            "Flagged",
            vec![code("Script (#3)")],
        ),
        item("/srv/flows/missing", "Ops", "Missing", vec![]),
        unknown_kind,
        item(
            "/srv/flows/close",
            "Sales",
            "Again",
            vec![code("Script (#3)")],
        ),
        item("/srv/flows/open", "Sales", "Close", vec![]),
    ];
    let answer = port.import(&machine, &engine(), &items).await.unwrap();
    let skipped = |path: &str, reason| SkippedItem {
        path: path.into(),
        reason,
    };
    assert_eq!(
        answer.skipped,
        [
            skipped("/srv/flows/flagged", Skipped::FlagsNotConfirmed),
            skipped("/srv/flows/flagged", Skipped::Duplicate),
            skipped("/srv/flows/missing", Skipped::NotFound),
            skipped("/srv/flows/open", Skipped::UnknownKind),
            skipped("/srv/flows/close", Skipped::Duplicate),
            skipped("/srv/flows/open", Skipped::Duplicate),
        ]
    );
    assert_eq!(answer.imported.len(), 1);
    let done = &answer.imported[0];
    let flow = h.store.flow(&done.flow).await.unwrap().unwrap();
    assert_eq!((flow.name.as_str(), flow.kind.as_str()), ("Close", "job"));
    assert_eq!(flow.cluster.as_deref(), Some("machine-1"));
    let folders = h.store.folders().await.unwrap();
    assert_eq!(
        folders
            .iter()
            .find(|f| f.id == flow.folder_id)
            .unwrap()
            .name,
        "Sales"
    );
    let deploy = h.store.deploy(&done.deploy).await.unwrap().unwrap();
    assert_eq!(
        (deploy.flow_id.as_str(), deploy.state.as_str()),
        (flow.id.as_str(), "queued")
    );
    let claimed = h
        .store
        .claim_deploy(
            &engine(),
            "machine-1",
            &format!("{RUNNER}/r1"),
            h.clock.now(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.source_path, "/srv/flows/close");
    assert_eq!(
        claimed.schedules,
        [ScheduleSource::Cron {
            file: "/etc/cron.d/flows".into(),
            line: 2
        }]
    );
}

#[tokio::test]
async fn a_second_import_changes_nothing() {
    let h = harness().await;
    found(&h).await;
    let port = EnginePort::new(h.store.clone(), h.clock.clone());
    let machine = Machine("machine-1".into());
    let first = [item(
        "/srv/flows/close",
        "Sales",
        "Close",
        vec![code("Script (#3)")],
    )];
    assert_eq!(
        port.import(&machine, &engine(), &first)
            .await
            .unwrap()
            .imported
            .len(),
        1
    );
    let skipped = |path: &str, reason| SkippedItem {
        path: path.into(),
        reason,
    };
    let again = [
        item(
            "/srv/flows/close",
            "Other",
            "Close 2",
            vec![code("Script (#3)")],
        ),
        item("/srv/flows/open", "Sales", "Close", vec![]),
    ];
    let answer = port.import(&machine, &engine(), &again).await.unwrap();
    assert!(answer.imported.is_empty());
    assert_eq!(
        answer.skipped,
        [
            skipped("/srv/flows/close", Skipped::AlreadyImported),
            skipped("/srv/flows/open", Skipped::NameTaken),
        ]
    );
    assert_eq!(
        h.store.flows(None).await.unwrap().len(),
        1,
        "one flow, once"
    );
}

#[tokio::test]
async fn an_import_takes_one_to_five_hundred_of_a_registered_engine() {
    let h = harness().await;
    found(&h).await;
    let port = EnginePort::new(h.store.clone(), h.clock.clone());
    let machine = Machine("machine-1".into());
    let one = item("/srv/flows/open", "Ops", "Open", vec![]);
    let many = vec![one.clone(); MAX_IMPORT + 1];
    for items in [&[][..], &many[..]] {
        let refused = port.import(&machine, &engine(), items).await;
        assert!(matches!(refused, Err(ImportError::Count)));
    }
    let mut other = engine();
    other.line = orchestration_model::Line::new("2").unwrap();
    let refused = port
        .import(&machine, &other, std::slice::from_ref(&one))
        .await;
    assert!(matches!(refused, Err(ImportError::UnknownEngine)));
    let at_most = vec![one; MAX_IMPORT];
    let answer = port.import(&machine, &engine(), &at_most).await.unwrap();
    assert_eq!(answer.imported.len(), 1);
    assert_eq!(answer.skipped.len(), MAX_IMPORT - 1, "the rest duplicates");
}
