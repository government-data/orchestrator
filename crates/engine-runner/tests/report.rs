//! A runner reports its inventory and what it found through the engine
//! port; an engine without artefacts reports neither.

use async_trait::async_trait;
use engine_port::{
    ArtefactRecord, Claimed, Descriptor, FolderRecord, Found, FoundArtefact, Inventory,
    InventoryItem, RunKind, PORT, RECORD_FORMAT,
};
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{Engine, EngineError, NoGuard, Outcome, Refused, RunContext, RunRef, Runner};
use orchestration_model::{EngineName, Line};
use serde_json::{json, Map};

const FLOW: &str = "00000000-0000-4000-8000-000000000001";

struct Fake {
    artefacts: bool,
}

#[async_trait]
impl Engine for Fake {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            engine: EngineName::new("engine-a").unwrap(),
            line: Line::new("-").unwrap(),
            engine_version: "1.0.0".into(),
            port: PORT,
            kinds: vec![RunKind {
                name: "job".into(),
                parameters: json!({"type": "object"}),
            }],
            concurrency: 1,
        }
    }

    async fn validate(&self, _: &Claimed) -> Result<(), Refused> {
        Ok(())
    }

    async fn run(&self, _: Claimed, _: RunContext) -> Outcome {
        Outcome::succeeded()
    }

    async fn cancel(&self, _: &RunRef) -> Result<(), EngineError> {
        Ok(())
    }

    async fn found(&self) -> Option<Found> {
        self.artefacts.then(|| Found {
            artefacts: vec![FoundArtefact {
                path: "/srv/flows/sales/close".into(),
                segments: vec!["sales".into(), "close".into()],
                sha256: "ab".repeat(32),
                size_bytes: 10,
                flags: vec![],
            }],
            schedules: vec![],
        })
    }

    async fn inventory(&self) -> Option<Inventory> {
        self.artefacts.then(|| Inventory {
            items: vec![InventoryItem {
                path: "/srv/flows/sales/close_abab".into(),
                record: ArtefactRecord {
                    format: RECORD_FORMAT,
                    flow: FLOW.into(),
                    deploy: "00000000-0000-4000-8000-0000000000d1".into(),
                    version: "v-abab".into(),
                    sha256: "ab".repeat(32),
                    folder: FolderRecord {
                        id: "00000000-0000-4000-8000-0000000000f1".into(),
                        name: "Sales".into(),
                    },
                    name: "Close".into(),
                    description: String::new(),
                    kind: "job".into(),
                    parameters: Map::new(),
                    timeout_secs: 60,
                    allow_overlap: false,
                    on_interrupt: None,
                    deployed_at: chrono::DateTime::from_timestamp(10, 0).unwrap(),
                },
            }],
        })
    }
}

#[tokio::test]
async fn a_runner_reports_its_inventory_once_and_what_it_found() {
    let fake = FakeOrchestrator::default();
    let runner = Runner::new(Fake { artefacts: true }, fake.clone(), NoGuard, "r-1");
    runner.register().await.unwrap();
    let first = runner.report_inventory().await.unwrap().unwrap();
    assert!(!first.already_adopted);
    assert_eq!(first.registered, [FLOW]);
    let again = runner.report_inventory().await.unwrap().unwrap();
    assert!(again.already_adopted && again.registered.is_empty());
    assert_eq!(runner.report_found().await.unwrap().unwrap().kept, 1);
    assert_eq!(fake.found().artefacts[0].path, "/srv/flows/sales/close");
}

#[tokio::test]
async fn an_engine_without_artefacts_reports_nothing() {
    let fake = FakeOrchestrator::default();
    let runner = Runner::new(Fake { artefacts: false }, fake.clone(), NoGuard, "r-1");
    assert_eq!(runner.report_inventory().await.unwrap(), None);
    assert_eq!(runner.report_found().await.unwrap(), None);
    assert_eq!(fake.found(), Found::default());
}
