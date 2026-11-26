//! A new cluster adopts its inventory once: a known flow on a gone machine
//! is re-attached with its runs, schedules and variables; an unknown one is
//! registered with its same id; skipped items carry their reason; never two
//! records for one flow id.

mod common;

use std::collections::HashSet;

use common::{at, descriptor, new_flow};
use engine_port::{ArtefactRecord, FolderRecord, InventoryItem, SkipReason, Skipped};
use orchestration_model::{EngineLine, EngineName, Line, Trigger};
use orchestrator_postgres::{Adoption, NewRun, PgStore, StoreError};
use serde_json::{json, Map};

const RUNNER: &str = "machine-new/an-engine/-";

fn engine() -> EngineLine {
    EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: Line::new("-").unwrap(),
    }
}

fn id(n: u8) -> String {
    format!("00000000-0000-4000-8000-0000000000{n:02}")
}

fn item(path: &str, flow: &str, folder: (&str, &str), name: &str) -> InventoryItem {
    InventoryItem {
        path: path.into(),
        record: ArtefactRecord {
            format: 1,
            flow: flow.into(),
            deploy: id(99),
            version: "v3".into(),
            sha256: "ab".repeat(32),
            folder: FolderRecord {
                id: folder.0.into(),
                name: folder.1.into(),
            },
            name: name.into(),
            description: String::new(),
            kind: "load".into(),
            parameters: Map::from_iter([("day".to_string(), json!("today"))]),
            timeout_secs: 900,
            allow_overlap: false,
            on_interrupt: Some("repeat".into()),
            deployed_at: at("2026-11-20T10:00:00Z"),
        },
    }
}

async fn adopt(
    store: &PgStore,
    live: &[&str],
    items: &[InventoryItem],
) -> Option<engine_port::InventoryAnswer> {
    let live: HashSet<String> = live.iter().map(|m| (*m).to_string()).collect();
    let engine = engine();
    let a = Adoption {
        runner: RUNNER,
        cluster: "machine-new",
        engine: &engine,
        live: &live,
        now: at("2026-11-26T10:00:00Z"),
    };
    store.adopt_inventory(&a, items).await.unwrap()
}

/// A flow placed on `cluster`, with a queued run, a schedule and a variable.
async fn placed(store: &PgStore, folder: &str, name: &str, cluster: &str) -> String {
    let mut new = new_flow(folder, name);
    new.cluster = Some(cluster.into());
    let flow = store.create_flow(&new).await.unwrap();
    let run = NewRun {
        flow_id: flow.id.clone(),
        engine: engine(),
        kind: "load".into(),
        cluster: Some(cluster.into()),
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: json!({}),
        timeout_secs: 600,
        lock_name: None,
        slot_pool: None,
        queued_at: at("2026-11-25T10:00:00Z"),
    };
    store.enqueue(&run).await.unwrap().unwrap();
    sqlx::query(
        "INSERT INTO schedules (flow_id, cron, timezone) VALUES ($1::uuid, '0 6 * * *', 'UTC')",
    )
    .bind(&flow.id)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO flow_variables (flow_id, name, value) VALUES ($1::uuid, 'region', '\"north\"')")
        .bind(&flow.id)
        .execute(store.pool())
        .await
        .unwrap();
    flow.id
}

async fn records(store: &PgStore, flow: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM flows WHERE id = $1::uuid")
        .bind(flow)
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn count(store: &PgStore, sql: &str, flow: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(flow)
        .fetch_one(store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_new_cluster_reattaches_known_flows_and_registers_unknown_ones_once() {
    let db = common::database().await;
    let store = db.store();
    store
        .register(
            &descriptor("an-engine", "-"),
            RUNNER,
            at("2026-11-26T09:59:00Z"),
        )
        .await
        .unwrap();
    let sales = store.create_folder("Sales").await.unwrap();
    let known = placed(&store, &sales.id, "close", "machine-gone").await;
    let unknown = id(1);
    let by_name = id(2);
    let items = [
        item(
            "/srv/flows/sales/close",
            &known,
            (&id(50), "Other"),
            "close",
        ),
        item("/srv/flows/hr/pay", &unknown, (&id(51), "HR"), "pay"),
        item(
            "/srv/flows/sales/open",
            &by_name,
            (&id(52), "Sales"),
            "open",
        ),
    ];
    assert!(store.adopts_inventory(RUNNER).await.unwrap());

    let answer = adopt(&store, &["machine-new"], &items).await.unwrap();
    assert!(!answer.already_adopted);
    assert_eq!(answer.reattached, std::slice::from_ref(&known));
    assert_eq!(answer.registered, [unknown.clone(), by_name.clone()]);
    assert_eq!(
        answer.folders_created, 1,
        "HR created again, Sales found by name"
    );
    assert!(answer.skipped.is_empty());

    let flow = store.flow(&known).await.unwrap().unwrap();
    assert_eq!(flow.cluster.as_deref(), Some("machine-new"));
    assert_eq!(flow.folder_id, sales.id, "its folder kept");
    let queued_here =
        "SELECT count(*) FROM runs WHERE flow_id = $1::uuid AND cluster = 'machine-new'";
    assert_eq!(
        count(&store, queued_here, &known).await,
        1,
        "its queued run moves"
    );
    assert_eq!(store.schedules(&known).await.unwrap().len(), 1);
    assert_eq!(store.variables(&known).await.unwrap().len(), 1);
    let pay = store.flow(&unknown).await.unwrap().unwrap();
    assert_eq!(pay.folder_id, id(51), "the folder created with its id");
    assert_eq!(pay.cluster.as_deref(), Some("machine-new"));
    assert_eq!(pay.timeout_secs, 900);
    let open = store.flow(&by_name).await.unwrap().unwrap();
    assert_eq!(open.folder_id, sales.id);

    assert!(!store.adopts_inventory(RUNNER).await.unwrap());
    store
        .register(
            &descriptor("an-engine", "-"),
            RUNNER,
            at("2026-11-26T11:00:00Z"),
        )
        .await
        .unwrap();
    let again = [item("/srv/flows/x", &id(3), (&id(53), "X"), "x")];
    assert_eq!(adopt(&store, &[], &again).await, None, "adopted once");
    assert!(
        store.flow(&id(3)).await.unwrap().is_none(),
        "nothing changed"
    );
    for flow in [&known, &unknown, &by_name] {
        assert_eq!(records(&store, flow).await, 1);
    }
}

#[tokio::test]
async fn skipped_items_carry_their_reason() {
    let db = common::database().await;
    let store = db.store();
    store
        .register(
            &descriptor("an-engine", "-"),
            RUNNER,
            at("2026-11-26T09:59:00Z"),
        )
        .await
        .unwrap();
    let f = store.create_folder("Sales").await.unwrap();
    let live = placed(&store, &f.id, "live", "machine-live").await;
    let anywhere = store
        .create_flow(&new_flow(&f.id, "anywhere"))
        .await
        .unwrap()
        .id;
    let mut other_line = new_flow(&f.id, "other-line");
    other_line.engine.line = Line::new("2").unwrap();
    other_line.cluster = Some("machine-gone".into());
    let other_line = store.create_flow(&other_line).await.unwrap().id;
    let items = [
        item("/srv/a", &live, (&f.id, "Sales"), "live"),
        item("/srv/b", &anywhere, (&f.id, "Sales"), "anywhere"),
        item("/srv/c", &other_line, (&f.id, "Sales"), "other-line"),
        item("/srv/d", &id(1), (&f.id, "Sales"), "new"),
        item("/srv/d", &id(2), (&f.id, "Sales"), "also-new"),
        item("/srv/e", &id(3), (&f.id, "Sales"), "live"),
    ];
    let answer = adopt(&store, &["machine-new", "machine-live"], &items)
        .await
        .unwrap();
    let skipped = |path: &str, reason| Skipped {
        path: path.into(),
        reason,
    };
    assert_eq!(answer.registered, [id(1)]);
    assert_eq!(
        answer.skipped,
        [
            skipped("/srv/a", SkipReason::OnLiveCluster),
            skipped("/srv/b", SkipReason::OnLiveCluster),
            skipped("/srv/c", SkipReason::Invalid),
            skipped("/srv/d", SkipReason::PathRegistered),
            skipped("/srv/e", SkipReason::Invalid),
        ]
    );
    let flow = store.flow(&live).await.unwrap().unwrap();
    assert_eq!(
        flow.cluster.as_deref(),
        Some("machine-live"),
        "a live cluster keeps it"
    );
    for flow in [id(2), id(3)] {
        assert_eq!(records(&store, &flow).await, 0);
    }
}

#[tokio::test]
async fn a_purged_flow_comes_back_over_its_same_record() {
    let db = common::database().await;
    let store = db.store();
    store
        .register(
            &descriptor("an-engine", "-"),
            RUNNER,
            at("2026-11-26T09:59:00Z"),
        )
        .await
        .unwrap();
    let f = store.create_folder("Sales").await.unwrap();
    let flow = placed(&store, &f.id, "close", "machine-gone").await;
    sqlx::query("DELETE FROM flows WHERE id = $1::uuid")
        .bind(&flow)
        .execute(store.pool())
        .await
        .unwrap();
    let items = [item("/srv/close", &flow, (&f.id, "Sales"), "close")];
    let answer = adopt(&store, &[], &items).await.unwrap();
    assert_eq!(answer.registered, std::slice::from_ref(&flow));
    assert_eq!(records(&store, &flow).await, 1);
}

#[tokio::test]
async fn a_runner_never_registered_adopts_nothing() {
    let db = common::database().await;
    let store = db.store();
    assert!(matches!(
        store.adopts_inventory(RUNNER).await,
        Err(StoreError::Invalid(_))
    ));
    let live = HashSet::new();
    let engine = engine();
    let a = Adoption {
        runner: RUNNER,
        cluster: "machine-new",
        engine: &engine,
        live: &live,
        now: at("2026-11-26T10:00:00Z"),
    };
    assert!(matches!(
        store.adopt_inventory(&a, &[]).await,
        Err(StoreError::Invalid(_))
    ));
}
