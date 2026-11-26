//! Deploys: claimed with SKIP LOCKED by a runner of their engine and line
//! on their cluster, handed again to the runner that claimed them, and
//! finished once (the same finish again changes nothing).

mod common;

use common::{at, new_flow};
use engine_port::{DeployFinish, DeployState, ScheduleSource};
use orchestration_model::{EngineLine, EngineName, Flow, Line};
use orchestrator_postgres::{DeployEnd, NewDeploy, PgStore, StoreError};

fn engine() -> EngineLine {
    EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: Line::new("-").unwrap(),
    }
}

async fn flow(store: &PgStore) -> Flow {
    let folder = store.create_folder("Sales").await.unwrap();
    store
        .create_flow(&new_flow(&folder.id, "close"))
        .await
        .unwrap()
}

fn deploy_of(flow: &Flow, cluster: &str, minute: u32) -> NewDeploy {
    NewDeploy {
        flow_id: flow.id.clone(),
        cluster: cluster.into(),
        source_path: "/srv/flows/sales/close".into(),
        source_sha256: "ab".repeat(32),
        schedules: vec![ScheduleSource::Timer {
            unit: "close.timer".into(),
        }],
        queued_at: at(&format!("2026-11-25T10:{minute:02}:00Z")),
    }
}

fn succeeded() -> DeployFinish {
    DeployFinish {
        state: DeployState::Succeeded,
        path: Some("/srv/managed/sales/close".into()),
        version: Some("v-1".into()),
        sha256: Some("cd".repeat(32)),
        external_disabled: false,
    }
}

#[tokio::test]
async fn a_deploy_is_claimed_on_its_cluster_with_its_flow() {
    let db = common::database().await;
    let store = db.store();
    let flow = flow(&store).await;
    let now = at("2026-11-25T11:00:00Z");
    let first = store
        .create_deploy(&deploy_of(&flow, "machine-1", 1))
        .await
        .unwrap();
    let second = store
        .create_deploy(&deploy_of(&flow, "machine-1", 2))
        .await
        .unwrap();
    let e = engine();
    let other = store
        .claim_deploy(&e, "machine-2", "m2/r", now)
        .await
        .unwrap();
    assert!(other.is_none(), "another cluster gets nothing");

    let claimed = store
        .claim_deploy(&e, "machine-1", "m1/r1", now)
        .await
        .unwrap()
        .expect("the oldest");
    assert_eq!(claimed.deploy, first);
    assert_eq!(claimed.flow, flow.id);
    assert_eq!(claimed.folder.name, "Sales");
    assert_eq!(claimed.name, "close");
    assert_eq!(claimed.parameters["day"], "today");
    assert_eq!(claimed.on_interrupt.as_deref(), Some("repeat"));
    assert_eq!(claimed.schedules.len(), 1);

    let again = store
        .claim_deploy(&e, "machine-1", "m1/r1", now)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.deploy, first, "the same runner takes it up again");
    let next = store
        .claim_deploy(&e, "machine-1", "m1/r2", now)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.deploy, second, "another runner takes the next");
    assert!(store
        .claim_deploy(&e, "machine-1", "m1/r3", now)
        .await
        .unwrap()
        .is_none());

    let missing = NewDeploy {
        flow_id: "00000000-0000-4000-8000-000000000000".into(),
        ..deploy_of(&flow, "machine-1", 3)
    };
    assert!(matches!(
        store.create_deploy(&missing).await,
        Err(StoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn a_finish_is_recorded_once() {
    let db = common::database().await;
    let store = db.store();
    let flow = flow(&store).await;
    let now = at("2026-11-25T11:00:00Z");
    let id = store
        .create_deploy(&deploy_of(&flow, "machine-1", 1))
        .await
        .unwrap();
    let conflict = |r: Result<DeployEnd, StoreError>| matches!(r, Err(StoreError::Conflict(_)));
    assert!(
        conflict(
            store
                .finish_deploy(&id, "machine-1", &succeeded(), now)
                .await
        ),
        "not claimed yet"
    );
    store
        .claim_deploy(&engine(), "machine-1", "m1/r1", now)
        .await
        .unwrap()
        .unwrap();
    assert!(
        conflict(
            store
                .finish_deploy(&id, "machine-2", &succeeded(), now)
                .await
        ),
        "only from its cluster"
    );
    let end = store
        .finish_deploy(&id, "machine-1", &succeeded(), now)
        .await;
    assert_eq!(end.unwrap(), DeployEnd::Recorded);
    let later = at("2026-11-25T11:05:00Z");
    let end = store
        .finish_deploy(&id, "machine-1", &succeeded(), later)
        .await;
    assert_eq!(end.unwrap(), DeployEnd::Unchanged);
    let other = DeployFinish {
        external_disabled: true,
        ..succeeded()
    };
    assert!(conflict(
        store.finish_deploy(&id, "machine-1", &other, later).await
    ));

    let d = store.deploy(&id).await.unwrap().unwrap();
    assert_eq!(d.state, "succeeded");
    assert_eq!(d.path.as_deref(), Some("/srv/managed/sales/close"));
    assert_eq!(d.version.as_deref(), Some("v-1"));
    assert_eq!(d.external_disabled, Some(false));
    let finished_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT finished_at FROM deploys WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(finished_at, now, "the second finish changed nothing");
    assert!(store
        .claim_deploy(&engine(), "machine-1", "m1/r1", later)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn concurrent_claims_never_take_one_deploy_twice() {
    let db = common::database().await;
    let store = db.store();
    let flow = flow(&store).await;
    for minute in 0..4 {
        store
            .create_deploy(&deploy_of(&flow, "machine-1", minute))
            .await
            .unwrap();
    }
    let now = at("2026-11-25T11:00:00Z");
    let tasks: Vec<_> = (0..8)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                let runner = format!("m1/r{i}");
                store
                    .claim_deploy(&engine(), "machine-1", &runner, now)
                    .await
                    .unwrap()
                    .map(|c| c.deploy)
            })
        })
        .collect();
    let mut taken = Vec::new();
    for t in tasks {
        taken.extend(t.await.unwrap());
    }
    taken.sort();
    let before = taken.len();
    taken.dedup();
    assert_eq!(taken.len(), before, "no deploy handed out twice");
    assert!(taken.len() <= 4);
}
