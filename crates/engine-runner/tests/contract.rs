//! The engine port's contract suite against this repository's server, over
//! HTTP, with a fake engine (decision 0011 step 9).

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::HeaderMap;
use chrono::Utc;
use engine_runner::contract::{run_suite, Seed, FAKE_ENGINE};
use engine_runner::HttpOrchestrator;
use orchestration_model::{EngineLine, EngineName, Line, OnInterrupt, Trigger};
use orchestrator_postgres::{NewDeploy, NewFlow, NewRun, PgStore};
use orchestrator_server::{
    router, runner_of, Authenticate, EnginePort, Machine, PortState, ScanAsked, SystemClock,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;

/// The machine named in `x-test-machine`: only tests read it.
struct HeaderMachine;

#[async_trait]
impl Authenticate for HeaderMachine {
    async fn machine(&self, headers: &HeaderMap) -> Option<Machine> {
        let name = headers.get("x-test-machine")?.to_str().ok()?;
        Some(Machine(name.to_string()))
    }
}

struct StoreSeed {
    store: PgStore,
    flow: String,
}

fn engine() -> EngineLine {
    EngineLine {
        engine: EngineName::new(FAKE_ENGINE).unwrap(),
        line: Line::new(Line::SINGLE).unwrap(),
    }
}

#[async_trait]
impl Seed for StoreSeed {
    async fn queue(&self, parameters: Value) -> String {
        let run = NewRun {
            flow_id: self.flow.clone(),
            engine: engine(),
            kind: "job".into(),
            cluster: None,
            trigger: Trigger::Manual("person-1".into()),
            fire_at: None,
            parameters,
            timeout_secs: 60,
            lock_name: None,
            slot_pool: None,
            queued_at: Utc::now(),
        };
        self.store.enqueue(&run).await.unwrap().unwrap()
    }

    async fn cancel(&self, run: &str) {
        self.store.cancel(run, Utc::now()).await.unwrap();
    }

    async fn state(&self, run: &str) -> String {
        sqlx::query_scalar("SELECT state FROM runs WHERE id = $1::uuid")
            .bind(run)
            .fetch_one(self.store.pool())
            .await
            .unwrap()
    }

    async fn queue_deploy(&self) -> String {
        let new = NewDeploy {
            flow_id: self.flow.clone(),
            cluster: "machine-1".into(),
            source_path: "/srv/found/contract".into(),
            source_sha256: "ab".repeat(32),
            schedules: Vec::new(),
            queued_at: Utc::now(),
        };
        self.store.create_deploy(&new).await.unwrap()
    }

    async fn deploy_state(&self, deploy: &str) -> String {
        self.store.deploy(deploy).await.unwrap().unwrap().state
    }

    async fn request_scan(&self) {
        let port = EnginePort::new(self.store.clone(), SystemClock);
        let asked = port.request_scan(&machine(), &engine()).await.unwrap();
        assert_eq!(asked, ScanAsked::Asked);
    }

    async fn scan_pending(&self) -> bool {
        let runner = runner_of(&machine(), &engine());
        self.store.scan_requested(&runner).await.unwrap().is_some()
    }
}

fn machine() -> Machine {
    Machine("machine-1".into())
}

#[tokio::test]
async fn the_suite_passes_against_the_server() {
    let container = Postgres::default()
        .with_name("postgres")
        .with_tag("16-alpine")
        .with_container_name(format!("qo-test-{}", uuid::Uuid::new_v4().simple()))
        .start()
        .await
        .expect("PostgreSQL starts (is Docker running?)");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
    orchestrator_postgres::migrate(&pool).await.unwrap();
    let store = PgStore::new(pool);
    let folder = store.create_folder("contract").await.unwrap();
    let flow = NewFlow {
        folder_id: folder.id,
        name: "contract".into(),
        description: String::new(),
        engine: engine(),
        kind: "job".into(),
        cluster: None,
        parameters: json!({}),
        timeout_secs: 60,
        allow_overlap: true,
        on_interrupt: Some(OnInterrupt::None),
    };
    let flow = store.create_flow(&flow).await.unwrap().id;

    let state = PortState {
        port: EnginePort::new(store.clone(), SystemClock),
        auth: Arc::new(HeaderMachine),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(Arc::new(state));
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-test-machine", "machine-1".parse().unwrap());
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    let orchestrator = HttpOrchestrator::new(client, &format!("http://{address}"));
    run_suite(orchestrator, &StoreSeed { store, flow }).await;
}
