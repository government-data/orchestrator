//! A fresh PostgreSQL for each test, in a container named `qo-test-…`
//! removed when the test ends (its `Drop`), and the engine port served in
//! process to a fake engine.

#![allow(dead_code, unreachable_pub)]

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use chrono::{DateTime, Utc};
use engine_port::{Descriptor, RunKind, PORT};
use orchestration_model::{EngineLine, EngineName, Line, OnInterrupt, Trigger};
use orchestrator_postgres::{NewFlow, NewRun, PgStore};
use orchestrator_server::{
    core_router, router, Authenticate, EnginePort, FixedClock, Machine, PortState,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tower::ServiceExt;

pub mod people;

pub fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

/// The machine a request names in `x-test-machine`: only tests read it.
struct HeaderMachine;

#[async_trait]
impl Authenticate for HeaderMachine {
    async fn machine(&self, headers: &HeaderMap) -> Option<Machine> {
        let name = headers.get("x-test-machine")?.to_str().ok()?;
        Some(Machine(name.to_string()))
    }

    async fn module(&self, headers: &HeaderMap) -> Option<String> {
        let name = headers.get("x-test-module")?.to_str().ok()?;
        Some(name.to_string())
    }
}

/// The engine port and the core's calls, as the server serves them.
fn app(state: PortState<FixedClock>) -> axum::Router {
    let state = Arc::new(state);
    router(Arc::clone(&state)).merge(core_router(state))
}

pub struct Harness {
    _container: ContainerAsync<Postgres>,
    pub store: PgStore,
    pub clock: FixedClock,
    pub app: axum::Router,
}

pub async fn harness() -> Harness {
    let container = Postgres::default()
        .with_name("postgres")
        .with_tag("16-alpine")
        .with_container_name(format!("qo-test-{}", uuid::Uuid::new_v4().simple()))
        .start()
        .await
        .expect("PostgreSQL starts (is Docker running?)");
    let port = container.get_host_port_ipv4(5432).await.expect("its port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.expect("it connects");
    orchestrator_postgres::migrate(&pool)
        .await
        .expect("the migrations apply");
    let store = PgStore::new(pool);
    let clock = FixedClock::at(at("2026-11-25T10:00:00Z"));
    let state = PortState {
        port: EnginePort::new(store.clone(), clock.clone()),
        auth: Arc::new(HeaderMachine),
    };
    Harness {
        _container: container,
        store,
        clock,
        app: app(state),
    }
}

pub fn engine() -> EngineLine {
    EngineLine {
        engine: EngineName::new("fake-engine").unwrap(),
        line: Line::new("-").unwrap(),
    }
}

pub fn descriptor() -> Descriptor {
    Descriptor {
        engine: engine().engine,
        line: engine().line,
        engine_version: "0.0.1".into(),
        port: PORT,
        kinds: vec![RunKind {
            name: "job".into(),
            parameters: json!({"type": "object"}),
        }],
        concurrency: 2,
    }
}

impl Harness {
    /// Serves the port again, opening sealed variables with `sealer`.
    pub fn with_sealer(&mut self, sealer: Arc<orchestrator_server::Sealer>) {
        let state = PortState {
            port: EnginePort::new(self.store.clone(), self.clock.clone()).with_sealer(sealer),
            auth: Arc::new(HeaderMachine),
        };
        self.app = app(state);
    }

    /// Serves the port again, reading the live machines from `machines`.
    pub fn with_machines(&mut self, machines: Arc<dyn orchestrator_server::MachineList>) {
        let state = PortState {
            port: EnginePort::new(self.store.clone(), self.clock.clone()).with_machines(machines),
            auth: Arc::new(HeaderMachine),
        };
        self.app = app(state);
    }

    /// Serves the port again, counting in `metrics`.
    pub fn with_metrics(&mut self, metrics: Arc<orchestrator_server::Metrics>) {
        let state = PortState {
            port: EnginePort::new(self.store.clone(), self.clock.clone())
                .with_metrics(Arc::clone(&metrics)),
            auth: Arc::new(HeaderMachine),
        };
        self.app = orchestrator_server::counted(app(state), metrics);
    }

    /// A call of the fake engine from `machine` (`None`: no identity).
    pub async fn call(
        &self,
        method: Method,
        path: &str,
        machine: Option<&str>,
        body: Value,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        if let Some(m) = machine {
            req = req
                .header("x-test-machine", m)
                .header("x-test-module", "engine-fake");
        }
        let req = req.body(Body::from(body.to_string())).unwrap();
        let res = self.app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    pub async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.call(Method::POST, path, Some("machine-1"), body).await
    }

    /// Registers the fake engine from machine-1.
    pub async fn register(&self) {
        let body = serde_json::to_value(descriptor()).unwrap();
        let (status, _) = self
            .call(
                Method::PUT,
                "/internal/engines/fake-engine/-",
                Some("machine-1"),
                body,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
    }

    /// A flow of the fake engine that repeats an interrupted run.
    pub async fn flow(&self, name: &str) -> orchestration_model::Flow {
        let folder = self.store.create_folder(name).await.unwrap();
        let new = NewFlow {
            folder_id: folder.id,
            name: name.into(),
            description: String::new(),
            engine: engine(),
            kind: "job".into(),
            cluster: None,
            parameters: json!({}),
            timeout_secs: 600,
            allow_overlap: false,
            on_interrupt: Some(OnInterrupt::Repeat),
        };
        self.store.create_flow(&new).await.unwrap()
    }

    /// Queues a manual run of `flow` now.
    pub async fn queue(&self, flow: &orchestration_model::Flow) -> String {
        use orchestrator_server::Clock;
        let run = NewRun {
            flow_id: flow.id.clone(),
            engine: flow.engine.clone(),
            kind: flow.kind.clone(),
            cluster: None,
            trigger: Trigger::Manual("person-1".into()),
            fire_at: None,
            parameters: json!({"n": 1}),
            timeout_secs: flow.timeout_secs,
            lock_name: None,
            slot_pool: None,
            queued_at: self.clock.now(),
        };
        self.store.enqueue(&run).await.unwrap().unwrap()
    }

    pub async fn claim(&self) -> (StatusCode, Value) {
        self.post(
            "/internal/engines/fake-engine/-/claim",
            json!({"runner": "r1"}),
        )
        .await
    }
}
