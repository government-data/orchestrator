//! A fresh PostgreSQL for each test, in a container named `qo-test-…` that
//! is removed when the test ends, also when it fails (its `Drop`).

#![allow(dead_code, unreachable_pub)]

use chrono::{DateTime, Utc};
use orchestration_model::{EngineLine, EngineName, Line, OnInterrupt};
use orchestrator_postgres::NewFlow;
use serde_json::json;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// The installation's PostgreSQL, as the core's tests run it.
const IMAGE: (&str, &str) = ("postgres", "16-alpine");

pub struct TestDb {
    _container: ContainerAsync<Postgres>,
    pub pool: PgPool,
}

impl TestDb {
    pub fn store(&self) -> orchestrator_postgres::PgStore {
        orchestrator_postgres::PgStore::new(self.pool.clone())
    }
}

/// Starts PostgreSQL, applies the migrations and connects.
pub async fn database() -> TestDb {
    let (name, tag) = IMAGE;
    let container = Postgres::default()
        .with_name(name)
        .with_tag(tag)
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
    TestDb {
        _container: container,
        pool,
    }
}

pub fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

/// A flow of an engine nobody knows, in `folder_id`.
pub fn new_flow(folder_id: &str, name: &str) -> NewFlow {
    NewFlow {
        folder_id: folder_id.into(),
        name: name.into(),
        description: String::new(),
        engine: EngineLine {
            engine: EngineName::new("an-engine").unwrap(),
            line: Line::new("-").unwrap(),
        },
        kind: "load".into(),
        cluster: None,
        parameters: json!({"day": "today"}),
        timeout_secs: 600,
        allow_overlap: false,
        on_interrupt: Some(OnInterrupt::Repeat),
    }
}
