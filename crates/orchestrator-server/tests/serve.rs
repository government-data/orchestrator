//! The server binary's wiring, started against a test PostgreSQL
//! (`qo-test-…`) and a stand-in core in process: the migrations apply, the
//! core's calls answer at `/internal/…` and under the internal route
//! prefix, the engine port wants an engine module's token, the people's
//! API sits under the web route, the scheduler reads the core's modules,
//! `core.iam.changed` is read, a run's end reaches the core's events, and
//! the server stops when asked.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use orchestration_model::{EngineLine, EngineName, Line, Trigger};
use orchestrator_postgres::{NewFlow, NewRun, PgConnectOptions, PgSslMode, PgStore};
use orchestrator_server::{serve, Every, MachineClient, Sealer, Settings};
use serde_json::{json, Value};
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;

/// What the stand-in core was asked: `METHOD path` and the topics.
type Seen = Arc<Mutex<Vec<String>>>;

fn saw(seen: &Seen, what: String) {
    seen.lock().unwrap().push(what);
}

async fn fake_core(seen: Seen) -> String {
    let app = Router::new()
        .route(
            "/oauth/token",
            post(|| async { Json(json!({"access_token": "core-token", "expires_in": 300})) }),
        )
        .route("/oauth/jwks", get(|| async { Json(json!({"keys": []})) }))
        .route(
            "/v1/modules",
            get(|State(s): State<Seen>| async move {
                saw(&s, "GET /v1/modules".into());
                Json(json!({"modules": [{"name": "orchestrator", "version": "0.1.0"}]}))
            }),
        )
        .route(
            "/v1/events",
            get(|State(s): State<Seen>, Query(q): Query<Value>| async move {
                saw(&s, format!("read {}", q["topic"].as_str().unwrap_or("")));
                tokio::time::sleep(Duration::from_millis(200)).await;
                Json(json!({"events": [], "cursor": 0}))
            })
            .post(|State(s): State<Seen>, Json(e): Json<Value>| async move {
                saw(&s, format!("publish {}", e["topic"].as_str().unwrap_or("")));
                StatusCode::CREATED
            }),
        )
        .route(
            "/v1/events/cursors/{topic}",
            put(|| async { StatusCode::NO_CONTENT }),
        )
        .with_state(seen);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

/// The machine certificate's subject, as Traefik passes it.
const MASTER: &str = "Subject%3D%22O%3Dqd%2COU%3Dapp%2CCN%3Dmaster%22%3BSerialNumber%3D%221%22";

async fn get_status(http: &reqwest::Client, url: &str) -> Option<u16> {
    let res = http
        .get(url)
        .header("x-forwarded-tls-client-cert-info", MASTER)
        .send()
        .await
        .ok()?;
    Some(res.status().as_u16())
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..100 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("never: {what}");
}

fn settings(db_port: u16, core: &str, listen: std::net::SocketAddr) -> Settings {
    Settings {
        database: PgConnectOptions::new()
            .host("127.0.0.1")
            .port(db_port)
            .username("postgres")
            .password("postgres")
            .database("postgres")
            .ssl_mode(PgSslMode::Disable),
        issuer: core.into(),
        people_client: "orchestrator".into(),
        machine_client: MachineClient {
            id: "orchestrator-machine".into(),
            secret: "not-a-real-secret".into(),
        },
        core_base: core.into(),
        sealer: Sealer::from_file_content("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap(),
        listen,
        core_flow: None,
    }
}

/// A run of a flow, cancelled while queued: its end goes to the outbox.
async fn end_a_run(store: &PgStore) {
    let folder = store.create_folder("daily").await.unwrap();
    let engine = EngineLine {
        engine: EngineName::new("fake-engine").unwrap(),
        line: Line::new("-").unwrap(),
    };
    let flow = store
        .create_flow(&NewFlow {
            folder_id: folder.id,
            name: "daily".into(),
            description: String::new(),
            engine: engine.clone(),
            kind: "job".into(),
            cluster: None,
            parameters: json!({}),
            timeout_secs: 60,
            allow_overlap: false,
            on_interrupt: None,
        })
        .await
        .unwrap();
    let now = chrono::Utc::now();
    let run = NewRun {
        flow_id: flow.id,
        engine,
        kind: "job".into(),
        cluster: None,
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: json!({}),
        timeout_secs: 60,
        lock_name: None,
        slot_pool: None,
        queued_at: now,
    };
    let id = store.enqueue(&run).await.unwrap().unwrap();
    store.cancel(&id, now).await.unwrap();
}

#[tokio::test]
async fn the_server_starts_serves_and_stops() {
    std::env::set_var("QD_METRICS_DIR", "off");
    let db = Postgres::default()
        .with_tag("16-alpine")
        .with_container_name(format!("qo-test-{}", uuid::Uuid::new_v4().simple()))
        .start()
        .await
        .expect("PostgreSQL starts (is Docker running?)");
    let db_port = db.get_host_port_ipv4(5432).await.unwrap();
    let seen: Seen = Arc::default();
    let core = fake_core(Arc::clone(&seen)).await;
    let listen = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let every = Every {
        tick: Duration::from_millis(100),
        outbox: Duration::from_millis(100),
        iam_retry: Duration::from_millis(100),
    };
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        settings(db_port, &core, listen),
        reqwest::Client::new(),
        every,
        async {
            stopped.await.ok();
        },
    ));

    let http = reqwest::Client::new();
    let base = format!("http://{listen}");
    let drain = format!("{base}/internal/drain?horizon_secs=60");
    for _ in 0..100 {
        if get_status(&http, &drain).await == Some(200) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        get_status(&http, &drain).await,
        Some(200),
        "the core's drain"
    );
    let prefixed = format!("{base}/modules/orchestrator/internal/removal?module=x");
    assert_eq!(
        get_status(&http, &prefixed).await,
        Some(200),
        "under the prefix"
    );
    let pools = format!("{base}/internal/engines/fake-engine/-/pools");
    let res = http
        .put(&pools)
        .header("x-forwarded-tls-client-cert-info", MASTER);
    let res = res.json(&json!({"pools": []})).send().await.unwrap();
    assert_eq!(
        res.status(),
        401,
        "the engine port wants an engine module's token"
    );
    let chains = http
        .post(format!("{base}/modules/orchestrator/internal/chains"))
        .header("x-forwarded-tls-client-cert-info", MASTER)
        .json(&json!({"job_id": "j-1", "steps": [{"kind": "tables"}]}));
    let res = chains.send().await.unwrap();
    assert_eq!(
        res.status(),
        503,
        "the core's machine; no flow for its chains"
    );
    let found = format!("{base}/orchestrator/api/v1/found?machine=m&engine=e&line=-");
    assert_eq!(get_status(&http, &found).await, Some(401), "not signed in");

    let pool = orchestrator_postgres::connect(settings(db_port, &core, listen).database, 2);
    end_a_run(&PgStore::new(pool.await.unwrap())).await;
    let has = |what: &str| seen.lock().unwrap().iter().any(|s| s == what);
    eventually("the scheduler reads the modules", || has("GET /v1/modules")).await;
    eventually("core.iam.changed is read", || has("read core.iam.changed")).await;
    eventually("the run's end is published", || {
        has("publish orchestrator.run.finished")
    })
    .await;

    stop.send(()).unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(10), server).await;
    assert_eq!(ended.expect("it stops").unwrap(), Ok(()));
}
