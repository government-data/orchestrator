//! The core's `/v1/events` as `HttpEvents` calls it, against a stand-in
//! core over HTTP: a publish is stored (201, or 200 again), refused (400,
//! 403) or unanswered; `core.iam.changed` read with its cursor drops the
//! people's cached actions and confirms what it handled.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{TimeZone, Utc};
use orchestrator_server::{
    iam_changes_once, CachedActions, CoreEvents, CoreToken, EffectiveActions, Event, FixedClock,
    HttpEvents, MachineClient, Person, Published,
};
use serde_json::{json, Value};

#[derive(Clone, Default)]
struct Core {
    publish_status: Arc<Mutex<u16>>,
    published: Arc<Mutex<Vec<Value>>>,
    reads: Arc<Mutex<Vec<HashMap<String, String>>>>,
    batch: Arc<Mutex<Value>>,
    confirmed: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn token() -> Response {
    let body = r#"{"access_token": "t1", "expires_in": 300}"#;
    ([("content-type", "application/json")], body).into_response()
}

async fn publish(State(core): State<Core>, Json(event): Json<Value>) -> Response {
    core.published.lock().unwrap().push(event);
    let status = *core.publish_status.lock().unwrap();
    StatusCode::from_u16(status).unwrap().into_response()
}

async fn read(State(core): State<Core>, Query(q): Query<HashMap<String, String>>) -> Response {
    core.reads.lock().unwrap().push(q);
    Json(core.batch.lock().unwrap().clone()).into_response()
}

async fn confirm(
    State(core): State<Core>,
    Path(topic): Path<String>,
    Json(body): Json<Value>,
) -> StatusCode {
    core.confirmed.lock().unwrap().push((topic, body));
    StatusCode::NO_CONTENT
}

async fn start() -> (Core, HttpEvents) {
    let core = Core::default();
    let app = Router::new()
        .route("/oauth/token", post(token))
        .route("/api/v1/events", get(read).post(publish))
        .route("/api/v1/events/cursors/{topic}", put(confirm))
        .with_state(core.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    let client = MachineClient {
        id: "orchestrator-machine".into(),
        secret: "s".into(),
    };
    let http = reqwest::Client::new();
    let token = Arc::new(CoreToken::new(
        http.clone(),
        &format!("http://{addr}"),
        client,
    ));
    (
        core,
        HttpEvents::new(http, &format!("http://{addr}/api"), token),
    )
}

fn event() -> Event {
    Event {
        id: "0190f3c4-0000-7000-8000-000000000001".into(),
        topic: "orchestrator.run.finished".into(),
        version: 1,
        occurred_at: Utc.with_ymd_and_hms(2026, 11, 27, 10, 0, 0).unwrap(),
        data: json!({"run_id": "r1"}),
    }
}

#[tokio::test]
async fn a_publish_is_stored_refused_or_unanswered() {
    let (core, events) = start().await;
    for (status, said) in [
        (201, Published::Stored),
        (200, Published::Stored),
        (400, Published::Refused(400)),
        (403, Published::Refused(403)),
        (503, Published::Unavailable),
    ] {
        *core.publish_status.lock().unwrap() = status;
        assert_eq!(events.publish(&event()).await, said, "{status}");
    }
    let sent = core.published.lock().unwrap()[0].clone();
    assert_eq!(
        sent,
        json!({"id": "0190f3c4-0000-7000-8000-000000000001",
               "topic": "orchestrator.run.finished", "version": 1,
               "occurred_at": "2026-11-27T10:00:00Z", "data": {"run_id": "r1"}})
    );
}

/// What the core says a person holds, counting the questions.
#[derive(Clone, Default)]
struct Asked(Arc<AtomicUsize>);

#[async_trait]
impl EffectiveActions for Asked {
    async fn held(&self, _person: &Person) -> Option<Vec<String>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(Vec::new())
    }
}

#[tokio::test]
async fn an_iam_change_drops_the_cached_actions_and_is_confirmed() {
    let (core, events) = start().await;
    let asked = Asked::default();
    let clock = FixedClock::at(Utc.with_ymd_and_hms(2026, 11, 27, 10, 0, 0).unwrap());
    let cache = CachedActions::new(asked.clone(), clock);
    let ana = Person { id: "ana".into() };
    cache.held(&ana).await;

    *core.batch.lock().unwrap() = json!({"events": [], "cursor": 7});
    let after = iam_changes_once(&events, &cache, None, 0).await;
    assert_eq!(after, Some(7));
    cache.held(&ana).await;
    assert_eq!(asked.0.load(Ordering::SeqCst), 1, "no event: kept");

    *core.batch.lock().unwrap() = json!({"events": [
        {"seq": 8, "id": "e8", "topic": "core.iam.changed", "version": 1,
         "publisher": "core", "occurred_at": "2026-11-27T10:00:00Z", "data": {"kind": "policy"}}
    ], "cursor": 8});
    assert_eq!(iam_changes_once(&events, &cache, after, 0).await, Some(8));
    cache.held(&ana).await;
    assert_eq!(asked.0.load(Ordering::SeqCst), 2, "an event: asked again");

    let reads = core.reads.lock().unwrap().clone();
    assert_eq!(reads[0]["topic"], "core.iam.changed");
    assert_eq!(reads[0]["version"], "1");
    assert!(
        !reads[0].contains_key("after"),
        "first read: the confirmed cursor"
    );
    assert_eq!(reads[1]["after"], "7");
    let confirmed = core.confirmed.lock().unwrap().clone();
    assert_eq!(
        confirmed,
        [
            (
                "core.iam.changed".to_string(),
                json!({"version": 1, "cursor": 7})
            ),
            (
                "core.iam.changed".to_string(),
                json!({"version": 1, "cursor": 8})
            ),
        ]
    );
}
