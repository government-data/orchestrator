//! The people's API with a fake core: the person a request names in
//! `x-test-person`, and the actions the fake core says everyone holds.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use orchestrator_server::{
    people_router, EffectiveActions, EnginePort, FixedClock, People, PeopleState, Person,
    PersonTokens,
};
use serde_json::Value;
use tower::ServiceExt;

use super::Harness;

/// The person a request names in `x-test-person`: only tests read it.
struct HeaderPerson;

#[async_trait]
impl PersonTokens for HeaderPerson {
    async fn person(&self, headers: &HeaderMap) -> Option<Person> {
        let id = headers.get("x-test-person")?.to_str().ok()?;
        Some(Person { id: id.into() })
    }
}

/// The core's answer for everyone; `None`: it does not answer.
#[derive(Clone, Default)]
pub struct FakeCore(pub Arc<Mutex<Option<Vec<String>>>>);

#[async_trait]
impl EffectiveActions for FakeCore {
    async fn held(&self, _person: &Person) -> Option<Vec<String>> {
        self.0.lock().unwrap().clone()
    }
}

impl FakeCore {
    /// The core says everyone holds `actions`.
    pub fn holds(&self, actions: &[&str]) {
        *self.0.lock().unwrap() = Some(actions.iter().map(|a| (*a).to_string()).collect());
    }

    /// The core does not answer.
    pub fn silent(&self) {
        *self.0.lock().unwrap() = None;
    }
}

/// The people's API on the harness's store, and its fake core.
pub fn people_app(h: &Harness) -> (axum::Router, FakeCore) {
    people_app_on(EnginePort::new(h.store.clone(), h.clock.clone()))
}

/// The people's API on `port`, and its fake core.
pub fn people_app_on(port: EnginePort<FixedClock>) -> (axum::Router, FakeCore) {
    let core = FakeCore::default();
    let state = PeopleState {
        port,
        people: People::new(Arc::new(HeaderPerson), Arc::new(core.clone())),
    };
    (people_router(Arc::new(state)), core)
}

/// A call to the people's API, as the person `ana` or as no one.
pub async fn call(
    app: &axum::Router,
    method: Method,
    uri: &str,
    person: bool,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if person {
        req = req.header("x-test-person", "ana");
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Each call is refused without a person (401), without `action` (403)
/// and while the core does not answer (503).
pub async fn refused_each_way(
    app: &axum::Router,
    core: &FakeCore,
    action: &str,
    calls: &[(Method, String, Value)],
) {
    for (method, uri, body) in calls {
        core.holds(&[action]);
        let (status, _) = call(app, method.clone(), uri, false, body.clone()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}: no person");
        core.holds(&["orchestrator:Other"]);
        let (status, _) = call(app, method.clone(), uri, true, body.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: not held");
        core.silent();
        let (status, _) = call(app, method.clone(), uri, true, body.clone()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri}: no answer");
    }
}
