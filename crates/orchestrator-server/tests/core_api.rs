//! The core's `GET /v1/machines` and `GET /v1/modules` as `HttpMachineList`
//! and `HttpModuleList` read them, against a stand-in core over HTTP: the
//! listed names are the answer; a 401, a 503, a malformed body or no token
//! is no answer (so nothing is adopted); the machine client's token is
//! shared, kept and fetched again after a 401.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use orchestrator_server::{
    CoreToken, HttpMachineList, HttpModuleList, MachineClient, MachineList, ModuleList,
};

/// What the stand-in core answers to `GET /api/v1/machines` and
/// `/api/v1/modules`.
#[derive(Clone)]
struct Core {
    answer: Arc<Mutex<(StatusCode, String)>>,
    tokens: Arc<AtomicUsize>,
    token_status: Arc<Mutex<StatusCode>>,
}

async fn token(State(core): State<Core>, headers: HeaderMap, body: String) -> Response {
    // `orchestrator-machine:s`, as HTTP Basic.
    let basic = "Basic b3JjaGVzdHJhdG9yLW1hY2hpbmU6cw==";
    assert_eq!(headers["authorization"], basic);
    assert!(body.contains("grant_type=client_credentials"));
    assert!(body.contains("audience=core"));
    let status = *core.token_status.lock().unwrap();
    if status != StatusCode::OK {
        return status.into_response();
    }
    let n = core.tokens.fetch_add(1, Ordering::SeqCst) + 1;
    let body = format!(r#"{{"access_token": "t{n}", "expires_in": 300}}"#);
    ([("content-type", "application/json")], body).into_response()
}

async fn listing(State(core): State<Core>, headers: HeaderMap) -> Response {
    let n = core.tokens.load(Ordering::SeqCst);
    if headers.get("authorization").and_then(|h| h.to_str().ok()) != Some(&format!("Bearer t{n}")) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (status, body) = core.answer.lock().unwrap().clone();
    (status, [("content-type", "application/json")], body).into_response()
}

async fn start() -> (Core, HttpMachineList, HttpModuleList) {
    let core = Core {
        answer: Arc::new(Mutex::new((StatusCode::OK, String::new()))),
        tokens: Arc::default(),
        token_status: Arc::new(Mutex::new(StatusCode::OK)),
    };
    let app = Router::new()
        .route("/oauth/token", post(token))
        .route("/api/v1/machines", get(listing))
        .route("/api/v1/modules", get(listing))
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
        &format!("http://{addr}/"),
        client,
    ));
    let base = format!("http://{addr}/api/");
    let machines = HttpMachineList::new(http.clone(), &base, token.clone());
    let modules = HttpModuleList::new(http, &base, token);
    (core, machines, modules)
}

fn answer(core: &Core, status: StatusCode, body: &str) {
    *core.answer.lock().unwrap() = (status, body.to_string());
}

const LISTED: &str = r#"{"machines": [
    {"name": "worker-1", "role": "worker", "state": "approved"},
    {"name": "worker-2", "role": "worker", "state": "draining"},
    {"name": "lake-a", "role": "lake", "state": "pending"}]}"#;

#[tokio::test]
async fn the_listed_machines_are_live_with_one_token() {
    let (core, list, _) = start().await;
    answer(&core, StatusCode::OK, LISTED);
    let live = list.live().await;
    assert_eq!(
        live,
        Some(vec!["worker-1".into(), "worker-2".into(), "lake-a".into()])
    );
    answer(&core, StatusCode::OK, r#"{"machines": []}"#);
    assert_eq!(list.live().await, Some(vec![]));
    assert_eq!(core.tokens.load(Ordering::SeqCst), 1, "the token is kept");
}

#[tokio::test]
async fn a_refused_token_is_no_answer_and_is_fetched_again() {
    let (core, list, _) = start().await;
    answer(&core, StatusCode::OK, LISTED);
    assert!(list.live().await.is_some());
    // The core no longer takes `t1` (a rotated secret, a restarted provider).
    core.tokens.fetch_add(1, Ordering::SeqCst);
    assert_eq!(list.live().await, None, "401: no answer");
    assert!(list.live().await.is_some(), "a new token after the 401");
    assert_eq!(core.tokens.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn an_unavailable_core_is_no_answer() {
    let (core, list, _) = start().await;
    answer(
        &core,
        StatusCode::SERVICE_UNAVAILABLE,
        r#"{"error": "unavailable"}"#,
    );
    assert_eq!(list.live().await, None);
}

#[tokio::test]
async fn a_malformed_body_is_no_answer() {
    let (core, list, _) = start().await;
    for body in [
        "not json",
        r#"{"modules": []}"#,
        r#"{"machines": [{"role": "worker"}]}"#,
        r#"{"machines": "worker-1"}"#,
    ] {
        answer(&core, StatusCode::OK, body);
        assert_eq!(list.live().await, None, "{body}");
    }
    answer(&core, StatusCode::ACCEPTED, LISTED);
    assert_eq!(list.live().await, None, "only a 200 is an answer");
}

#[tokio::test]
async fn no_token_from_the_provider_is_no_answer() {
    let (core, list, _) = start().await;
    answer(&core, StatusCode::OK, LISTED);
    *core.token_status.lock().unwrap() = StatusCode::UNAUTHORIZED;
    assert_eq!(list.live().await, None);
    *core.token_status.lock().unwrap() = StatusCode::OK;
    assert!(list.live().await.is_some(), "asked again on the next call");
}

#[test]
fn the_secret_is_not_in_debug() {
    let client = MachineClient {
        id: "orchestrator-machine".into(),
        secret: "very-secret".into(),
    };
    assert!(!format!("{client:?}").contains("very-secret"));
}

#[tokio::test]
async fn the_listed_modules_are_installed_with_the_shared_token() {
    let (core, machines, modules) = start().await;
    answer(&core, StatusCode::OK, LISTED);
    assert!(machines.live().await.is_some());
    answer(
        &core,
        StatusCode::OK,
        r#"{"modules": [{"name": "orchestrator", "version": "0.3.0"},
        {"name": "engine-a", "version": "0.2.0"}]}"#,
    );
    assert_eq!(
        modules.installed().await,
        Some(vec!["orchestrator".into(), "engine-a".into()])
    );
    assert_eq!(core.tokens.load(Ordering::SeqCst), 1, "one token for both");
}

#[tokio::test]
async fn a_module_list_that_cannot_be_read_is_no_answer() {
    let (core, _, modules) = start().await;
    answer(&core, StatusCode::SERVICE_UNAVAILABLE, "{}");
    assert_eq!(modules.installed().await, None);
    answer(&core, StatusCode::OK, LISTED);
    assert_eq!(modules.installed().await, None, "machines are not modules");
    core.tokens.fetch_add(1, Ordering::SeqCst);
    answer(&core, StatusCode::OK, r#"{"modules": []}"#);
    assert_eq!(modules.installed().await, None, "401: no answer");
    assert_eq!(modules.installed().await, Some(vec![]));
}
