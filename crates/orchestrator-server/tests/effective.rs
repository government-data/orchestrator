//! The core's `GET /v1/iam/effective` as `HttpEffectiveActions` reads it,
//! against a stand-in core over HTTP: the held actions are the answer; an
//! unknown account holds none; a 503, a malformed body or a refused token
//! is no answer (so the call is refused as unavailable, never allowed).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use orchestrator_server::{
    CoreToken, EffectiveActions, HttpEffectiveActions, MachineClient, Person,
};

#[derive(Clone)]
struct Core {
    answer: Arc<Mutex<(StatusCode, String)>>,
    subjects: Arc<Mutex<Vec<String>>>,
}

async fn token() -> Response {
    let body = r#"{"access_token": "t1", "expires_in": 300}"#;
    ([("content-type", "application/json")], body).into_response()
}

async fn effective(
    State(core): State<Core>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if headers["authorization"] != "Bearer t1" {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    core.subjects.lock().unwrap().push(q["subject"].clone());
    let (status, body) = core.answer.lock().unwrap().clone();
    (status, [("content-type", "application/json")], body).into_response()
}

async fn start() -> (Core, HttpEffectiveActions) {
    let core = Core {
        answer: Arc::new(Mutex::new((StatusCode::OK, String::new()))),
        subjects: Arc::default(),
    };
    let app = Router::new()
        .route("/oauth/token", post(token))
        .route("/api/v1/iam/effective", get(effective))
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
    let actions = HttpEffectiveActions::new(http, &format!("http://{addr}/api"), token);
    (core, actions)
}

fn answer(core: &Core, status: StatusCode, body: &str) {
    *core.answer.lock().unwrap() = (status, body.into());
}

#[tokio::test]
async fn what_the_core_says_a_person_holds() {
    let (core, actions) = start().await;
    let person = Person { id: "a b&c".into() };
    answer(
        &core,
        StatusCode::OK,
        r#"{"subject": "a b&c", "actions": ["orchestrator:ImportFlows"]}"#,
    );
    assert_eq!(
        actions.held(&person).await,
        Some(vec!["orchestrator:ImportFlows".to_string()])
    );
    assert_eq!(
        core.subjects.lock().unwrap()[0],
        "a b&c",
        "the subject encoded"
    );

    answer(
        &core,
        StatusCode::NOT_FOUND,
        r#"{"error": "no such subject"}"#,
    );
    assert_eq!(
        actions.held(&person).await,
        Some(Vec::new()),
        "unknown: none"
    );

    for (status, body) in [
        (StatusCode::SERVICE_UNAVAILABLE, "{}"),
        (StatusCode::FORBIDDEN, r#"{"error": "not a module"}"#),
        (StatusCode::OK, r#"{"subject": "x"}"#),
        (StatusCode::OK, "not json"),
    ] {
        answer(&core, status, body);
        assert_eq!(actions.held(&person).await, None, "{status} {body}");
    }
}
