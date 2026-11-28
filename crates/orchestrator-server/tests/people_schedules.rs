//! A flow's schedules for people with a fake core: read behind
//! `orchestrator:ViewFlows`, changed behind `orchestrator:ManageSchedules`;
//! a call refused (401, 403, 503) changes nothing, and a schedule the core
//! keeps is changed by the core alone.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use common::people::{call, people_app, refused_each_way};
use orchestrator_server::{MANAGE_SCHEDULES, VIEW_FLOWS};
use schedule::ScheduleSpec;
use serde_json::{json, Value};

const NO_ID: &str = "00000000-0000-0000-0000-000000000000";

fn weekdays() -> Value {
    json!({"cron": "0  6 * * 1-5", "timezone": "Europe/Madrid", "parameters": {"region": "north"}})
}

#[tokio::test]
async fn a_call_without_its_action_is_refused_and_changes_nothing() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let (app, core) = people_app(&h);
    core.holds(&[MANAGE_SCHEDULES]);
    let path = format!("/api/v1/flows/{}/schedules", flow.id);
    let (status, made) = call(&app, Method::POST, &path, true, weekdays()).await;
    assert_eq!(status, StatusCode::CREATED);
    let one = format!("/api/v1/schedules/{}", made["id"].as_str().unwrap());
    let before = h.store.schedules(&flow.id).await.unwrap();

    let read = [(Method::GET, path.clone(), Value::Null)];
    refused_each_way(&app, &core, VIEW_FLOWS, &read).await;
    let changes = [
        (Method::POST, path.clone(), weekdays()),
        (Method::PATCH, one.clone(), json!({"enabled": false})),
        (Method::DELETE, one.clone(), Value::Null),
    ];
    refused_each_way(&app, &core, MANAGE_SCHEDULES, &changes).await;
    core.holds(&[VIEW_FLOWS]);
    for (method, uri, body) in &changes {
        let (status, _) = call(&app, method.clone(), uri, true, body.clone()).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{uri}: seeing is not managing"
        );
    }
    assert_eq!(h.store.schedules(&flow.id).await.unwrap(), before);
}

#[tokio::test]
async fn a_person_creates_disables_enables_and_deletes_a_schedule() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let (app, core) = people_app(&h);
    core.holds(&[VIEW_FLOWS, MANAGE_SCHEDULES]);
    let path = format!("/api/v1/flows/{}/schedules", flow.id);

    let (status, made) = call(&app, Method::POST, &path, true, weekdays()).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        (made["cron"].as_str(), made["enabled"].as_bool()),
        (Some("0 6 * * 1-5"), Some(true))
    );
    assert_eq!(
        made["next_fire_at"], "2026-11-26T05:00:00Z",
        "06:00 in Madrid, the next weekday"
    );
    let one = format!("/api/v1/schedules/{}", made["id"].as_str().unwrap());
    for bad in [
        json!({"cron": "61 * * * *", "timezone": "UTC"}),
        json!({"cron": "0 6 * * *", "timezone": "Nowhere/City"}),
        json!({"cron": "0 6 * * *", "timezone": "UTC", "parameters": [1]}),
    ] {
        assert_eq!(
            call(&app, Method::POST, &path, true, bad.clone()).await.0,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{bad}"
        );
    }
    let unknown = format!("/api/v1/flows/{NO_ID}/schedules");
    assert_eq!(
        call(&app, Method::POST, &unknown, true, weekdays()).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, Method::GET, &unknown, true, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );

    let (status, off) = call(&app, Method::PATCH, &one, true, json!({"enabled": false})).await;
    assert_eq!(
        (
            status,
            off["enabled"].as_bool(),
            off["next_fire_at"].clone()
        ),
        (StatusCode::OK, Some(false), Value::Null)
    );
    let (_, on) = call(&app, Method::PATCH, &one, true, json!({"enabled": true})).await;
    assert_eq!(
        (on["enabled"].as_bool(), on["next_fire_at"].as_str()),
        (Some(true), Some("2026-11-26T05:00:00Z"))
    );
    let (status, listed) = call(&app, Method::GET, &path, true, Value::Null).await;
    assert_eq!(
        (status, listed["schedules"].as_array().unwrap().len()),
        (StatusCode::OK, 1)
    );
    assert_eq!(listed["schedules"][0]["kept_by_core"], false);

    let (status, _) = call(&app, Method::DELETE, &one, true, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(h.store.schedules(&flow.id).await.unwrap().is_empty());
    assert_eq!(
        call(&app, Method::DELETE, &one, true, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    let gone = "/api/v1/schedules/x";
    assert_eq!(
        call(&app, Method::PATCH, gone, true, json!({"enabled": true}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_schedule_the_core_keeps_is_not_changed_by_a_person() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("core").await;
    let spec = ScheduleSpec {
        cron: "0 2 * * *".into(),
        timezone: "UTC".into(),
        trigger: None,
    };
    h.store
        .keep_core_schedule(&flow.id, "core-schedule-1", &spec, None)
        .await
        .unwrap();
    let kept = h.store.schedules(&flow.id).await.unwrap();
    let (app, core) = people_app(&h);
    core.holds(&[VIEW_FLOWS, MANAGE_SCHEDULES]);
    let (_, listed) = call(
        &app,
        Method::GET,
        &format!("/api/v1/flows/{}/schedules", flow.id),
        true,
        Value::Null,
    )
    .await;
    assert_eq!(listed["schedules"][0]["kept_by_core"], true);
    let one = format!("/api/v1/schedules/{}", kept[0].id);
    let (status, _) = call(&app, Method::PATCH, &one, true, json!({"enabled": true})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = call(&app, Method::DELETE, &one, true, Value::Null).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(h.store.schedules(&flow.id).await.unwrap(), kept);
}
