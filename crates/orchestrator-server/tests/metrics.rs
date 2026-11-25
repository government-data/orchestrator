//! The metrics file: after runs end, are lost and the queue fills, every
//! label is one of the fixed names, and every label value is from its fixed
//! list: an engine, line and kind registered, a result, a route template.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use chrono::Duration;
use common::harness;
use orchestrator_server::{Metrics, Scheduler};
use prometheus_client::registry::Registry;
use serde_json::json;

const LABELS: [&str; 6] = ["engine", "line", "kind", "result", "route", "status"];
const RESULTS: [&str; 5] = ["succeeded", "failed", "timed_out", "cancelled", "lost"];
const ROUTES: [&str; 7] = [
    "/internal/engines/{engine}/{line}",
    "/internal/engines/{engine}/{line}/claim",
    "/internal/engines/{engine}/hold",
    "/internal/engines/{engine}/release",
    "/internal/runs/{id}/renew",
    "/internal/runs/{id}/logs",
    "/internal/runs/{id}/finish",
];

#[tokio::test]
async fn every_label_value_is_from_the_fixed_list() {
    let mut h = harness().await;
    let mut registry = Registry::default();
    let metrics = Metrics::register(&mut registry);
    h.with_metrics(Arc::clone(&metrics));
    h.register().await;
    let flow = h.flow("daily").await;
    let done = h.queue(&flow).await;
    let (_, claimed) = h.claim().await;
    let finish = json!({"lease": claimed["lease"], "state": "succeeded", "error_class": null});
    let (status, _) = h
        .post(&format!("/internal/runs/{done}/finish"), finish)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    h.queue(&flow).await;
    h.claim().await;
    h.queue(&flow).await;
    h.clock.advance(Duration::minutes(3));
    let scheduler =
        Scheduler::new(h.store.clone(), h.clock.clone()).with_metrics(Arc::clone(&metrics));
    scheduler.tick().await.unwrap();

    let dir = std::env::temp_dir().join(format!("qo-metrics-{}", uuid::Uuid::new_v4().simple()));
    let file = logging::metrics::MetricsFile::new("orchestrator", &dir, registry);
    let text = file.render();
    let samples = logging::metrics::samples(&text).unwrap();
    let ours: Vec<_> = samples
        .iter()
        .filter(|s| s.name.starts_with("orchestrator_"))
        .collect();
    let names: Vec<&str> = ours.iter().map(|s| s.name.as_str()).collect();
    for wanted in [
        "orchestrator_runs_total",
        "orchestrator_lost_runs_total",
        "orchestrator_queue_ready_runs",
        "orchestrator_runners_live",
        "orchestrator_http_requests_total",
        "orchestrator_schedule_lag_seconds",
    ] {
        assert!(names.contains(&wanted), "{wanted} in {names:?}");
    }
    for s in &ours {
        for (key, value) in &s.labels {
            if key == "le" {
                continue;
            }
            assert!(LABELS.contains(&key.as_str()), "{}: label {key}", s.name);
            let fixed = match key.as_str() {
                "engine" => value == "fake-engine",
                "line" => value == "-",
                "kind" => value == "job",
                "result" => RESULTS.contains(&value.as_str()),
                "route" => ROUTES.contains(&value.as_str()),
                _ => value.parse::<u16>().is_ok(),
            };
            assert!(fixed, "{}: {key}={value}", s.name);
        }
    }
    let results: Vec<_> = ours
        .iter()
        .filter(|s| s.name == "orchestrator_runs_total")
        .map(|s| (s.label("result").unwrap_or(""), s.value))
        .collect();
    assert!(results.contains(&("succeeded", 1.0)), "{results:?}");
    assert!(results.contains(&("lost", 1.0)), "{results:?}");
    assert!(!text.contains(&done), "no run id in the file");
    assert!(!text.contains(&flow.id), "no flow id in the file");
}
