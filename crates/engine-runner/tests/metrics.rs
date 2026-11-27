//! The runner's metrics: why it pauses, its busy slots, the runs it ended
//! and its stages' times, every label from its fixed list.

use engine_runner::contract::FakeEngine;
use std::time::Duration;

use engine_runner::{Ended, Engine, Pause, RunnerMetrics};
use orchestration_model::FinalState;
use prometheus_client::registry::Registry;

#[test]
fn every_label_value_is_from_the_fixed_list() {
    let mut registry = Registry::default();
    let m = RunnerMetrics::register(&FakeEngine::default().descriptor(), &mut registry);
    m.paused(Some(Pause::LowDisk));
    m.busy(1);
    m.ended("job", &Ended::Reported(FinalState::Succeeded));
    m.ended("job", &Ended::Fenced);
    m.stage("job", "bundle", Duration::from_millis(30));
    m.stage("job", "Not-a-stage 1", Duration::from_millis(30));
    let dir = std::env::temp_dir().join("qo-runner-metrics");
    let text = logging::metrics::MetricsFile::new("engine-test", &dir, registry).render();
    let samples = logging::metrics::samples(&text).unwrap();
    let ours: Vec<_> = samples
        .iter()
        .filter(|s| s.name.starts_with("orchestrator_runner_"))
        .collect();
    assert!(ours
        .iter()
        .any(|s| s.name == "orchestrator_runner_claim_paused"
            && s.label("reason") == Some("low_disk")
            && s.value == 1.0));
    assert!(ours
        .iter()
        .any(|s| s.name == "orchestrator_runner_claim_paused"
            && s.label("reason") == Some("low_memory")
            && s.value == 0.0));
    assert!(
        ours.iter()
            .any(|s| s.name == "orchestrator_runner_runs_total"
                && s.label("result") == Some("fenced"))
    );
    let bundle_count = ours.iter().find(|s| {
        s.name == "orchestrator_runner_stage_seconds_count" && s.label("stage") == Some("bundle")
    });
    assert_eq!(bundle_count.map(|s| s.value), Some(1.0));
    assert!(ours
        .iter()
        .any(|s| s.name == "orchestrator_runner_stage_seconds_bucket"
            && s.label("le") == Some("0.05")
            && s.value == 1.0));
    for s in &ours {
        for (key, value) in &s.labels {
            let fixed = match key.as_str() {
                "engine" => value == engine_runner::contract::FAKE_ENGINE,
                "line" => value == "-",
                "kind" => value == "job",
                "reason" => ["low_disk", "low_memory"].contains(&value.as_str()),
                "result" => ["succeeded", "failed", "timed_out", "cancelled", "fenced"]
                    .contains(&value.as_str()),
                "stage" => value == "bundle",
                // A histogram's bucket bound.
                "le" => value.parse::<f64>().is_ok() || value == "+Inf",
                _ => false,
            };
            assert!(fixed, "{}: {key}={value}", s.name);
        }
    }
}
