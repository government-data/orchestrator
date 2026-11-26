//! The runner's metrics: why it pauses, its busy slots and the runs it
//! ended, every label from its fixed list.

use engine_runner::contract::FakeEngine;
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
    for s in &ours {
        for (key, value) in &s.labels {
            let fixed = match key.as_str() {
                "engine" => value == engine_runner::contract::FAKE_ENGINE,
                "line" => value == "-",
                "kind" => value == "job",
                "reason" => ["low_disk", "low_memory"].contains(&value.as_str()),
                "result" => ["succeeded", "failed", "timed_out", "cancelled", "fenced"]
                    .contains(&value.as_str()),
                _ => false,
            };
            assert!(fixed, "{}: {key}={value}", s.name);
        }
    }
}
