//! engine-trino's job runner: claims runs of the engine `trino` from the
//! orchestrator and runs the core's signed bundles on the coordinator.
//!
//! Settings: `ORCHESTRATOR_URL` (its route on the `internal` entry point),
//! `CORE_URL`, `TRINO_URL` (`https://<coordinator>:8490`), `TRINO_VERSION`
//! (the coordinator's release), `QD_OIDC_ISSUER` (the provider, whose JWKS
//! verifies the bundles), `QD_OIDC_MACHINE_CLIENT_ID` and the machine
//! secret file, `QD_MACHINE_DIR` (default `/run/qd-node`),
//! `RUNNER_CONCURRENCY` (default 1), `RUNNER_NAME` (default the host name),
//! `TRINO_TIER` (default `s`: the coordinator's tier, which sizes the
//! `rules` pool sent at start),
//! `QD_WORKER_MIN_FREE_BYTES` and `QD_WORKER_MIN_FREE_MEMORY_PCT` (the
//! guard's minimums), `QD_METRICS_DIR` (where its metrics file
//! `engine-trino.prom` goes, `off` for none).

use std::path::Path;
use std::time::Duration;

use engine_runner::{metrics_file, Engine, HttpOrchestrator, Runner, RunnerMetrics};
use engine_trino_service::{core_from_env, machine_client, setting, setting_or, ClientTrino};
use trino_client::TrinoClient;
use trino_runner::{pools, TrinoEngine};

#[tokio::main]
async fn main() {
    logging::init("engine-trino-runner");
    if let Err(e) = run().await {
        tracing::error!(error = %e, "the job runner stopped");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let dir = setting_or("QD_MACHINE_DIR", "/run/qd-node");
    let client = machine_client(Path::new(&dir))?;
    let ca = std::fs::read(Path::new(&dir).join("ca.crt")).map_err(|e| format!("ca.crt: {e}"))?;
    let concurrency = setting_or("RUNNER_CONCURRENCY", "1")
        .parse()
        .map_err(|_| "RUNNER_CONCURRENCY: a number".to_string())?;
    let pools = pools(&setting_or("TRINO_TIER", "s"))?;
    let engine = TrinoEngine::new(
        core_from_env(client.clone())?,
        ClientTrino(TrinoClient::new(&setting("TRINO_URL")?, Some(&ca))?),
        &setting("TRINO_VERSION")?,
        concurrency,
    );
    let mut registry = prometheus_client::registry::Registry::default();
    let metrics = RunnerMetrics::register(&engine.descriptor(), &mut registry);
    if let Some(file) = metrics_file("engine-trino", registry) {
        file.spawn();
    }
    let orchestrator = HttpOrchestrator::new(client, &setting("ORCHESTRATOR_URL")?);
    let name = setting_or("RUNNER_NAME", &setting_or("HOSTNAME", "runner"));
    let runner = Runner::new(
        engine,
        orchestrator,
        engine_trino_service::guard::from_env()?,
        &name,
    )
    .with_metrics(metrics);
    while let Err(e) = runner.register().await {
        tracing::warn!(error = %e, "registration refused or unreachable; again in 10 s");
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    // The pools go before the first claim, so the rule kinds are admitted
    // by the tier's R from the start; the same pools again at every start.
    while let Err(e) = runner.send_pools(&pools).await {
        tracing::warn!(error = %e, "pools refused or unreachable; again in 10 s");
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    tracing::info!("registered and pools sent; claiming runs");
    loop {
        match runner.claim_once().await {
            Ok(Some(_)) => {}
            Ok(None) => tokio::time::sleep(Duration::from_secs(2)).await,
            Err(e) => {
                tracing::warn!(error = %e, "claim failed; again in 10 s");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
