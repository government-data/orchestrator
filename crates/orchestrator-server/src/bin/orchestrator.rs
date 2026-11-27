//! The orchestrator's server (decision 0011): the engine port, the core's
//! calls and the people's API on one listener, with the scheduler, the
//! outbox sender, the watch of the core's IAM changes and the metrics
//! file. Its settings are the module's environment (`Settings`).

use std::path::Path;

use orchestrator_server::{machine_dir, machine_http, serve, stop_signal, Every, Settings};

#[tokio::main]
async fn main() {
    orchestrator_server::init_logs();
    if let Err(e) = run().await {
        tracing::error!(error = %e, "the orchestrator stopped");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let settings = Settings::from_env()?;
    let http = machine_http(Path::new(&machine_dir()))?;
    serve(settings, http, Every::default(), stop_signal()).await
}
