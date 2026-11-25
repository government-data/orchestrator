//! engine-trino's rules service: Trino's access rules from the core's
//! grants, served to the coordinator on the module's own network.
//!
//! Settings: `CORE_URL` (the core's `internal` entry point, with `/api`),
//! `QD_OIDC_ISSUER`, `QD_OIDC_MACHINE_CLIENT_ID` and the machine secret file,
//! `QD_MACHINE_DIR` (`machine.pem`, `ca.crt`; default `/run/qd-node`),
//! `RULES_LISTEN` (default `0.0.0.0:8070`), `RULES_MODULE_GRANTS` (a JSON
//! file of engine modules' grants; optional).

use std::path::Path;

use engine_trino_service::{core_from_env, machine_client, rules, setting_or};
use trino_rules::Grant;

#[tokio::main]
async fn main() {
    logging::init("engine-trino-rules");
    if let Err(e) = serve().await {
        tracing::error!(error = %e, "the rules service stopped");
        std::process::exit(1);
    }
}

async fn serve() -> Result<(), String> {
    let dir = setting_or("QD_MACHINE_DIR", "/run/qd-node");
    let core = core_from_env(machine_client(Path::new(&dir))?)?;
    let modules: Vec<Grant> = match std::env::var("RULES_MODULE_GRANTS") {
        Ok(f) => {
            let text = std::fs::read_to_string(&f).map_err(|e| format!("{f}: {e}"))?;
            serde_json::from_str(&text).map_err(|e| format!("{f}: {e}"))?
        }
        Err(_) => Vec::new(),
    };
    let listen = setting_or("RULES_LISTEN", "0.0.0.0:8070");
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|e| format!("{listen}: {e}"))?;
    tracing::info!(listen = %listen, "serving Trino's rules");
    axum::serve(listener, rules::router(core, modules))
        .await
        .map_err(|e| e.to_string())
}
