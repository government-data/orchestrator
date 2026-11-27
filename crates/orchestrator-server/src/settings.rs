//! The server's settings, from the environment the update agent writes
//! for the module (quality-sdk decision 0009) and the files it mounts:
//!
//! | Setting | From |
//! | --- | --- |
//! | the database | `QD_DB_HOST`, `QD_DB_PORT` (5432), `QD_DB_USER`, `QD_DB_NAME`, `QD_DB_CA_FILE`, `QD_DB_SSLMODE` (`verify-full`), the password in `QD_DB_PASSWORD_FILE` (`/run/secrets/database-password`) |
//! | the provider | `QD_OIDC_ISSUER`; people's client `QD_OIDC_PEOPLE_CLIENT_ID` |
//! | the machine client | `QD_OIDC_MACHINE_CLIENT_ID`, its secret in `QD_OIDC_MACHINE_SECRET_FILE` (`/run/secrets/oidc-machine-secret`) |
//! | the core | `CORE_URL`: its API on the internal entry point, `https://<internal name>/api` |
//! | the machine's certificate | `QD_MACHINE_DIR` (`/run/qd-node`): `ca.crt`, `machine.pem` |
//! | the sealing key | `QD_SEALING_KEY_FILE` (`/run/secrets/sealing-key`) |
//! | where it listens | `ORCHESTRATOR_LISTEN` (`0.0.0.0:8080`) |
//! | the flow the core's chains run on | `ORCHESTRATOR_CORE_FLOW` (a flow's id; unset: the core's schedules are not kept and its chains not started) |
//!
//! No secret is kept in a `Debug` or an error.

use std::net::SocketAddr;
use std::path::Path;

use orchestrator_postgres::{PgConnectOptions, PgSslMode};

use crate::{MachineClient, Sealer};

/// What the server runs with.
pub struct Settings {
    pub database: PgConnectOptions,
    /// The core's provider.
    pub issuer: String,
    /// The people's client id: a person's token names it.
    pub people_client: String,
    pub machine_client: MachineClient,
    /// The core's API on its internal entry point.
    pub core_base: String,
    pub sealer: Sealer,
    pub listen: SocketAddr,
    /// The flow the core's schedules fire on and its chains start on.
    pub core_flow: Option<String>,
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("issuer", &self.issuer)
            .field("core_base", &self.core_base)
            .field("listen", &self.listen)
            .field("core_flow", &self.core_flow)
            .finish_non_exhaustive()
    }
}

fn setting(name: &str) -> Result<String, String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
        _ => Err(format!("{name} is not set")),
    }
}

fn setting_or(name: &str, default: &str) -> String {
    setting(name).unwrap_or_else(|_| default.to_string())
}

/// A secret file's content, trimmed; the error names the file only.
fn secret_file(name: &str, default: &str) -> Result<String, String> {
    let path = setting_or(name, default);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {}", e.kind()))?;
    Ok(text.trim().to_string())
}

fn ssl_mode(mode: &str) -> Result<PgSslMode, String> {
    match mode {
        "verify-full" => Ok(PgSslMode::VerifyFull),
        "verify-ca" => Ok(PgSslMode::VerifyCa),
        "require" => Ok(PgSslMode::Require),
        "disable" => Ok(PgSslMode::Disable),
        other => Err(format!(
            "QD_DB_SSLMODE {other:?}: verify-full, verify-ca, require or disable"
        )),
    }
}

impl Settings {
    /// The settings from the environment and the files it names.
    pub fn from_env() -> Result<Self, String> {
        let port = setting_or("QD_DB_PORT", "5432")
            .parse::<u16>()
            .map_err(|_| "QD_DB_PORT: a port".to_string())?;
        let mut database = PgConnectOptions::new()
            .host(&setting("QD_DB_HOST")?)
            .port(port)
            .username(&setting("QD_DB_USER")?)
            .database(&setting("QD_DB_NAME")?)
            .password(&secret_file(
                "QD_DB_PASSWORD_FILE",
                "/run/secrets/database-password",
            )?)
            .ssl_mode(ssl_mode(&setting_or("QD_DB_SSLMODE", "verify-full"))?);
        if let Ok(ca) = setting("QD_DB_CA_FILE") {
            database = database.ssl_root_cert(ca);
        }
        let sealer = Sealer::from_file_content(&secret_file(
            "QD_SEALING_KEY_FILE",
            "/run/secrets/sealing-key",
        )?)
        .ok_or("the sealing key is not 32 bytes in base64url")?;
        Ok(Self {
            database,
            issuer: setting("QD_OIDC_ISSUER")?,
            people_client: setting("QD_OIDC_PEOPLE_CLIENT_ID")?,
            machine_client: MachineClient {
                id: setting("QD_OIDC_MACHINE_CLIENT_ID")?,
                secret: secret_file(
                    "QD_OIDC_MACHINE_SECRET_FILE",
                    "/run/secrets/oidc-machine-secret",
                )?,
            },
            core_base: setting("CORE_URL")?,
            sealer,
            listen: setting_or("ORCHESTRATOR_LISTEN", "0.0.0.0:8080")
                .parse()
                .map_err(|_| "ORCHESTRATOR_LISTEN: an address and port".to_string())?,
            core_flow: core_flow()?,
        })
    }
}

/// `ORCHESTRATOR_CORE_FLOW`, a flow's id, when set.
fn core_flow() -> Result<Option<String>, String> {
    let Ok(flow) = setting("ORCHESTRATOR_CORE_FLOW") else {
        return Ok(None);
    };
    uuid::Uuid::parse_str(&flow)
        .map(|id| Some(id.to_string()))
        .map_err(|_| "ORCHESTRATOR_CORE_FLOW: a flow's id".to_string())
}

/// An HTTP client that presents the machine's certificate (`machine.pem`:
/// certificate then key) and trusts the installation's CA (`ca.crt`), both
/// in `dir`; with a timeout longer than an events read's wait.
pub fn machine_http(dir: &Path) -> Result<reqwest::Client, String> {
    let read = |f: &str| std::fs::read(dir.join(f)).map_err(|e| format!("{f}: {}", e.kind()));
    let identity = reqwest::Identity::from_pem(&read("machine.pem")?)
        .map_err(|_| "machine.pem: not a certificate and key".to_string())?;
    let ca = reqwest::Certificate::from_pem(&read("ca.crt")?)
        .map_err(|_| "ca.crt: not a certificate".to_string())?;
    reqwest::Client::builder()
        .use_rustls_tls()
        .identity(identity)
        .add_root_certificate(ca)
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(|e| e.to_string())
}

/// The machine's directory: `QD_MACHINE_DIR` or `/run/qd-node`.
#[must_use]
pub fn machine_dir() -> String {
    setting_or("QD_MACHINE_DIR", "/run/qd-node")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_modules_environment_and_keeps_its_secrets_out_of_debug() {
        let dir = std::env::temp_dir().join(format!("qo-settings-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = |name: &str, text: &str| {
            let path = dir.join(name);
            std::fs::write(&path, text).unwrap();
            path.display().to_string()
        };
        let vars = [
            ("QD_DB_HOST", "postgres".to_string()),
            ("QD_DB_USER", "orchestrator".into()),
            ("QD_DB_NAME", "orchestrator".into()),
            ("QD_DB_PASSWORD_FILE", file("db", "db-password-1\n")),
            ("QD_OIDC_ISSUER", "https://qd.example/api".into()),
            ("QD_OIDC_PEOPLE_CLIENT_ID", "orchestrator".into()),
            ("QD_OIDC_MACHINE_CLIENT_ID", "orchestrator-machine".into()),
            (
                "QD_OIDC_MACHINE_SECRET_FILE",
                file("machine", "machine-secret-1"),
            ),
            ("CORE_URL", "https://master.example/api".into()),
            ("QD_SEALING_KEY_FILE", file("key", &"A".repeat(43))),
        ];
        for (k, v) in &vars {
            std::env::set_var(k, v);
        }
        let s = Settings::from_env().unwrap();
        assert_eq!(s.listen.port(), 8080);
        assert_eq!(s.machine_client.secret, "machine-secret-1");
        assert_eq!(s.database.get_host(), "postgres");
        let shown = format!("{s:?} {:?}", s.machine_client);
        assert!(
            !shown.contains("secret-1") && !shown.contains("password-1"),
            "{shown}"
        );
        assert_eq!(s.core_flow, None);
        let flow = "0b9d6a52-2f5e-4c1e-9d8a-1f2e3d4c5b6a";
        std::env::set_var("ORCHESTRATOR_CORE_FLOW", flow);
        assert_eq!(
            Settings::from_env().unwrap().core_flow.as_deref(),
            Some(flow)
        );
        std::env::set_var("ORCHESTRATOR_CORE_FLOW", "daily");
        assert!(Settings::from_env()
            .unwrap_err()
            .contains("ORCHESTRATOR_CORE_FLOW"));
        std::env::remove_var("ORCHESTRATOR_CORE_FLOW");
        std::env::set_var("QD_DB_SSLMODE", "prefer-not");
        assert!(Settings::from_env().unwrap_err().contains("QD_DB_SSLMODE"));
        for (k, _) in &vars {
            std::env::remove_var(k);
        }
        std::env::remove_var("QD_DB_SSLMODE");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
