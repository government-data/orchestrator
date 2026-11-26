//! engine-trino's services and their adapters (decision 0011): the core's
//! routes for the job runner and the rules service over mTLS with the
//! machine's certificate, and Trino's protocol through `trino-client`.
//!
//! The core's routes (on its `internal` entry point, under `CORE_URL`),
//! each called with a machine token of the module's OIDC client for the
//! audience `quality-jobs` (client credentials at the provider):
//!
//! | Call | Answer |
//! | --- | --- |
//! | `POST /internal/quality/v1/runs/{run}/bundle` `{kind, dataset?, cut?, attempt}` | `{"job_id", "digest", "bundle": <compact JWS>}` (core #192) |
//! | `POST /internal/jobs/{job}/token` `{"as": "job" \| "report"}` | `{"token": <JWT>}` |
//! | `GET /internal/jobs/{job}/credentials` | `{"credentials": [{"name", "current", "previous"}]}` |
//! | `GET /internal/trino/grants` | `{"grants": [<neutral grant>…]}` |
//!
//! The last three are the shapes the user accepted on 2026-11-25 for the
//! core to build. The provider's keys are its JWKS (`<issuer>/oauth/jwks`).

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use trino_client::TrinoClient;
use trino_runner::{
    Answer, Core, CoreError, Credential, JobRequest, Keys, RunAs, SignedBundle, Trino, TrinoFailure,
};

pub mod guard;
pub mod rules;

/// An HTTP client that presents the machine's certificate (`machine.pem`:
/// certificate then key) and trusts the installation's CA (`ca.crt`), both
/// in the node agent's directory.
pub fn machine_client(dir: &Path) -> Result<reqwest::Client, String> {
    let read = |f: &str| std::fs::read(dir.join(f)).map_err(|e| format!("{f}: {e}"));
    let identity = reqwest::Identity::from_pem(&read("machine.pem")?)
        .map_err(|_| "machine.pem: not a certificate and key".to_string())?;
    let ca = reqwest::Certificate::from_pem(&read("ca.crt")?)
        .map_err(|_| "ca.crt: not a certificate".to_string())?;
    reqwest::Client::builder()
        .use_rustls_tls()
        .identity(identity)
        .add_root_certificate(ca)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())
}

/// The module's OIDC machine client (decision 0009): its id and secret.
#[derive(Clone)]
pub struct MachineClient {
    pub id: String,
    pub secret: String,
}

impl std::fmt::Debug for MachineClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MachineClient")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// The audience of the core's job routes.
pub const JOBS_AUDIENCE: &str = "quality-jobs";

/// The core's routes for the job runner and the rules service.
#[derive(Debug, Clone)]
pub struct HttpCore {
    http: reqwest::Client,
    base: String,
    issuer: String,
    client: MachineClient,
    /// The machine token and when it is no longer used.
    token: std::sync::Arc<Mutex<Option<(String, tokio::time::Instant)>>>,
}

fn failed(what: &str, e: reqwest::Error) -> CoreError {
    CoreError(format!("{what}: {}", e.without_url()))
}

impl HttpCore {
    /// The core at `base` (its internal entry point, `/api`), its provider
    /// at `issuer`.
    #[must_use]
    pub fn new(http: reqwest::Client, base: &str, issuer: &str, client: MachineClient) -> Self {
        Self {
            http,
            base: base.trim_end_matches('/').to_string(),
            issuer: issuer.trim_end_matches('/').to_string(),
            client,
            token: std::sync::Arc::default(),
        }
    }

    /// A machine token for the job routes, kept until a minute before it
    /// expires.
    async fn machine_token(&self) -> Result<String, CoreError> {
        let mut held = self.token.lock().await;
        if let Some((t, until)) = held.as_ref() {
            if tokio::time::Instant::now() < *until {
                return Ok(t.clone());
            }
        }
        let res = self
            .http
            .post(format!("{}/oauth/token", self.issuer))
            .basic_auth(&self.client.id, Some(&self.client.secret))
            .form(&[
                ("grant_type", "client_credentials"),
                ("audience", JOBS_AUDIENCE),
            ])
            .send()
            .await
            .map_err(|e| failed("token", e))?;
        if !res.status().is_success() {
            return Err(CoreError(format!("token: HTTP {}", res.status().as_u16())));
        }
        let v: Value = res.json().await.map_err(|e| failed("token", e))?;
        let token = v["access_token"]
            .as_str()
            .ok_or_else(|| CoreError("token: shape".into()))?
            .to_string();
        let secs = v["expires_in"]
            .as_u64()
            .unwrap_or(300)
            .saturating_sub(60)
            .max(10);
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
        *held = Some((token.clone(), until));
        Ok(token)
    }

    async fn get(&self, path: &str, body: Option<Value>) -> Result<Value, CoreError> {
        let url = format!("{}{path}", self.base);
        let req = match body {
            Some(b) => self.http.post(url).json(&b),
            None => self.http.get(url),
        };
        let res = req
            .bearer_auth(self.machine_token().await?)
            .send()
            .await
            .map_err(|e| failed(path, e))?;
        if !res.status().is_success() {
            return Err(CoreError(format!("{path}: HTTP {}", res.status().as_u16())));
        }
        res.json().await.map_err(|e| failed(path, e))
    }

    /// The core's neutral grants for people and job users.
    pub async fn grants(&self) -> Result<Vec<trino_rules::Grant>, CoreError> {
        let v = self.get("/internal/trino/grants", None).await?;
        serde_json::from_value(v["grants"].clone()).map_err(|_| CoreError("grants: shape".into()))
    }
}

fn job_path(job: &str, what: &str) -> Result<String, CoreError> {
    let safe = job.len() == 36 && job.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-');
    if safe {
        Ok(format!("/internal/jobs/{job}/{what}"))
    } else {
        Err(CoreError("not a job id".into()))
    }
}

#[derive(Deserialize)]
struct WireCredential {
    name: String,
    current: String,
    previous: Option<String>,
}

#[async_trait]
impl Core for HttpCore {
    async fn bundle(&self, run: &str, job: &JobRequest) -> Result<SignedBundle, CoreError> {
        let safe = !run.is_empty()
            && run.len() <= 64
            && run
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if !safe {
            return Err(CoreError("not a run id".into()));
        }
        let body = json!({"kind": job.kind, "dataset": job.dataset, "cut": job.cut,
            "attempt": job.attempt});
        let path = format!("/internal/quality/v1/runs/{run}/bundle");
        let v = self.get(&path, Some(body)).await?;
        match (v["bundle"].as_str(), v["digest"].as_str()) {
            (Some(jws), Some(digest)) => Ok(SignedBundle {
                jws: jws.to_string(),
                digest: digest.to_string(),
            }),
            _ => Err(CoreError("bundle: shape".into())),
        }
    }

    async fn keys(&self) -> Result<Keys, CoreError> {
        let res = self
            .http
            .get(format!("{}/oauth/jwks", self.issuer))
            .send()
            .await
            .map_err(|e| failed("jwks", e))?;
        if !res.status().is_success() {
            return Err(CoreError(format!("jwks: HTTP {}", res.status().as_u16())));
        }
        let v: Value = res.json().await.map_err(|e| failed("jwks", e))?;
        Ok(Keys::from_jwks(&v))
    }

    async fn token(&self, job: &str, run_as: RunAs) -> Result<String, CoreError> {
        let who = match run_as {
            RunAs::Job => "job",
            RunAs::Report => "report",
        };
        let v = self
            .get(&job_path(job, "token")?, Some(json!({"as": who})))
            .await?;
        v["token"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| CoreError("token: shape".into()))
    }

    async fn credentials(&self, job: &str) -> Result<Vec<Credential>, CoreError> {
        let v = self.get(&job_path(job, "credentials")?, None).await?;
        let list: Vec<WireCredential> = serde_json::from_value(v["credentials"].clone())
            .map_err(|_| CoreError("credentials: shape".into()))?;
        Ok(list
            .into_iter()
            .map(|c| Credential {
                name: c.name,
                current: c.current,
                previous: c.previous,
            })
            .collect())
    }
}

/// Trino through `trino-client`.
#[derive(Debug, Clone)]
pub struct ClientTrino(pub TrinoClient);

fn failure(e: &trino_client::TrinoError) -> TrinoFailure {
    TrinoFailure {
        kind: e.kind().to_string(),
    }
}

#[async_trait]
impl Trino for ClientTrino {
    async fn run(
        &self,
        token: &str,
        sql: &str,
        extra: &[(String, String)],
        started: &(dyn for<'q> Fn(&'q str) + Send + Sync),
    ) -> Result<Answer, TrinoFailure> {
        let extra: Vec<(&str, &str)> = extra
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();
        let a = self
            .0
            .run(token, sql, &extra, |id| started(id))
            .await
            .map_err(|e| failure(&e))?;
        Ok(Answer {
            query_id: a.query_id,
            update_type: a.update_type,
            columns: a
                .columns
                .into_iter()
                .map(|c| (c.name, c.type_name))
                .collect(),
            rows: a.rows,
        })
    }

    async fn kill(&self, token: &str, id: &str) -> Result<(), TrinoFailure> {
        self.0.kill(token, id).await.map_err(|e| failure(&e))
    }

    async fn ended(&self, token: &str, id: &str) -> Result<bool, TrinoFailure> {
        self.0.query_ended(token, id).await.map_err(|e| failure(&e))
    }
}

/// A setting from the environment, or why the service cannot start.
pub fn setting(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is not set"))
}

/// A setting, or its default.
#[must_use]
pub fn setting_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// The module's machine client from the agent's environment and secret
/// file (`QD_OIDC_MACHINE_CLIENT_ID`, `/run/secrets/oidc-machine-secret`).
pub fn machine_client_credentials() -> Result<MachineClient, String> {
    let file = setting_or(
        "QD_OIDC_MACHINE_SECRET_FILE",
        "/run/secrets/oidc-machine-secret",
    );
    let secret = std::fs::read_to_string(&file).map_err(|e| format!("{file}: {e}"))?;
    Ok(MachineClient {
        id: setting("QD_OIDC_MACHINE_CLIENT_ID")?,
        secret: secret.trim().to_string(),
    })
}

/// The core from the environment: `CORE_URL`, `QD_OIDC_ISSUER` and the
/// machine client, over `http`.
pub fn core_from_env(http: reqwest::Client) -> Result<HttpCore, String> {
    Ok(HttpCore::new(
        http,
        &setting("CORE_URL")?,
        &setting("QD_OIDC_ISSUER")?,
        machine_client_credentials()?,
    ))
}
