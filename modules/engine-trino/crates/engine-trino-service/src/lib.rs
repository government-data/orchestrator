//! engine-trino's services and their adapters (decision 0011): the core's
//! routes for the job runner and the rules service over mTLS with the
//! machine's certificate, and Trino's protocol through `trino-client`.
//!
//! The core's routes (on its `internal` entry point, under `CORE_URL`),
//! each called with a machine token of the module's OIDC client for the
//! audience `core` (core decision 0090, ruling 97; client credentials at
//! the provider):
//!
//! | Call | Answer |
//! | --- | --- |
//! | `POST /internal/quality/v1/runs/{run}/bundle` `{kind, dataset?, cut?, source?, attempt}` | `{"job_id", "digest", "bundle": <compact JWS>}` |
//! | `POST /internal/quality/v1/runs/{run}/trino-token` `{"user": "rules" \| "report" \| "maint" \| "extract" \| "source"}` | `{"access_token": <JWT>, "expires_in"}` |
//! | `POST /internal/quality/v1/runs/{run}/source-secrets` | `{"current": {<name>: <value>}, "previous": {…} \| null}` |
//! | `POST /internal/quality/v1/runs/{run}/rows` `{statement, columns?, rows}` | `204`; a values read's rows, before the report (`columns`, the answer's names, for a text read only) |
//! | `POST /internal/quality/v1/runs/{run}/document` | `{"document"}` once; `410 gone` after (an `entity_key` run's typed document, never logged) |
//! | `POST /internal/quality/v1/runs/{run}/report` `{digest, state, statements, snapshots, error_class?}` | `204`; `409 already_reported` when kept before |
//! | `GET /internal/quality/v1/job-grants` | `{"grants": [<neutral grant>…]}` |
//!
//! The provider's keys are its JWKS (`<issuer>/oauth/jwks`).

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use trino_client::TrinoClient;
use trino_runner::{
    Answer, Core, CoreError, Credential, Delivery, Document, JobRequest, Keys, RunReport,
    SignedBundle, Trino, TrinoFailure, TrinoUser,
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

/// The audience of the core's job routes (core decision 0090, ruling 97:
/// every `/internal/quality/v1/*` call).
pub const JOBS_AUDIENCE: &str = "core";
/// The audience of the core's job grants route, the same (ruling 97).
pub const GRANTS_AUDIENCE: &str = JOBS_AUDIENCE;

/// The core's routes for the job runner and the rules service.
#[derive(Debug, Clone)]
pub struct HttpCore {
    http: reqwest::Client,
    base: String,
    issuer: String,
    client: MachineClient,
    /// The machine token and when it is no longer used.
    /// The machine tokens by audience, each until it is no longer used.
    tokens: std::sync::Arc<Mutex<BTreeMap<&'static str, (String, tokio::time::Instant)>>>,
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
            tokens: std::sync::Arc::default(),
        }
    }

    /// A machine token for `audience`, kept until a minute before it
    /// expires.
    async fn machine_token(&self, audience: &'static str) -> Result<String, CoreError> {
        let mut held = self.tokens.lock().await;
        if let Some((t, until)) = held.get(audience) {
            if tokio::time::Instant::now() < *until {
                return Ok(t.clone());
            }
        }
        let res = self
            .http
            .post(format!("{}/oauth/token", self.issuer))
            .basic_auth(&self.client.id, Some(&self.client.secret))
            .form(&[("grant_type", "client_credentials"), ("audience", audience)])
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
        held.insert(audience, (token.clone(), until));
        Ok(token)
    }

    async fn get(&self, path: &str, body: Option<Value>) -> Result<Value, CoreError> {
        self.call(JOBS_AUDIENCE, path, body).await
    }

    async fn call(
        &self,
        audience: &'static str,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, CoreError> {
        let url = format!("{}{path}", self.base);
        let req = match body {
            Some(b) => self.http.post(url).json(&b),
            None => self.http.get(url),
        };
        let res = req
            .bearer_auth(self.machine_token(audience).await?)
            .send()
            .await
            .map_err(|e| failed(path, e))?;
        if !res.status().is_success() {
            return Err(CoreError(format!("{path}: HTTP {}", res.status().as_u16())));
        }
        res.json().await.map_err(|e| failed(path, e))
    }

    /// Posts a run's report or rows: kept, refused for good (a 4xx but
    /// 408 and 429, by the core's error name; `already_reported` is kept
    /// for a report), or not answered.
    async fn post_kept(
        &self,
        path: &str,
        body: &impl serde::Serialize,
        report: bool,
    ) -> Result<Delivery, CoreError> {
        let res = self
            .http
            .post(format!("{}{path}", self.base))
            .bearer_auth(self.machine_token(JOBS_AUDIENCE).await?)
            .json(body)
            .send()
            .await
            .map_err(|e| failed(path, e))?;
        let status = res.status();
        if status.is_success() {
            return Ok(Delivery::Kept);
        }
        let retried = status.is_server_error() || matches!(status.as_u16(), 408 | 429);
        if retried || !status.is_client_error() {
            return Err(CoreError(format!("{path}: HTTP {}", status.as_u16())));
        }
        let error = res
            .json::<Value>()
            .await
            .ok()
            .and_then(|v| v["error"].as_str().map(str::to_string))
            // A name only: nothing else of the core's answer reaches a log.
            .filter(|e| e.len() <= 64 && e.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'))
            .unwrap_or_else(|| format!("http_{}", status.as_u16()));
        Ok(if report && error == "already_reported" {
            Delivery::Kept
        } else {
            Delivery::Refused(error)
        })
    }

    /// The core's neutral grants for the job users.
    pub async fn grants(&self) -> Result<Vec<trino_rules::Grant>, CoreError> {
        let v = self
            .call(GRANTS_AUDIENCE, "/internal/quality/v1/job-grants", None)
            .await?;
        serde_json::from_value(v["grants"].clone()).map_err(|_| CoreError("grants: shape".into()))
    }
}

/// A run's route on the core, for a run id that is safe in a path.
fn run_path(run: &str, what: &str) -> Result<String, CoreError> {
    let safe = !run.is_empty()
        && run.len() <= 64
        && run
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if safe {
        Ok(format!("/internal/quality/v1/runs/{run}/{what}"))
    } else {
        Err(CoreError("not a run id".into()))
    }
}

/// A source's extra credentials by name: the current ones and, for one
/// retry on a refused login, the previous ones.
#[derive(Deserialize)]
struct WireSecrets {
    current: BTreeMap<String, String>,
    #[serde(default)]
    previous: Option<BTreeMap<String, String>>,
}

#[async_trait]
impl Core for HttpCore {
    async fn bundle(&self, run: &str, job: &JobRequest) -> Result<SignedBundle, CoreError> {
        let mut body = json!({"kind": job.kind, "dataset": job.dataset, "cut": job.cut,
            "attempt": job.attempt});
        if let Some(source) = &job.source {
            body["source"] = json!(source);
        }
        let v = self.get(&run_path(run, "bundle")?, Some(body)).await?;
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

    async fn token(&self, job: &str, user: TrinoUser) -> Result<String, CoreError> {
        let body = json!({"user": user.as_str()});
        let v = self.get(&run_path(job, "trino-token")?, Some(body)).await?;
        v["access_token"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| CoreError("token: shape".into()))
    }

    async fn report(&self, job: &str, report: &RunReport) -> Result<Delivery, CoreError> {
        self.post_kept(&run_path(job, "report")?, report, true)
            .await
    }

    async fn rows(
        &self,
        job: &str,
        statement: &str,
        columns: Option<&[String]>,
        rows: &[Vec<Value>],
    ) -> Result<Delivery, CoreError> {
        let mut body = json!({"statement": statement, "rows": rows});
        if let Some(names) = columns {
            body["columns"] = json!(names);
        }
        self.post_kept(&run_path(job, "rows")?, &body, false).await
    }

    async fn document(&self, job: &str) -> Result<Option<Document>, CoreError> {
        let path = run_path(job, "document")?;
        let res = self
            .http
            .post(format!("{}{path}", self.base))
            .bearer_auth(self.machine_token(JOBS_AUDIENCE).await?)
            .json(&json!({}))
            .send()
            .await
            .map_err(|e| failed("document", e))?;
        match res.status().as_u16() {
            410 => return Ok(None),
            s if !res.status().is_success() => {
                return Err(CoreError(format!("document: HTTP {s}")));
            }
            _ => {}
        }
        // A fixed error only: nothing of the answer reaches a log.
        let v: Value = res
            .json()
            .await
            .map_err(|_| CoreError("document: shape".into()))?;
        match v {
            Value::Object(mut o) => match o.remove("document") {
                Some(Value::String(d)) => Ok(Some(Document::new(d))),
                _ => Err(CoreError("document: shape".into())),
            },
            _ => Err(CoreError("document: shape".into())),
        }
    }

    async fn credentials(&self, job: &str) -> Result<Vec<Credential>, CoreError> {
        let v = self
            .get(&run_path(job, "source-secrets")?, Some(json!({})))
            .await?;
        let mut s: WireSecrets =
            serde_json::from_value(v).map_err(|_| CoreError("source secrets: shape".into()))?;
        Ok(s.current
            .into_iter()
            .map(|(name, current)| Credential {
                previous: s.previous.as_mut().and_then(|p| p.remove(&name)),
                name,
                current,
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
