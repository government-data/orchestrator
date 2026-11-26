//! The core's API as the orchestrator calls it (decisions 0011 and 0015):
//! `GET /v1/machines` and `GET /v1/modules` under the core's base on its
//! internal entry point (`https://<internal name>/api`, with the machine's
//! certificate), each with an access token of the module's machine client
//! (`<module>-machine`, client credentials) for the audience `core`.
//!
//! The server binary builds one [`CoreToken`] and shares it between
//! [`HttpMachineList`] (set with `EnginePort::with_machines`) and
//! [`HttpModuleList`] (set with `Scheduler::with_modules`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::{MachineList, ModuleList};

/// The audience of the core's API for a module's machine client.
pub const CORE_AUDIENCE: &str = "core";

/// A token is no longer used this long before it expires.
const MARGIN: Duration = Duration::from_secs(60);

/// The module's OIDC machine client (quality-sdk decision 0009): its id
/// and secret. The secret never appears in `Debug`.
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

/// The machine client's access token for the core, from the provider's
/// `<issuer>/oauth/token` (client credentials), kept until a minute before
/// it expires. Shared by the core's adapters.
#[derive(Debug)]
pub struct CoreToken {
    http: reqwest::Client,
    url: String,
    client: MachineClient,
    held: Mutex<Option<(String, Instant)>>,
}

impl CoreToken {
    #[must_use]
    pub fn new(http: reqwest::Client, issuer: &str, client: MachineClient) -> Self {
        Self {
            http,
            url: format!("{}/oauth/token", issuer.trim_end_matches('/')),
            client,
            held: Mutex::new(None),
        }
    }

    /// The token, fetched again when none is held or it is about to
    /// expire; `None` when the provider did not give one.
    pub async fn get(&self) -> Option<String> {
        let mut held = self.held.lock().await;
        if let Some((token, until)) = held.as_ref() {
            if Instant::now() < *until {
                return Some(token.clone());
            }
        }
        let fetched = self.fetch().await;
        if fetched.is_none() {
            tracing::warn!("core token: the provider gave no token");
        }
        *held = fetched;
        held.as_ref().map(|(t, _)| t.clone())
    }

    /// Drops the held token (the core refused it), so the next call
    /// fetches another.
    pub async fn forget(&self) {
        *self.held.lock().await = None;
    }

    async fn fetch(&self) -> Option<(String, Instant)> {
        let res = self
            .http
            .post(&self.url)
            .basic_auth(&self.client.id, Some(&self.client.secret))
            .form(&[
                ("grant_type", "client_credentials"),
                ("audience", CORE_AUDIENCE),
            ])
            .send()
            .await
            .ok()?;
        if !res.status().is_success() {
            return None;
        }
        let v: Value = res.json().await.ok()?;
        let token = v.get("access_token")?.as_str()?.to_string();
        let secs = v.get("expires_in").and_then(Value::as_u64).unwrap_or(300);
        let keep = Duration::from_secs(secs).saturating_sub(MARGIN);
        Some((token, Instant::now() + keep.max(Duration::from_secs(10))))
    }
}

/// `GET url` with the core token: the body when the core answers 200 with
/// JSON, `None` on any error (a 401 also drops the token).
pub(crate) async fn get_json(
    http: &reqwest::Client,
    token: &CoreToken,
    url: &str,
) -> Option<Value> {
    let res = http.get(url).bearer_auth(token.get().await?).send().await;
    let res = match res {
        Ok(res) => res,
        Err(e) => {
            tracing::warn!("core: {} failed: {}", url, e.without_url());
            return None;
        }
    };
    let status = res.status();
    if status != reqwest::StatusCode::OK {
        if status == reqwest::StatusCode::UNAUTHORIZED {
            token.forget().await;
        }
        tracing::warn!("core: {url} answered HTTP {}", status.as_u16());
        return None;
    }
    res.json().await.ok()
}

/// The `name` of each item of `body[key]`, an array of objects; `None`
/// when the body has another shape.
fn names(body: Value, key: &str, url: &str) -> Option<Vec<String>> {
    #[derive(Deserialize)]
    struct Named {
        name: String,
    }
    let items = body
        .get(key)
        .cloned()
        .map(serde_json::from_value::<Vec<Named>>);
    if let Some(Ok(items)) = items {
        return Some(items.into_iter().map(|n| n.name).collect());
    }
    tracing::warn!("core: {url} answered an unexpected shape");
    None
}

/// The core's `GET /v1/machines`: every machine not retired.
#[derive(Debug, Clone)]
pub struct HttpMachineList {
    http: reqwest::Client,
    url: String,
    token: Arc<CoreToken>,
}

impl HttpMachineList {
    /// The core at `base` (its API on the internal entry point, ending in
    /// `/api`), over `http` (the machine's certificate).
    #[must_use]
    pub fn new(http: reqwest::Client, base: &str, token: Arc<CoreToken>) -> Self {
        Self {
            http,
            url: format!("{}/v1/machines", base.trim_end_matches('/')),
            token,
        }
    }
}

#[async_trait]
impl MachineList for HttpMachineList {
    /// `{"machines": [{"name", "role", "state"}]}`: the names.
    async fn live(&self) -> Option<Vec<String>> {
        let body = get_json(&self.http, &self.token, &self.url).await?;
        names(body, "machines", &self.url)
    }
}

/// The core's `GET /v1/modules`: every installed module.
#[derive(Debug, Clone)]
pub struct HttpModuleList {
    http: reqwest::Client,
    url: String,
    token: Arc<CoreToken>,
}

impl HttpModuleList {
    /// The core at `base`, as [`HttpMachineList::new`].
    #[must_use]
    pub fn new(http: reqwest::Client, base: &str, token: Arc<CoreToken>) -> Self {
        Self {
            http,
            url: format!("{}/v1/modules", base.trim_end_matches('/')),
            token,
        }
    }
}

#[async_trait]
impl ModuleList for HttpModuleList {
    /// `{"modules": [{"name", "version"}]}`: the names.
    async fn installed(&self) -> Option<Vec<String>> {
        let body = get_json(&self.http, &self.token, &self.url).await?;
        names(body, "modules", &self.url)
    }
}
