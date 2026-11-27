//! [`CoreSchedules`] over the core's HTTP routes (core decision 0090, step
//! 19), under its base on the internal entry point, with the machine
//! client's token ([`CoreToken`]):
//!
//! | Call | Answers |
//! | --- | --- |
//! | `GET /internal/quality/v1/schedules` | `{"schedules": [{"id", "cron", "timezone", "trigger"?, "enabled"}]}` |
//! | `POST /internal/quality/v1/schedules/{id}/fires` `{"fire_at"}` | `{"steps": [{"kind", "parameters", "setup"?: {"key", "fingerprint"}}]}` |
//!
//! Other fields of a schedule are not read. `fire_at` is RFC 3339 in UTC,
//! to the second; the same fire asked twice gets the same steps. A fire of
//! a schedule the core no longer has (`404`) runs nothing; any other
//! status, a malformed body or no token is no answer, so the list is kept
//! and the fire asked again.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use orchestration_model::ChainStep;
use schedule::ScheduleSpec;
use serde::Deserialize;
use serde_json::json;

use crate::core_api::{get_json, CoreToken};
use crate::{is_core_id, CoreFire, CoreSchedule, CoreSchedules};

/// The core's schedules and fires at its base.
#[derive(Debug, Clone)]
pub struct HttpCoreSchedules {
    http: reqwest::Client,
    url: String,
    token: Arc<CoreToken>,
}

impl HttpCoreSchedules {
    /// The core at `base`, as [`crate::HttpMachineList::new`].
    #[must_use]
    pub fn new(http: reqwest::Client, base: &str, token: Arc<CoreToken>) -> Self {
        Self {
            http,
            url: format!(
                "{}/internal/quality/v1/schedules",
                base.trim_end_matches('/')
            ),
            token,
        }
    }
}

#[derive(Deserialize)]
struct Listed {
    schedules: Vec<ListedSchedule>,
}

#[derive(Deserialize)]
struct ListedSchedule {
    id: String,
    #[serde(flatten)]
    spec: ScheduleSpec,
    enabled: bool,
}

#[derive(Deserialize)]
struct Fired {
    steps: Vec<ChainStep>,
}

#[async_trait]
impl CoreSchedules for HttpCoreSchedules {
    async fn schedules(&self) -> Option<Vec<CoreSchedule>> {
        let body = get_json(&self.http, &self.token, &self.url).await?;
        let Ok(listed) = serde_json::from_value::<Listed>(body) else {
            tracing::warn!("core: {} answered an unexpected shape", self.url);
            return None;
        };
        let schedules = listed.schedules.into_iter().map(|s| CoreSchedule {
            id: s.id,
            spec: s.spec,
            enabled: s.enabled,
        });
        Some(schedules.collect())
    }

    async fn fire(&self, id: &str, fire_at: DateTime<Utc>) -> Option<CoreFire> {
        if !is_core_id(id) {
            return None;
        }
        let url = format!("{}/{id}/fires", self.url);
        let body = json!({ "fire_at": fire_at.to_rfc3339_opts(SecondsFormat::Secs, true) });
        let res = self
            .http
            .post(&url)
            .bearer_auth(self.token.get().await?)
            .json(&body)
            .send()
            .await;
        let res = match res {
            Ok(res) => res,
            Err(e) => {
                tracing::warn!("core: {url} failed: {}", e.without_url());
                return None;
            }
        };
        match res.status() {
            reqwest::StatusCode::OK => {}
            reqwest::StatusCode::NOT_FOUND => return Some(CoreFire { steps: Vec::new() }),
            status => {
                if status == reqwest::StatusCode::UNAUTHORIZED {
                    self.token.forget().await;
                }
                tracing::warn!("core: {url} answered HTTP {}", status.as_u16());
                return None;
            }
        }
        match res.json::<Fired>().await {
            Ok(fired) => Some(CoreFire { steps: fired.steps }),
            Err(_) => {
                tracing::warn!("core: {url} answered an unexpected shape");
                None
            }
        }
    }
}
