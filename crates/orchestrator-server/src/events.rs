//! The core's module events (quality-sdk decision 0017) as the
//! orchestrator uses them: it publishes its own topics (`POST
//! /v1/events`) and reads `core.iam.changed` with a cursor (`GET
//! /v1/events`, `PUT /v1/events/cursors/{topic}`) to drop the people's
//! cached actions. `data` is never logged.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::core_api::CoreToken;
use crate::{CachedActions, Clock, EffectiveActions};

/// The core's topic for a change of its IAM, at its version.
pub const IAM_CHANGED: (&str, u32) = ("core.iam.changed", 1);

/// The longest the core waits on a read with no event.
const WAIT_SECS: u64 = 30;

/// An event the orchestrator publishes, as its outbox keeps it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Event {
    /// A UUID, the same at every try.
    pub id: String,
    pub topic: String,
    pub version: u32,
    pub occurred_at: DateTime<Utc>,
    pub data: Value,
}

/// An event as the core gives it to a consumer.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Received {
    pub seq: i64,
    pub id: String,
}

/// What the core said to a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// `201` or `200`: stored now or before; the outbox lets it go.
    Stored,
    /// `400` or `403`: never stored as it is; kept for a person to see.
    Refused(u16),
    /// No answer, or another status: tried again later.
    Unavailable,
}

/// The core's events, as the orchestrator calls them.
#[async_trait]
pub trait CoreEvents: Send + Sync + 'static {
    async fn publish(&self, event: &Event) -> Published;

    /// The events of `topic` after `after` (or after the confirmed
    /// cursor), waiting up to `wait`; with the cursor to confirm. `None`
    /// when the core did not answer.
    async fn read(
        &self,
        topic: (&str, u32),
        after: Option<i64>,
        wait: u64,
    ) -> Option<(Vec<Received>, i64)>;

    /// Handled `topic` up to `cursor`.
    async fn confirm(&self, topic: (&str, u32), cursor: i64) -> bool;
}

/// The core's `/v1/events` at its base on the internal entry point.
#[derive(Debug, Clone)]
pub struct HttpEvents {
    http: reqwest::Client,
    url: String,
    token: Arc<CoreToken>,
}

impl HttpEvents {
    /// The core at `base`, as [`crate::HttpMachineList::new`]; `http`
    /// needs a timeout longer than a read's 30 s wait.
    #[must_use]
    pub fn new(http: reqwest::Client, base: &str, token: Arc<CoreToken>) -> Self {
        Self {
            http,
            url: format!("{}/v1/events", base.trim_end_matches('/')),
            token,
        }
    }

    async fn answered(
        &self,
        res: Result<reqwest::Response, reqwest::Error>,
    ) -> Option<reqwest::Response> {
        let res = match res {
            Ok(res) => res,
            Err(e) => {
                tracing::warn!("core: {} failed: {}", self.url, e.without_url());
                return None;
            }
        };
        if res.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.token.forget().await;
        }
        Some(res)
    }
}

#[async_trait]
impl CoreEvents for HttpEvents {
    async fn publish(&self, event: &Event) -> Published {
        let Some(token) = self.token.get().await else {
            return Published::Unavailable;
        };
        let res = self.http.post(&self.url).bearer_auth(token).json(event);
        let Some(res) = self.answered(res.send().await).await else {
            return Published::Unavailable;
        };
        match res.status().as_u16() {
            200 | 201 => Published::Stored,
            status @ (400 | 403) => {
                tracing::warn!(topic = event.topic, status, "core: an event was refused");
                Published::Refused(status)
            }
            status => {
                tracing::warn!("core: {} answered HTTP {status}", self.url);
                Published::Unavailable
            }
        }
    }

    async fn read(
        &self,
        (topic, version): (&str, u32),
        after: Option<i64>,
        wait: u64,
    ) -> Option<(Vec<Received>, i64)> {
        #[derive(Deserialize)]
        struct Batch {
            events: Vec<Received>,
            cursor: i64,
        }
        let mut query = vec![
            ("topic", topic.to_string()),
            ("version", version.to_string()),
            ("wait", wait.to_string()),
        ];
        if let Some(after) = after {
            query.push(("after", after.to_string()));
        }
        let req = self.http.get(&self.url).query(&query);
        let res = req.bearer_auth(self.token.get().await?).send().await;
        let res = self.answered(res).await?;
        if res.status() != reqwest::StatusCode::OK {
            tracing::warn!("core: {} answered HTTP {}", self.url, res.status().as_u16());
            return None;
        }
        let batch: Batch = res.json().await.ok()?;
        Some((batch.events, batch.cursor))
    }

    async fn confirm(&self, (topic, version): (&str, u32), cursor: i64) -> bool {
        let Some(token) = self.token.get().await else {
            return false;
        };
        let url = format!("{}/cursors/{topic}", self.url);
        let body = json!({ "version": version, "cursor": cursor });
        let res = self
            .http
            .put(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await;
        self.answered(res)
            .await
            .is_some_and(|r| r.status() == reqwest::StatusCode::NO_CONTENT)
    }
}

/// One read of `core.iam.changed` after `after`: any event drops every
/// cached answer, then the cursor is confirmed. The next `after`, or
/// `None` when the core did not answer (read from the confirmed cursor
/// again: an event seen twice only drops the cache twice).
pub async fn iam_changes_once<E: EffectiveActions, C: Clock>(
    events: &dyn CoreEvents,
    cache: &CachedActions<E, C>,
    after: Option<i64>,
    wait: u64,
) -> Option<i64> {
    let (seen, cursor) = events.read(IAM_CHANGED, after, wait).await?;
    if !seen.is_empty() {
        cache.forget().await;
        tracing::info!(
            events = seen.len(),
            "core IAM changed: permissions read again"
        );
    }
    if Some(cursor) != after && !events.confirm(IAM_CHANGED, cursor).await {
        tracing::warn!("core: the core.iam.changed cursor was not confirmed");
    }
    Some(cursor)
}

/// Reads `core.iam.changed` for as long as the server runs, waiting
/// `retry` after the core did not answer.
pub async fn watch_iam_changes<E: EffectiveActions, C: Clock>(
    events: Arc<dyn CoreEvents>,
    cache: Arc<CachedActions<E, C>>,
    retry: Duration,
) {
    let mut after = None;
    loop {
        match iam_changes_once(&*events, &cache, after, WAIT_SECS).await {
            Some(cursor) => after = Some(cursor),
            None => {
                // The cache may be stale while the core is not read.
                cache.forget().await;
                after = None;
                tokio::time::sleep(retry).await;
            }
        }
    }
}
