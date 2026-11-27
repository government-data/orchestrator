//! Who may call the people's API (decision 0011, step 6). A person is
//! signed in at the core's OIDC provider (quality-sdk decision 0009) and
//! calls with an access token of the orchestrator's people client; what
//! they may do is which of the orchestrator's actions the core says they
//! hold now (`/v1/iam/effective`, quality-sdk decision 0002,
//! "Permissions"), cached at most 60 s and dropped when the core says its
//! IAM changed. The core's access tokens carry no module role claim, so an
//! action is never read from the token.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::token::verified_claims;
use crate::{Clock, Expected, Keys};

/// Load what a cluster already has as flows (decision 0011, import).
pub const IMPORT_FLOWS: &str = "orchestrator:ImportFlows";

/// How long the core's answer for a person is kept.
const CACHED_FOR: Duration = Duration::seconds(60);

/// A person signed in at the core: their account id, as the `sub` of
/// their tokens. Never logged.
#[derive(Clone, PartialEq, Eq)]
pub struct Person {
    pub id: String,
}

impl std::fmt::Debug for Person {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Person").finish_non_exhaustive()
    }
}

/// Reads the person who calls from a request's headers: `None` when no
/// valid person's token is there.
#[async_trait]
pub trait PersonTokens: Send + Sync + 'static {
    async fn person(&self, headers: &HeaderMap) -> Option<Person>;
}

/// Which of the orchestrator's actions a person holds, as the core says.
#[async_trait]
pub trait EffectiveActions: Send + Sync + 'static {
    /// The held actions; `None` when the core did not answer.
    async fn held(&self, person: &Person) -> Option<Vec<String>>;
}

/// A person's access token of the people client `expected.audience`
/// (the module's name), signed by the core's provider with EdDSA: issued
/// to that client (`client_id`), not exchanged (no `act`), with a `sub`.
/// A module's machine token is not a person's.
pub struct OidcPeople<K, C> {
    keys: Arc<K>,
    expected: Expected,
    clock: C,
}

impl<K, C> std::fmt::Debug for OidcPeople<K, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcPeople").finish_non_exhaustive()
    }
}

impl<K: Keys, C: Clock> OidcPeople<K, C> {
    pub fn new(keys: K, expected: Expected, clock: C) -> Self {
        Self {
            keys: Arc::new(keys),
            expected,
            clock,
        }
    }
}

/// The person `token` names, if it is a live person's access token of
/// the people client `e.audience`.
#[must_use]
pub fn person_of(
    token: &str,
    keys: &[(String, Vec<u8>)],
    e: &Expected,
    now: DateTime<Utc>,
) -> Option<Person> {
    let c = verified_claims(token, keys, e, now)?;
    let own_client = c.get("client_id")? == &Value::String(e.audience.clone());
    let sub = c.get("sub")?.as_str()?;
    let person = own_client && c.get("act").is_none() && !sub.is_empty();
    person.then(|| Person { id: sub.into() })
}

#[async_trait]
impl<K: Keys, C: Clock> PersonTokens for OidcPeople<K, C> {
    async fn person(&self, headers: &HeaderMap) -> Option<Person> {
        let bearer = headers.get("authorization")?.to_str().ok()?;
        let token = bearer.strip_prefix("Bearer ")?;
        let keys = self.keys.keys().await;
        person_of(token, &keys, &self.expected, self.clock.now())
    }
}

/// The actions a person held, by account id, and when the core said so.
type Answers = HashMap<String, (DateTime<Utc>, Vec<String>)>;

/// The core's answer per person, kept [`CACHED_FOR`]; [`Self::forget`]
/// drops every answer (on `core.iam.changed`). No answer is not kept.
pub struct CachedActions<E, C> {
    inner: E,
    clock: C,
    held: Mutex<Answers>,
}

impl<E, C> std::fmt::Debug for CachedActions<E, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedActions").finish_non_exhaustive()
    }
}

impl<E: EffectiveActions, C: Clock> CachedActions<E, C> {
    pub fn new(inner: E, clock: C) -> Self {
        Self {
            inner,
            clock,
            held: Mutex::new(HashMap::new()),
        }
    }

    /// Drops every kept answer: the next call asks the core.
    pub async fn forget(&self) {
        self.held.lock().await.clear();
    }
}

#[async_trait]
impl<E: EffectiveActions, C: Clock> EffectiveActions for CachedActions<E, C> {
    async fn held(&self, person: &Person) -> Option<Vec<String>> {
        let now = self.clock.now();
        let mut held = self.held.lock().await;
        held.retain(|_, (at, _)| now - *at < CACHED_FOR);
        if let Some((_, actions)) = held.get(&person.id) {
            return Some(actions.clone());
        }
        let actions = self.inner.held(person).await?;
        held.insert(person.id.clone(), (now, actions.clone()));
        Some(actions)
    }
}

/// Why a person's call is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// No valid person's token: 401.
    NotSignedIn,
    /// The person does not hold the action: 403.
    NotAllowed,
    /// The core did not say what the person holds: 503.
    Unavailable,
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        let (status, why) = match self {
            Self::NotSignedIn => (StatusCode::UNAUTHORIZED, "not signed in"),
            Self::NotAllowed => (StatusCode::FORBIDDEN, "not allowed"),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "permissions cannot be read now",
            ),
        };
        (status, Json(json!({ "error": why }))).into_response()
    }
}

/// The people's API's check: who calls, and whether they hold an action.
#[derive(Clone)]
pub struct People {
    tokens: Arc<dyn PersonTokens>,
    actions: Arc<dyn EffectiveActions>,
}

impl std::fmt::Debug for People {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("People").finish_non_exhaustive()
    }
}

impl People {
    pub fn new(tokens: Arc<dyn PersonTokens>, actions: Arc<dyn EffectiveActions>) -> Self {
        Self { tokens, actions }
    }

    /// The caller, if they are signed in and hold `action` now.
    pub async fn allow(&self, headers: &HeaderMap, action: &str) -> Result<Person, Refused> {
        let person = self
            .tokens
            .person(headers)
            .await
            .ok_or(Refused::NotSignedIn)?;
        let held = self
            .actions
            .held(&person)
            .await
            .ok_or(Refused::Unavailable)?;
        if held.iter().any(|a| a == action) {
            Ok(person)
        } else {
            Err(Refused::NotAllowed)
        }
    }
}

#[cfg(test)]
#[path = "people_tests.rs"]
mod tests;
