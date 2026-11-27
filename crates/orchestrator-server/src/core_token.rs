//! The core's own calls (decision 0011, rulings 100 and 103): the core's
//! machine certificate (`OU=app`, [`CoreMachine`]) and, with it, an access
//! token the core signs itself for its internal client [`CORE_CLIENT`],
//! checked as an engine module's token (EdDSA, the provider's published
//! keys, its issuer, the orchestrator's audience, its expiry). Either alone
//! is refused.

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::HeaderMap;

use crate::token::verified_claims;
use crate::{Authenticate, Clock, CoreMachine, Expected, Keys, Machine};

/// The client id of the core's own token.
pub const CORE_CLIENT: &str = "qd-core";

/// The core's caller: its machine certificate and its own token; both or
/// nothing.
pub struct CoreCaller<K, C> {
    keys: Arc<K>,
    expected: Expected,
    clock: C,
}

impl<K, C> std::fmt::Debug for CoreCaller<K, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreCaller").finish_non_exhaustive()
    }
}

impl<K: Keys, C: Clock> CoreCaller<K, C> {
    pub fn new(keys: K, expected: Expected, clock: C) -> Self {
        Self {
            keys: Arc::new(keys),
            expected,
            clock,
        }
    }

    /// Whether `headers` carry a live token of the core's own client.
    async fn has_core_token(&self, headers: &HeaderMap) -> bool {
        let Some(token) = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        else {
            return false;
        };
        let keys = self.keys.keys().await;
        verified_claims(token, &keys, &self.expected, self.clock.now())
            .and_then(|c| c.get("client_id").cloned())
            .is_some_and(|client| client == CORE_CLIENT)
    }
}

#[async_trait]
impl<K: Keys, C: Clock> Authenticate for CoreCaller<K, C> {
    async fn machine(&self, headers: &HeaderMap) -> Option<Machine> {
        if !self.has_core_token(headers).await {
            return None;
        }
        CoreMachine.machine(headers).await
    }
}
