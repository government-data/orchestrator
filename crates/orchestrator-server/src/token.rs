//! The engine module a call comes from: its machine client's access token
//! (quality-sdk decision 0009: client `<module>-machine`, client
//! credentials), signed by the core's provider with EdDSA and checked here
//! against the provider's published keys, its issuer, its audience (the
//! `token-audience` the orchestrator declares) and its expiry.

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::HeaderMap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde_json::Value;

use crate::{Authenticate, Clock, Machine, TraefikMachine};

/// The suffix of a module's machine client id.
pub(crate) const MACHINE_CLIENT_SUFFIX: &str = "-machine";

/// Ed25519 public keys by key id.
pub type KeyList = Vec<(String, Vec<u8>)>;

/// The provider's Ed25519 public keys, by key id.
#[async_trait]
pub trait Keys: Send + Sync + 'static {
    async fn keys(&self) -> Vec<(String, Vec<u8>)>;
}

/// What a valid machine token must say.
#[derive(Debug, Clone)]
pub struct Expected {
    pub issuer: String,
    pub audience: String,
}

/// The module whose machine client signed `token`'s request, if `token` is
/// valid at `now`.
pub fn module_of(
    token: &str,
    keys: &[(String, Vec<u8>)],
    e: &Expected,
    now: DateTime<Utc>,
) -> Option<String> {
    let mut parts = token.split('.');
    let (header, claims, signature) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let head: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).ok()?).ok()?;
    if head.get("alg")? != "EdDSA" || head.get("typ")? != "at+jwt" {
        return None;
    }
    let kid = head.get("kid")?.as_str()?;
    let (_, key) = keys.iter().find(|(k, _)| k == kid)?;
    let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
    UnparsedPublicKey::new(&ED25519, key)
        .verify(format!("{header}.{claims}").as_bytes(), &signature)
        .ok()?;
    let c: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims).ok()?).ok()?;
    let audience_ok = match c.get("aud")? {
        Value::String(a) => a == &e.audience,
        Value::Array(all) => all.iter().any(|a| a == &Value::String(e.audience.clone())),
        _ => false,
    };
    let live = c.get("exp")?.as_i64()? > now.timestamp();
    if c.get("iss")? != &Value::String(e.issuer.clone()) || !audience_ok || !live {
        return None;
    }
    let client = c.get("client_id")?.as_str()?;
    let module = client.strip_suffix(MACHINE_CLIENT_SUFFIX)?;
    (!module.is_empty()).then(|| module.to_string())
}

/// The engine port's caller: the machine by its certificate (Traefik's
/// header) and the engine module by its machine client's token; both or
/// nothing.
pub struct InternalCaller<K, C> {
    keys: Arc<K>,
    expected: Expected,
    clock: C,
}

impl<K, C> std::fmt::Debug for InternalCaller<K, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalCaller").finish_non_exhaustive()
    }
}

impl<K: Keys, C: Clock> InternalCaller<K, C> {
    pub fn new(keys: K, expected: Expected, clock: C) -> Self {
        Self {
            keys: Arc::new(keys),
            expected,
            clock,
        }
    }

    async fn module(&self, headers: &HeaderMap) -> Option<String> {
        let bearer = headers.get("authorization")?.to_str().ok()?;
        let token = bearer.strip_prefix("Bearer ")?;
        let keys = self.keys.keys().await;
        module_of(token, &keys, &self.expected, self.clock.now())
    }
}

#[async_trait]
impl<K: Keys, C: Clock> Authenticate for InternalCaller<K, C> {
    async fn machine(&self, headers: &HeaderMap) -> Option<Machine> {
        self.module(headers).await?;
        TraefikMachine.machine(headers).await
    }

    async fn module(&self, headers: &HeaderMap) -> Option<String> {
        Self::module(self, headers).await
    }
}

/// The provider's published keys (`<issuer>/oauth/jwks`), fetched at most
/// every five minutes, kept when a fetch fails.
#[derive(Debug)]
pub struct JwksKeys {
    client: reqwest::Client,
    url: String,
    cache: tokio::sync::Mutex<(Option<std::time::Instant>, KeyList)>,
}

impl JwksKeys {
    #[must_use]
    pub fn new(client: reqwest::Client, issuer: &str) -> Self {
        Self {
            client,
            url: format!("{}/oauth/jwks", issuer.trim_end_matches('/')),
            cache: tokio::sync::Mutex::new((None, Vec::new())),
        }
    }

    async fn fetch(&self) -> Option<Vec<(String, Vec<u8>)>> {
        let jwks: Value = self
            .client
            .get(&self.url)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        Some(ed25519_keys(&jwks))
    }
}

/// The Ed25519 keys of a JWKS document, by key id.
#[must_use]
pub fn ed25519_keys(jwks: &Value) -> Vec<(String, Vec<u8>)> {
    let Some(keys) = jwks.get("keys").and_then(Value::as_array) else {
        return Vec::new();
    };
    keys.iter()
        .filter(|k| {
            k.get("kty") == Some(&Value::from("OKP"))
                && k.get("crv") == Some(&Value::from("Ed25519"))
        })
        .filter_map(|k| {
            let kid = k.get("kid")?.as_str()?.to_string();
            let x = URL_SAFE_NO_PAD.decode(k.get("x")?.as_str()?).ok()?;
            Some((kid, x))
        })
        .collect()
}

#[async_trait]
impl Keys for JwksKeys {
    async fn keys(&self) -> Vec<(String, Vec<u8>)> {
        let mut cache = self.cache.lock().await;
        let fresh = cache
            .0
            .is_some_and(|t| t.elapsed() < std::time::Duration::from_secs(300));
        if !fresh {
            match self.fetch().await {
                Some(keys) => *cache = (Some(std::time::Instant::now()), keys),
                None => tracing::warn!("machine tokens: the provider's keys were not fetched"),
            }
        }
        cache.1.clone()
    }
}

#[cfg(test)]
#[path = "token_tests.rs"]
mod tests;
