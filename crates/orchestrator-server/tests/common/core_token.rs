//! The core's own token (rulings 100 and 103) for tests: an Ed25519 key
//! the core signs with, the keys the orchestrator reads, and the caller
//! that checks both the core's machine and its token.

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use orchestrator_server::{CoreCaller, Expected, FixedClock, Keys, CORE_CLIENT};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{json, Value};

pub const ISSUER: &str = "https://core.test/api";
pub const AUDIENCE: &str = "orchestrator";

/// The provider's key, with its id.
pub struct CoreKey(Ed25519KeyPair);

impl CoreKey {
    pub fn new() -> Self {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        Self(Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap())
    }

    /// A token with `claims`, signed with this key.
    pub fn sign(&self, claims: &Value) -> String {
        let header = json!({"alg": "EdDSA", "typ": "at+jwt", "kid": "k1"});
        let input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header.to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let sig = URL_SAFE_NO_PAD.encode(self.0.sign(input.as_bytes()).as_ref());
        format!("{input}.{sig}")
    }

    /// A live token of `client` for the orchestrator, at `clock`'s time.
    pub fn token_of(&self, client: &str, clock: &FixedClock) -> String {
        use orchestrator_server::Clock as _;
        let exp = clock.now().timestamp() + 900;
        self.sign(&json!({"iss": ISSUER, "aud": AUDIENCE, "client_id": client,
                          "sub": client, "exp": exp}))
    }

    /// The core's own token.
    pub fn core_token(&self, clock: &FixedClock) -> String {
        self.token_of(CORE_CLIENT, clock)
    }

    /// The caller the orchestrator checks the core with.
    pub fn caller(&self, clock: FixedClock) -> CoreCaller<StaticKeys, FixedClock> {
        let keys = StaticKeys(vec![("k1".into(), self.0.public_key().as_ref().to_vec())]);
        let expected = Expected {
            issuer: ISSUER.into(),
            audience: AUDIENCE.into(),
        };
        CoreCaller::new(keys, expected, clock)
    }
}

/// Keys known up front.
pub struct StaticKeys(Vec<(String, Vec<u8>)>);

#[async_trait]
impl Keys for StaticKeys {
    async fn keys(&self) -> Vec<(String, Vec<u8>)> {
        self.0.clone()
    }
}
