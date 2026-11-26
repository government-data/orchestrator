//! The core's statement bundle (core decision 0090, point 2; ruled by the
//! user 2026-11-25): a job's ordered statements as a compact JWS, signed by
//! the core's OIDC provider with its ES256 key (`typ` `qd-bundle+jwt`, the
//! signature over `header.payload`). The runner checks the header, finds the
//! key by `kid` in the provider's JWKS, verifies the signature, then the
//! payload's digest and its job, and runs nothing that is not in a bundle
//! that verified.

use std::collections::BTreeMap;

use base64::Engine as _;
use ring::signature::{UnparsedPublicKey, ECDSA_P256_SHA256_FIXED};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// The JWS `typ` of a bundle.
pub const BUNDLE_TYP: &str = "qd-bundle+jwt";

/// A bundle as the core answers it: the compact JWS and the digest it
/// pinned for the run (the SHA-256 of the payload's bytes).
#[derive(Clone, PartialEq, Eq)]
pub struct SignedBundle {
    pub jws: String,
    pub digest: String,
}

impl std::fmt::Debug for SignedBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedBundle")
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// What a statement must answer (point 7.3), checked before the next.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expects {
    /// A write: one of these update types with one `bigint` row, the count.
    Count { update_types: Vec<String> },
    /// A read: exactly these columns (name and type), at most `max_rows`.
    Rows {
        columns: Vec<(String, String)>,
        max_rows: u64,
    },
    /// A statement that changes the catalog or a table's layout: no rows.
    Done,
}

/// Who a statement runs as: the job's own user, or its dataset's report
/// user, which reads gold only (point 7.3: counts are read back as
/// `job_report_<d>`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunAs {
    #[default]
    Job,
    Report,
}

/// One statement of a bundle.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    /// Its id in the bundle (`[a-z0-9_]`), named in logs and reports.
    pub id: String,
    pub sql: String,
    pub expects: Expects,
    #[serde(default)]
    pub run_as: RunAs,
    /// Whether it reads the source and so takes its secrets as extra
    /// credentials (point 7.5).
    #[serde(default)]
    pub reads_source: bool,
    /// On a `DELETE`: run only if the earlier count it names was not 0
    /// (amendment A1); otherwise it is skipped.
    #[serde(default)]
    pub only_if_rows: Option<OnlyIfRows>,
}

/// The count a `DELETE` waits on.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnlyIfRows {
    pub after: String,
}

/// A placeholder's type: both are decimal integers of at most 19 digits;
/// only a `bigint` may be negative. There is no text type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaceholderType {
    SnapshotId,
    Bigint,
}

/// Where a placeholder's value comes from: a column of an earlier
/// statement's one-row answer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub statement: String,
    pub column: String,
}

/// A `{{name}}` token the runner fills (amendment A1).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placeholder {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: PlaceholderType,
    pub from: Source,
}

impl std::fmt::Debug for Statement {
    // The SQL holds rule literals: never in a log line (point 7.6).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement")
            .field("id", &self.id)
            .field("expects", &self.expects)
            .finish_non_exhaustive()
    }
}

/// A verified bundle: the job and its statements, in order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bundle {
    pub job_id: String,
    pub kind: String,
    pub dataset: Option<String>,
    pub cut: Option<String>,
    pub attempt: u32,
    pub run_id: Option<String>,
    pub run_seq: Option<u64>,
    pub catalog_commit: Option<String>,
    pub compiler_version: String,
    pub statements: Vec<Statement>,
    /// The tokens its statements hold, each filled from an earlier answer.
    #[serde(default)]
    pub placeholders: Vec<Placeholder>,
}

/// Why a bundle is refused: a class, never the bundle's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleRefused {
    /// Not a compact JWS of ES256 and `typ` `qd-bundle+jwt`.
    Header,
    /// Its `kid` is not in the provider's keys.
    UnknownKey,
    /// The signature does not verify with that key.
    Signature,
    /// Its payload is not the digest pinned for the run.
    Digest,
    /// It is not a bundle, or not this job's.
    Content,
}

impl BundleRefused {
    #[must_use]
    pub fn class(self) -> &'static str {
        match self {
            Self::Header => "bundle_not_a_jws",
            Self::UnknownKey => "bundle_key_unknown",
            Self::Signature => "bundle_signature_invalid",
            Self::Digest => "bundle_digest_mismatch",
            Self::Content => "bundle_invalid",
        }
    }
}

/// The provider's P-256 public keys by `kid`, from its JWKS (other keys,
/// Ed25519 among them, are left out).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Keys(BTreeMap<String, Vec<u8>>);

fn b64url(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .ok()
}

impl Keys {
    /// The EC P-256 keys of a JWKS document (`{"keys": [...]}`).
    #[must_use]
    pub fn from_jwks(jwks: &Value) -> Self {
        let mut keys = BTreeMap::new();
        for k in jwks["keys"].as_array().into_iter().flatten() {
            let text = |f: &str| k[f].as_str();
            if text("kty") != Some("EC") || text("crv") != Some("P-256") {
                continue;
            }
            let (Some(kid), Some(x), Some(y)) = (text("kid"), text("x"), text("y")) else {
                continue;
            };
            if let (Some(x), Some(y)) = (b64url(x), b64url(y)) {
                if x.len() == 32 && y.len() == 32 {
                    let mut point = vec![0x04];
                    point.extend_from_slice(&x);
                    point.extend_from_slice(&y);
                    keys.insert(kid.to_string(), point);
                }
            }
        }
        Self(keys)
    }

    /// Whether `kid` is one of them.
    #[must_use]
    pub fn has(&self, kid: &str) -> bool {
        self.0.contains_key(kid)
    }
}

/// The SHA-256 of `bytes` in lowercase hex.
#[must_use]
pub fn digest_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn is_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// The `kid` a bundle's header names, if it is one of ES256 and the
/// bundle's `typ`; the caller fetches the keys again for an unknown one.
pub fn bundle_kid(jws: &str) -> Result<String, BundleRefused> {
    let header = jws.split('.').next().ok_or(BundleRefused::Header)?;
    let header: Value = b64url(header)
        .and_then(|h| serde_json::from_slice(&h).ok())
        .ok_or(BundleRefused::Header)?;
    if header["alg"] != "ES256" || header["typ"] != BUNDLE_TYP {
        return Err(BundleRefused::Header);
    }
    header["kid"]
        .as_str()
        .map(str::to_string)
        .ok_or(BundleRefused::Header)
}

impl SignedBundle {
    /// The bundle, once its header is ES256 and the bundle's `typ`, its
    /// signature over `header.payload` verifies with the key its `kid`
    /// names, its payload is `digest`, it is `job`'s, and its statements'
    /// ids are well formed and unique.
    pub fn verify(&self, keys: &Keys, job: &str) -> Result<Bundle, BundleRefused> {
        let kid = bundle_kid(&self.jws)?;
        let parts: Vec<&str> = self.jws.split('.').collect();
        let [header, payload, signature] = parts[..] else {
            return Err(BundleRefused::Header);
        };
        let key = keys.0.get(&kid).ok_or(BundleRefused::UnknownKey)?;
        let signature = b64url(signature).ok_or(BundleRefused::Signature)?;
        let signed = format!("{header}.{payload}");
        UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, key)
            .verify(signed.as_bytes(), &signature)
            .map_err(|_| BundleRefused::Signature)?;
        let payload = b64url(payload).ok_or(BundleRefused::Content)?;
        if digest_hex(&payload) != self.digest.to_ascii_lowercase() {
            return Err(BundleRefused::Digest);
        }
        let bundle: Bundle =
            serde_json::from_slice(&payload).map_err(|_| BundleRefused::Content)?;
        let mut ids = std::collections::BTreeSet::new();
        let ids_ok = bundle
            .statements
            .iter()
            .all(|s| is_id(&s.id) && ids.insert(s.id.as_str()));
        if bundle.job_id != job || bundle.statements.is_empty() || !ids_ok {
            return Err(BundleRefused::Content);
        }
        crate::steps::check(&bundle).map_err(|_| BundleRefused::Content)?;
        Ok(bundle)
    }
}
