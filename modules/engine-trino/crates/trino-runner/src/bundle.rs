//! The core's statement bundle (core decision 0090, point 2): a job's
//! ordered statements, signed by the core with ES256. The operator hands
//! out only the job's id and the bundle's digest; the runner fetches the
//! bundle from the core, checks the digest and the signature, and runs
//! nothing that is not in a bundle that verified.

use base64::Engine as _;
use ring::signature::{UnparsedPublicKey, ECDSA_P256_SHA256_FIXED};
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// A bundle as fetched: the signed bytes and the signature over them, the
/// raw `r || s` of ES256 (64 bytes).
#[derive(Clone, PartialEq, Eq)]
pub struct SignedBundle {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}

impl std::fmt::Debug for SignedBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedBundle")
            .field("bytes", &self.payload.len())
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
}

/// Why a bundle is refused: a class, never the bundle's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleRefused {
    /// Its bytes are not the digest the operator gave.
    Digest,
    /// The signature does not verify with the core's key.
    Signature,
    /// It is not a bundle, or not this job's.
    Content,
}

impl BundleRefused {
    #[must_use]
    pub fn class(self) -> &'static str {
        match self {
            Self::Digest => "bundle_digest_mismatch",
            Self::Signature => "bundle_signature_invalid",
            Self::Content => "bundle_invalid",
        }
    }
}

/// The DER prefix of a P-256 public key's SubjectPublicKeyInfo, before its
/// uncompressed point (`04 || x || y`).
const P256_SPKI_PREFIX: [u8; 26] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a,
    0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
];

/// The core's public key for bundles: a P-256 point, from its PEM
/// (SubjectPublicKeyInfo).
#[derive(Debug, Clone)]
pub struct BundleKey(Vec<u8>);

impl BundleKey {
    pub fn from_pem(pem: &str) -> Result<Self, String> {
        let refused = || "not a P-256 public key in PEM".to_string();
        let body: String = pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<Vec<_>>()
            .concat();
        let der = base64::engine::general_purpose::STANDARD
            .decode(body.trim())
            .map_err(|_| refused())?;
        let point = der
            .strip_prefix(&P256_SPKI_PREFIX[..])
            .ok_or_else(refused)?;
        if point.len() != 65 || point[0] != 0x04 {
            return Err(refused());
        }
        Ok(Self(point.to_vec()))
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

impl SignedBundle {
    /// The bundle, once its digest is `digest`, its signature verifies with
    /// `key`, it is `job`'s, and its statements' ids are well formed and
    /// unique.
    pub fn verify(
        &self,
        key: &BundleKey,
        digest: &str,
        job: &str,
    ) -> Result<Bundle, BundleRefused> {
        if digest_hex(&self.payload) != digest.to_ascii_lowercase() {
            return Err(BundleRefused::Digest);
        }
        UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, &key.0)
            .verify(&self.payload, &self.signature)
            .map_err(|_| BundleRefused::Signature)?;
        let bundle: Bundle =
            serde_json::from_slice(&self.payload).map_err(|_| BundleRefused::Content)?;
        let mut ids = std::collections::BTreeSet::new();
        let ids_ok = bundle
            .statements
            .iter()
            .all(|s| is_id(&s.id) && ids.insert(s.id.as_str()));
        if bundle.job_id != job || bundle.statements.is_empty() || !ids_ok {
            return Err(BundleRefused::Content);
        }
        Ok(bundle)
    }
}
