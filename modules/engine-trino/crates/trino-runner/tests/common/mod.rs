//! A signing core, a scripted Trino and helpers for the engine's tests.
#![allow(dead_code, unreachable_pub)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use serde_json::{json, Value};
use tokio::sync::Notify;
use trino_runner::{
    digest_hex, Answer, Core, CoreError, Credential, Delivery, JobRequest, Keys, RunReport,
    SignedBundle, Trino, TrinoFailure, TrinoUser, BUNDLE_TYP,
};

/// A values read's rows.
pub type Rows = Vec<Vec<Value>>;

pub const JOB: &str = "00000000-0000-4000-8000-00000000000a";

pub fn key() -> EcdsaKeyPair {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap()
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// The provider's JWKS with `k` as `kid` (and an Ed25519 key, left out).
pub fn jwks(k: &EcdsaKeyPair, kid: &str) -> Value {
    let point = k.public_key().as_ref();
    json!({"keys": [
        {"kty": "OKP", "crv": "Ed25519", "kid": "ed", "x": b64(&[1; 32])},
        {"kty": "EC", "crv": "P-256", "kid": kid, "alg": "ES256",
         "x": b64(&point[1..33]), "y": b64(&point[33..65])}
    ]})
}

pub fn keys(k: &EcdsaKeyPair, kid: &str) -> Keys {
    Keys::from_jwks(&jwks(k, kid))
}

/// A compact JWS of `payload` with this header, signed over
/// `header.payload` as the core's provider signs.
pub fn jws_with(header: &Value, payload: &Value, k: &EcdsaKeyPair) -> SignedBundle {
    let body = serde_json::to_vec(payload).unwrap();
    let signed = format!(
        "{}.{}",
        b64(&serde_json::to_vec(header).unwrap()),
        b64(&body)
    );
    let sig = k.sign(&SystemRandom::new(), signed.as_bytes()).unwrap();
    SignedBundle {
        jws: format!("{signed}.{}", b64(sig.as_ref())),
        digest: digest_hex(&body),
    }
}

pub fn sign(payload: &Value, k: &EcdsaKeyPair) -> SignedBundle {
    jws_with(
        &json!({"alg": "ES256", "typ": BUNDLE_TYP, "kid": "k1"}),
        payload,
        k,
    )
}

/// A bundle of kind `evaluate` with these statements.
pub fn bundle(statements: Value) -> Value {
    json!({"job_id": JOB, "kind": "evaluate", "dataset": "loans", "cut": "2026-10-31",
        "attempt": 1, "run_id": "r1", "run_seq": 1, "catalog_commit": "abc",
        "compiler_version": "1.0.0", "statements": statements})
}

pub fn write(id: &str, sql: &str) -> Value {
    json!({"id": id, "sql": sql, "expects": {"kind": "count", "update_types": ["INSERT"]}})
}

/// A core that compiles `template` for the run it is asked for, signs it
/// with its key `k1`, and serves its JWKS.
#[derive(Clone)]
pub struct FakeCore {
    pub key: Arc<EcdsaKeyPair>,
    pub template: Value,
    pub credentials: Vec<Credential>,
    /// The digest it answers instead of the bundle's own (a broken pin).
    pub digest: Option<String>,
    pub asked: Arc<Mutex<Vec<(String, JobRequest)>>>,
    /// Each report sent, with its run.
    pub reports: Arc<Mutex<Vec<(String, RunReport)>>>,
    /// What it answers each report in turn (`Kept` once these run out).
    pub answers: Arc<Mutex<Vec<Result<Delivery, CoreError>>>>,
    /// Each values read's rows sent, with its statement.
    pub rows: Arc<Mutex<Vec<(String, Rows)>>>,
    /// What it answers each rows post in turn (`Kept` once these run out).
    pub row_answers: Arc<Mutex<Vec<Result<Delivery, CoreError>>>>,
}

impl std::fmt::Debug for FakeCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FakeCore")
    }
}

impl FakeCore {
    pub fn new(template: Value, credentials: Vec<Credential>) -> Self {
        Self {
            key: Arc::new(key()),
            template,
            credentials,
            digest: None,
            asked: Arc::default(),
            reports: Arc::default(),
            answers: Arc::default(),
            rows: Arc::default(),
            row_answers: Arc::default(),
        }
    }
}

#[async_trait]
impl Core for FakeCore {
    async fn bundle(&self, run: &str, job: &JobRequest) -> Result<SignedBundle, CoreError> {
        self.asked.lock().unwrap().push((run.into(), job.clone()));
        let mut payload = self.template.clone();
        payload["job_id"] = json!(run);
        let mut signed = sign(&payload, &self.key);
        if let Some(d) = &self.digest {
            signed.digest.clone_from(d);
        }
        Ok(signed)
    }
    async fn keys(&self) -> Result<Keys, CoreError> {
        Ok(keys(&self.key, "k1"))
    }
    async fn token(&self, _: &str, user: TrinoUser) -> Result<String, CoreError> {
        Ok(match user {
            TrinoUser::Rules => "job-token".into(),
            other => format!("{}-token", other.as_str()),
        })
    }
    async fn credentials(&self, _: &str) -> Result<Vec<Credential>, CoreError> {
        Ok(self.credentials.clone())
    }
    async fn rows(
        &self,
        _: &str,
        statement: &str,
        rows: &[Vec<Value>],
    ) -> Result<Delivery, CoreError> {
        self.rows
            .lock()
            .unwrap()
            .push((statement.into(), rows.to_vec()));
        let mut answers = self.row_answers.lock().unwrap();
        if answers.is_empty() {
            Ok(Delivery::Kept)
        } else {
            answers.remove(0)
        }
    }
    async fn report(&self, run: &str, report: &RunReport) -> Result<Delivery, CoreError> {
        self.reports
            .lock()
            .unwrap()
            .push((run.into(), report.clone()));
        let mut answers = self.answers.lock().unwrap();
        if answers.is_empty() {
            Ok(Delivery::Kept)
        } else {
            answers.remove(0)
        }
    }
}

/// What a statement answers: a count, an error kind, or waiting until its
/// query is killed.
#[derive(Debug, Clone)]
pub enum Script {
    Count(u64),
    /// A one-row read: its columns (name, type) and its row.
    Read(Vec<(&'static str, &'static str)>, Value),
    /// A `DELETE` that removed this many rows.
    Deleted(u64),
    Fail(&'static str),
    Wait,
}

#[derive(Debug, Default)]
pub struct Seen {
    /// Each run: its SQL, token and extra credentials' values.
    pub runs: Vec<(String, String, Vec<String>)>,
    pub kills: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct FakeTrino {
    pub script: Arc<Mutex<HashMap<String, Vec<Script>>>>,
    pub seen: Arc<Mutex<Seen>>,
    pub killed: Arc<Notify>,
}

impl FakeTrino {
    /// `sql` answers each of `answers` in turn, the last one after that.
    pub fn on(&self, sql: &str, answers: Vec<Script>) {
        self.script.lock().unwrap().insert(sql.into(), answers);
    }
}

#[async_trait]
impl Trino for FakeTrino {
    async fn run(
        &self,
        token: &str,
        sql: &str,
        extra: &[(String, String)],
        started: &(dyn for<'q> Fn(&'q str) + Send + Sync),
    ) -> Result<Answer, TrinoFailure> {
        let n = {
            let mut seen = self.seen.lock().unwrap();
            seen.runs.push((
                sql.into(),
                token.into(),
                extra.iter().map(|e| e.1.clone()).collect(),
            ));
            seen.runs.len()
        };
        let id = format!("q_{n}");
        started(&id);
        let next = {
            let mut s = self.script.lock().unwrap();
            let list = s.get_mut(sql).expect("a scripted statement");
            if list.len() > 1 {
                list.remove(0)
            } else {
                list[0].clone()
            }
        };
        match next {
            Script::Count(c) => Ok(Answer {
                query_id: Some(id),
                update_type: Some("INSERT".into()),
                columns: vec![("rows".into(), "bigint".into())],
                rows: vec![vec![json!(c)]],
            }),
            Script::Read(cols, row) => Ok(Answer {
                query_id: Some(id),
                update_type: None,
                columns: cols
                    .iter()
                    .map(|(n, t)| (n.to_string(), t.to_string()))
                    .collect(),
                rows: vec![row.as_array().unwrap().clone()],
            }),
            Script::Deleted(c) => Ok(Answer {
                query_id: Some(id),
                update_type: Some("DELETE".into()),
                columns: vec![("rows".into(), "bigint".into())],
                rows: vec![vec![json!(c)]],
            }),
            Script::Fail(kind) => Err(TrinoFailure { kind: kind.into() }),
            Script::Wait => {
                self.killed.notified().await;
                Err(TrinoFailure {
                    kind: "USER_CANCELED".into(),
                })
            }
        }
    }
    async fn kill(&self, _: &str, id: &str) -> Result<(), TrinoFailure> {
        self.seen.lock().unwrap().kills.push(id.into());
        self.killed.notify_waiters();
        Ok(())
    }
    async fn ended(&self, _: &str, _: &str) -> Result<bool, TrinoFailure> {
        Ok(true)
    }
}
