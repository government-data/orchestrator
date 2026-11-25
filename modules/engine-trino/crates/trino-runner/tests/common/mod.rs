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
    Answer, BundleKey, Core, CoreError, Credential, RunAs, SignedBundle, Trino, TrinoFailure,
};

pub const JOB: &str = "00000000-0000-4000-8000-00000000000a";

pub fn key() -> EcdsaKeyPair {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap()
}

/// The key's PEM, as the core publishes it.
pub fn public(k: &EcdsaKeyPair) -> BundleKey {
    let mut der = vec![
        0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08,
        0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
    ];
    der.extend_from_slice(k.public_key().as_ref());
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    BundleKey::from_pem(&format!(
        "-----BEGIN PUBLIC KEY-----\n{b64}\n-----END PUBLIC KEY-----\n"
    ))
    .unwrap()
}

pub fn sign(payload: &Value, k: &EcdsaKeyPair) -> SignedBundle {
    let payload = serde_json::to_vec(payload).unwrap();
    let sig = k.sign(&SystemRandom::new(), &payload).unwrap();
    SignedBundle {
        payload,
        signature: sig.as_ref().to_vec(),
    }
}

/// A bundle of kind `evaluate` with these statements.
pub fn bundle(statements: Value) -> Value {
    json!({"job_id": JOB, "kind": "evaluate", "dataset": "loans", "cut": "c1", "attempt": 1,
        "run_id": "r1", "run_seq": 1, "catalog_commit": "abc", "compiler_version": "1.0.0",
        "statements": statements})
}

pub fn write(id: &str, sql: &str) -> Value {
    json!({"id": id, "sql": sql, "expects": {"kind": "count", "update_types": ["INSERT"]}})
}

#[derive(Debug, Clone)]
pub struct FakeCore {
    pub bundle: SignedBundle,
    pub credentials: Vec<Credential>,
}

#[async_trait]
impl Core for FakeCore {
    async fn bundle(&self, _: &str) -> Result<SignedBundle, CoreError> {
        Ok(self.bundle.clone())
    }
    async fn token(&self, _: &str, run_as: RunAs) -> Result<String, CoreError> {
        Ok(match run_as {
            RunAs::Job => "job-token".into(),
            RunAs::Report => "report-token".into(),
        })
    }
    async fn credentials(&self, _: &str) -> Result<Vec<Credential>, CoreError> {
        Ok(self.credentials.clone())
    }
}

/// What a statement answers: a count, an error kind, or waiting until its
/// query is killed.
#[derive(Debug, Clone)]
pub enum Script {
    Count(u64),
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
