//! A bundle verifies only with its digest, the core's key and its job;
//! answers fit their statements; errors are classed by name.

use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::{EncodePublicKey, LineEnding};
use serde_json::json;
use trino_runner::{
    class, digest_hex, fits, Answer, BundleKey, BundleRefused, Expects, SignedBundle,
};

const JOB: &str = "00000000-0000-4000-8000-00000000000a";

fn key() -> SigningKey {
    SigningKey::from_slice(&[7u8; 32]).unwrap()
}

fn public(k: &SigningKey) -> BundleKey {
    let pem = k.verifying_key().to_public_key_pem(LineEnding::LF).unwrap();
    BundleKey::from_pem(&pem).unwrap()
}

fn signed(payload: serde_json::Value, k: &SigningKey) -> SignedBundle {
    let payload = serde_json::to_vec(&payload).unwrap();
    let sig: Signature = k.sign(&payload);
    SignedBundle {
        payload,
        signature: sig.to_bytes().to_vec(),
    }
}

fn payload() -> serde_json::Value {
    json!({
        "job_id": JOB, "kind": "evaluate", "dataset": "loans", "cut": "2026-11",
        "attempt": 1, "run_id": "r1", "run_seq": 4, "catalog_commit": "abc",
        "compiler_version": "1.2.0",
        "statements": [
            {"id": "insert_icd", "sql": "INSERT INTO lake.quality_scratch.record_icd SELECT 1",
             "expects": {"kind": "count", "update_types": ["INSERT"]}},
            {"id": "report", "sql": "SELECT count(*) FROM lake.quality_gold.run_summary",
             "expects": {"kind": "rows", "columns": [["n", "bigint"]], "max_rows": 1},
             "run_as": "report"}
        ]
    })
}

#[test]
fn a_bundle_verifies_with_its_digest_key_and_job() {
    let k = key();
    let b = signed(payload(), &k);
    let d = digest_hex(&b.payload);
    let bundle = b.verify(&public(&k), &d, JOB).unwrap();
    assert_eq!(bundle.statements.len(), 2);
    assert_eq!(bundle.statements[1].run_as, trino_runner::RunAs::Report);
    assert!(!format!("{:?}", bundle.statements[0]).contains("INSERT INTO"));
}

#[test]
fn a_tampered_or_foreign_bundle_is_refused() {
    let k = key();
    let b = signed(payload(), &k);
    let d = digest_hex(&b.payload);
    let mut tampered = b.clone();
    tampered.payload[10] ^= 1;
    let td = digest_hex(&tampered.payload);
    assert_eq!(
        tampered.verify(&public(&k), &d, JOB),
        Err(BundleRefused::Digest)
    );
    assert_eq!(
        tampered.verify(&public(&k), &td, JOB),
        Err(BundleRefused::Signature)
    );
    let other = SigningKey::from_slice(&[9u8; 32]).unwrap();
    assert_eq!(
        b.verify(&public(&other), &d, JOB),
        Err(BundleRefused::Signature)
    );
    assert_eq!(
        b.verify(&public(&k), &d, "another-job"),
        Err(BundleRefused::Content)
    );
    let mut twice = payload();
    twice["statements"][1]["id"] = json!("insert_icd");
    let t = signed(twice, &k);
    assert_eq!(
        t.verify(&public(&k), &digest_hex(&t.payload), JOB),
        Err(BundleRefused::Content)
    );
    assert_eq!(BundleRefused::Signature.class(), "bundle_signature_invalid");
}

fn count(update: &str, cols: &[(&str, &str)], rows: serde_json::Value) -> Answer {
    Answer {
        query_id: Some("q".into()),
        update_type: Some(update.into()),
        columns: cols
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect(),
        rows: serde_json::from_value(rows).unwrap(),
    }
}

#[test]
fn answers_fit_their_statement_or_not() {
    let write = Expects::Count {
        update_types: vec!["INSERT".into(), "DELETE".into()],
    };
    assert_eq!(
        fits(
            &count("INSERT", &[("rows", "bigint")], json!([[12]])),
            &write
        ),
        Some(12)
    );
    assert_eq!(
        fits(
            &count("UPDATE", &[("rows", "bigint")], json!([[1]])),
            &write
        ),
        None
    );
    assert_eq!(
        fits(
            &count("INSERT", &[("rows", "varchar")], json!([["1"]])),
            &write
        ),
        None
    );
    assert_eq!(
        fits(
            &count("INSERT", &[("rows", "bigint")], json!([[1], [2]])),
            &write
        ),
        None
    );
    let mut read = count("", &[("n", "bigint")], json!([[1]]));
    read.update_type = None;
    let rows = Expects::Rows {
        columns: vec![("n".into(), "bigint".into())],
        max_rows: 1,
    };
    assert_eq!(fits(&read, &rows), Some(0));
    read.rows.push(vec![json!(2)]);
    assert_eq!(fits(&read, &rows), None, "more rows than allowed");
    read.columns[0].0 = "value".into();
    assert_eq!(fits(&read, &rows), None, "another column");
}

#[test]
fn errors_are_classed_by_name() {
    for k in [
        "unavailable",
        "gone",
        "SERVER_STARTING_UP",
        "CLUSTER_OUT_OF_MEMORY",
        "QUERY_QUEUE_FULL",
        "REMOTE_TASK_ERROR",
    ] {
        assert!(class(k).transient, "{k} is repeated");
    }
    assert_eq!(
        class("EXCEEDED_LOCAL_MEMORY_LIMIT").class,
        "too_big_for_tier"
    );
    assert!(!class("TRANSACTION_CONFLICT").transient);
    assert_eq!(class("PERMISSION_DENIED").class, "permission_denied");
    assert_eq!(class("SYNTAX_ERROR").class, "syntax_error");
    assert_eq!(class("DIVISION_BY_ZERO").class, "statement_failed");
}
