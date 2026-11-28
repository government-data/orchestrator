//! A bundle verifies only with its digest, the core's key and its job;
//! answers fit their statements; errors are classed by name.

mod common;

use common::{jws_with, key, keys, sign};
use serde_json::json;
use trino_runner::{
    bundle_kid, class, fits, Answer, BundleRefused, Expects, Keys, SignedBundle, BUNDLE_TYP,
};

const JOB: &str = "00000000-0000-4000-8000-00000000000a";

fn payload() -> serde_json::Value {
    json!({
        "job_id": JOB, "kind": "evaluate", "dataset": "loans", "cut": "2026-11-30",
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
fn a_jws_verifies_with_its_kid_its_digest_and_its_job() {
    let k = key();
    let b = sign(&payload(), &k);
    assert_eq!(bundle_kid(&b.jws), Ok("k1".to_string()));
    let bundle = b.verify(&keys(&k, "k1"), JOB).unwrap();
    assert_eq!(bundle.statements.len(), 2);
    assert_eq!(bundle.statements[1].run_as, trino_runner::RunAs::Report);
    assert!(!format!("{:?}", bundle.statements[0]).contains("INSERT INTO"));
    assert!(!format!("{b:?}").contains(&b.jws));
}

#[test]
fn a_tampered_foreign_or_odd_jws_is_refused() {
    let k = key();
    let b = sign(&payload(), &k);
    let ks = keys(&k, "k1");
    let parts: Vec<&str> = b.jws.split('.').collect();
    // Another payload under the same signature.
    let other = sign(&json!({"job_id": JOB}), &k);
    let swapped = SignedBundle {
        jws: format!(
            "{}.{}.{}",
            parts[0],
            other.jws.split('.').nth(1).unwrap(),
            parts[2]
        ),
        digest: other.digest.clone(),
    };
    assert_eq!(swapped.verify(&ks, JOB), Err(BundleRefused::Signature));
    let mut wrong_digest = b.clone();
    wrong_digest.digest = "0".repeat(64);
    assert_eq!(wrong_digest.verify(&ks, JOB), Err(BundleRefused::Digest));
    assert_eq!(
        b.verify(&keys(&key(), "k1"), JOB),
        Err(BundleRefused::Signature)
    );
    assert_eq!(
        b.verify(&Keys::default(), JOB),
        Err(BundleRefused::UnknownKey)
    );
    assert_eq!(b.verify(&ks, "another-job"), Err(BundleRefused::Content));
    for header in [
        json!({"alg": "HS256", "typ": BUNDLE_TYP, "kid": "k1"}),
        json!({"alg": "ES256", "typ": "JWT", "kid": "k1"}),
        json!({"alg": "ES256", "typ": BUNDLE_TYP}),
    ] {
        let odd = jws_with(&header, &payload(), &k);
        assert_eq!(odd.verify(&ks, JOB), Err(BundleRefused::Header), "{header}");
    }
    let mut twice = payload();
    twice["statements"][1]["id"] = json!("insert_icd");
    assert_eq!(
        sign(&twice, &k).verify(&ks, JOB),
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

#[test]
fn a_gold_write_takes_the_lock_the_core_signs_on_its_statement() {
    // The core's canonical statement (ruling 92(b)): `lock` after
    // `reads_source`, `only_if_rows` left out when none.
    let mut p = payload();
    p["statements"] = json!([{"id": "cells",
        "sql": "INSERT INTO \"lake\".\"quality_gold\".\"cells\" (dataset) SELECT 1",
        "expects": {"kind": "count", "update_types": ["INSERT"]},
        "run_as": "job", "reads_source": false, "lock": "lake.quality_gold.cells"}]);
    let k = key();
    let bundle = sign(&p, &k).verify(&keys(&k, "k1"), JOB).unwrap();
    assert_eq!(
        bundle.statements[0].lock.as_deref(),
        Some("lake.quality_gold.cells")
    );
    p["statements"][0]["lock"] = json!("\"lake\".\"quality_gold\".\"cells\"");
    let quoted = sign(&p, &k).verify(&keys(&k, "k1"), JOB);
    assert_eq!(quoted, Err(BundleRefused::Content), "not a table lock");
}
