//! The rules document from fake grants shaped as the core serves them
//! (core decisions 0089, point 3.4, and 0090, point 7.2).

use serde_json::{json, Value};
use trino_rules::{document, Grant};

const ANA: &str = "person-00000000-0000-4000-8000-00000000000a";

fn grants(v: Value) -> Vec<Grant> {
    serde_json::from_value(v).expect("grants parse")
}

fn core() -> Vec<Grant> {
    grants(json!([
        {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["*"],
         "privileges": ["INSERT", "DELETE"], "rows": {"column": "dataset", "in": ["loans"]}},
        {"subject": "job_rules_loans", "namespace": "loans_silver", "tables": ["*"],
         "privileges": ["SELECT"]},
        {"subject": "job_report_loans", "namespace": "quality_gold", "tables": ["*"],
         "privileges": ["SELECT"], "rows": {"column": "dataset", "in": ["loans"]}},
        {"subject": "job_source_crm", "namespace": "src_crm.*", "tables": ["*"],
         "privileges": ["SELECT"]},
        {"subject": "job_maint_loans", "namespace": "loans_bronze", "tables": ["*"],
         "privileges": ["SELECT", "INSERT", "DELETE"]},
        {"subject": ANA, "namespace": "quality_gold", "tables": ["*"], "privileges": ["SELECT"],
         "rows": {"column": "dataset", "in": ["loans", "savings"]}},
        {"subject": "person-00000000-0000-4000-8000-00000000000b", "namespace": "quality_gold",
         "tables": ["*"], "privileges": ["SELECT"], "rows": {"column": "dataset", "in": []}}
    ]))
}

fn tables(doc: &Value) -> Vec<Value> {
    doc["tables"].as_array().unwrap().clone()
}

fn position(rules: &[Value], user: &str, table: &str) -> usize {
    rules
        .iter()
        .position(|r| r["user"] == user && r["table"] == table)
        .unwrap_or_else(|| panic!("no rule for {user} on {table}"))
}

#[test]
fn the_order_is_admin_dollar_jobs_people_superset_modules_none() {
    let modules = grants(
        json!([{"subject": "job_extract_loans", "namespace": "loans_bronze",
        "tables": ["*"], "privileges": ["INSERT"]}]),
    );
    let (doc, invalid) = document(&core(), &modules);
    assert!(invalid.is_empty());
    let t = tables(&doc);
    assert_eq!(t[0]["user"], "qd-admin");
    assert_eq!(
        t[1]["table"], ".*[$].*",
        "people refused metadata tables first"
    );
    assert_eq!(
        t[2]["table"], ".*[$]refs",
        "jobs' $refs before their filtered rules"
    );
    let rules = position(&t, "job_rules_loans", ".*");
    let report_snapshots = position(&t, "job_report_loans", ".*[$]snapshots");
    let report = position(&t, "job_report_loans", ".*");
    let ana = position(&t, ANA, ".*");
    let superset = t.iter().position(|r| r["user"] == "bi-superset").unwrap();
    let module = position(&t, "job_extract_loans", ".*");
    assert!(rules < report_snapshots && report_snapshots < report && report < ana);
    assert!(ana < superset && superset < module);
    assert_eq!(t.last().unwrap(), &json!({"privileges": []}));
}

#[test]
fn grants_become_table_rules_with_row_filters() {
    let (doc, _) = document(&core(), &[]);
    let t = tables(&doc);
    let rules = &t[position(&t, "job_rules_loans", ".*")];
    assert_eq!(rules["catalog"], "lake");
    assert_eq!(rules["schema"], "quality_gold");
    assert_eq!(rules["privileges"], json!(["INSERT", "DELETE"]));
    assert_eq!(rules["filter"], "dataset IN ('loans')");
    assert!(rules.get("OWNERSHIP").is_none());
    let ana = &t[position(&t, ANA, ".*")];
    assert_eq!(ana["catalog"], "gold");
    assert_eq!(ana["filter"], "dataset IN ('loans', 'savings')");
    let nobody = &t[position(&t, "person-00000000-0000-4000-8000-00000000000b", ".*")];
    assert_eq!(nobody["filter"], "false", "no dataset: no row");
    let maint = &t[position(&t, "job_maint_loans", ".*")];
    assert_eq!(maint["catalog"], "lake_maint");
    let source = &t[position(&t, "job_source_crm", ".*")];
    assert_eq!(source["catalog"], "src_crm");
    assert!(source.get("schema").is_none());
    let superset = t.iter().find(|r| r["user"] == "bi-superset").unwrap();
    assert_eq!(superset["filter"], "false");
}

#[test]
fn catalogs_queries_sessions_functions_and_procedures() {
    let (doc, _) = document(&core(), &[]);
    let catalogs = doc["catalogs"].as_array().unwrap();
    let access = |user: &str, catalog: &str| {
        catalogs
            .iter()
            .find(|r| r["user"] == user && r["catalog"] == catalog)
            .map(|r| r["allow"].clone())
    };
    assert_eq!(access("job_rules_loans", "lake"), Some(json!("all")));
    assert_eq!(access(ANA, "gold"), Some(json!("read-only")));
    assert_eq!(access(ANA, "lake"), None);
    assert_eq!(catalogs.last().unwrap(), &json!({"allow": "none"}));
    assert_eq!(
        access("job_catalog", "src_[a-z0-9_]+"),
        Some(json!("owner"))
    );
    assert_eq!(
        doc["system_session_properties"],
        json!([{"user": "qd-admin", "allow": true}, {"allow": false}])
    );
    assert_eq!(
        doc["queries"][0]["allow"],
        json!(["execute", "view", "kill"])
    );
    assert_eq!(doc["queries"][1]["allow"], json!(["execute"]));
    assert_eq!(doc["functions"][0]["schema"], "builtin");
    assert_eq!(doc["procedures"], json!([{"privileges": []}]));
}

#[test]
fn a_grant_that_could_change_a_rule_is_left_out() {
    let bad = grants(json!([
        {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["*"],
         "privileges": ["DELETE"], "rows": {"column": "dataset", "in": ["x') OR (1=1"]}},
        {"subject": "job_rules_lo.ans", "namespace": "quality_gold", "tables": ["*"],
         "privileges": ["DELETE"]},
        {"subject": "jobx_rules_loans", "namespace": "a", "tables": ["*"], "privileges": ["SELECT"]},
        {"subject": ANA, "namespace": "loans_silver", "tables": ["*"], "privileges": ["SELECT"]},
        {"subject": ANA, "namespace": "quality_gold", "tables": ["*"], "privileges": ["INSERT"]},
        {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["t.*"],
         "privileges": ["SELECT"]},
        {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["*"],
         "privileges": []}
    ]));
    let (doc, invalid) = document(&bad, &[]);
    assert_eq!(invalid.len(), 7);
    let text = doc.to_string();
    assert!(!text.contains("1=1") && !text.contains("lo.ans") && !text.contains("jobx"));
}

#[test]
fn unknown_fields_are_refused() {
    let r: Result<Vec<Grant>, _> = serde_json::from_value(json!([{"subject": "job_rules_a",
        "namespace": "n", "tables": ["*"], "privileges": ["SELECT"], "mask": "x"}]));
    assert!(r.is_err());
}

#[test]
fn a_refs_table_is_granted_by_its_exact_name() {
    let g = grants(json!([
        {"subject": "job_report_loans", "namespace": "loans_silver", "tables": ["records$refs"],
         "privileges": ["SELECT"]},
        {"subject": "job_report_loans", "namespace": "loans_silver", "tables": ["records$files"],
         "privileges": ["SELECT"]},
        {"subject": "job_report_loans", "namespace": "loans_silver", "tables": ["$refs"],
         "privileges": ["SELECT"]}
    ]));
    let (doc, invalid) = document(&g, &[]);
    assert_eq!(invalid.len(), 2, "another $ table, and no base table");
    let t = tables(&doc);
    let rules: Vec<&Value> = t
        .iter()
        .filter(|r| r["user"] == "job_report_loans" && r["schema"] == "loans_silver")
        .collect();
    assert_eq!(rules.len(), 1, "no $snapshots rule on a $refs grant");
    assert_eq!(rules[0]["table"], "(records[$]refs)");
    assert_eq!(rules[0]["privileges"], json!(["SELECT"]));
}
