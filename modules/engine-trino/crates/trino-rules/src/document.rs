//! The rules document: fixed rules and the grants, in the crate's order.

use serde_json::{json, Map, Value};

use crate::{subject, Grant, Invalid, Privilege, Subject};

/// People and Superset: the users that read gold through `gold`.
const READERS: &str = "person-[0-9a-f-]{36}|bi-superset";
/// Every job user.
const JOBS: &str = "job_[a-z0-9_]+";

/// The document from the core's grants and the engine modules' own; a grant
/// that does not check is left out and returned, so the caller counts it.
#[must_use]
pub fn document(core: &[Grant], modules: &[Grant]) -> (Value, Vec<Invalid>) {
    let mut invalid = Vec::new();
    let mut valid = |gs: &[Grant]| -> Vec<Grant> {
        gs.iter()
            .filter(|g| match g.check() {
                Ok(()) => true,
                Err(e) => {
                    invalid.push(e);
                    false
                }
            })
            .cloned()
            .collect()
    };
    let core = valid(core);
    let modules = valid(modules);
    let person = |g: &&Grant| subject(&g.subject) == Some(Subject::Person);
    let (people, jobs): (Vec<&Grant>, Vec<&Grant>) = core.iter().partition(person);

    let mut tables = vec![
        json!({"user": "qd-admin", "privileges":
            ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP", "GRANT_SELECT"]}),
        json!({"user": READERS, "table": ".*[$].*", "privileges": []}),
        json!({"user": JOBS, "catalog": "lake|lake_maint", "table": ".*[$]refs",
            "privileges": ["SELECT"]}),
    ];
    for g in &jobs {
        if g.subject.starts_with("job_report_") && !g.tables.iter().any(|t| t.contains('$')) {
            let mut snapshots = table_rule(g, None);
            snapshots.insert(
                "table".into(),
                json!(format!("{}[$]snapshots", tables_of(g))),
            );
            snapshots.insert("privileges".into(), json!(["SELECT"]));
            tables.push(Value::Object(snapshots));
        }
        tables.push(Value::Object(table_rule(g, filter(g))));
    }
    tables.extend(
        people
            .iter()
            .map(|g| Value::Object(table_rule(g, filter(g)))),
    );
    tables.push(
        json!({"user": "bi-superset", "catalog": "gold", "schema": "quality_gold",
        "privileges": ["SELECT"], "filter": "false"}),
    );
    tables.extend(
        modules
            .iter()
            .map(|g| Value::Object(table_rule(g, filter(g)))),
    );
    tables.push(json!({"privileges": []}));

    let all: Vec<&Grant> = core.iter().chain(&modules).collect();
    let doc = json!({
        "catalogs": catalogs(&all),
        "schemas": [{"user": "qd-admin", "owner": true}, {"owner": false}],
        "tables": tables,
        "queries": [
            {"user": "qd-admin", "allow": ["execute", "view", "kill"]},
            {"user": format!("{JOBS}|{READERS}"), "allow": ["execute"]},
        ],
        "system_session_properties": [{"user": "qd-admin", "allow": true}, {"allow": false}],
        "catalog_session_properties": [{"user": "qd-admin", "allow": true}, {"allow": false}],
        "functions": [{"catalog": "system", "schema": "builtin",
            "privileges": ["EXECUTE", "GRANT_EXECUTE"]}],
        "procedures": [{"privileges": []}],
        "system_information": [{"user": "qd-admin", "allow": ["read", "write"]}],
    });
    (doc, invalid)
}

/// `a|b` for named tables (a `$` written `[$]`), `.*` for all.
fn tables_of(g: &Grant) -> String {
    if g.tables.iter().any(|t| t == "*") {
        ".*".into()
    } else {
        let names: Vec<String> = g.tables.iter().map(|t| t.replace('$', "[$]")).collect();
        format!("({})", names.join("|"))
    }
}

fn table_rule(g: &Grant, filter: Option<String>) -> Map<String, Value> {
    let (catalog, schema) = g.place();
    let mut rule = Map::new();
    rule.insert("user".into(), json!(g.subject));
    rule.insert("catalog".into(), json!(catalog));
    if schema != "*" {
        rule.insert("schema".into(), json!(schema));
    }
    rule.insert("table".into(), json!(tables_of(g)));
    let privileges: Vec<&str> = g.privileges.iter().map(|p| p.as_str()).collect();
    rule.insert("privileges".into(), json!(privileges));
    if let Some(f) = filter {
        rule.insert("filter".into(), json!(f));
    }
    rule
}

/// `column IN ('a', 'b')`, or `false` for no value. Names are checked
/// `[a-z0-9_]` before, so they need no quoting beyond the literal's.
fn filter(g: &Grant) -> Option<String> {
    let rows = g.rows.as_ref()?;
    if rows.values.is_empty() {
        return Some("false".into());
    }
    let values: Vec<String> = rows.values.iter().map(|v| format!("'{v}'")).collect();
    Some(format!("{} IN ({})", rows.column, values.join(", ")))
}

/// Who may use which catalog: `qd-admin` owns every one, `job_catalog` the
/// sources' (Trino creates and drops a catalog only for its owner), each
/// grant's subject its catalog (people read-only), `bi-superset` gold
/// read-only, nobody else any.
fn catalogs(grants: &[&Grant]) -> Vec<Value> {
    let mut rules = vec![
        json!({"user": "qd-admin", "allow": "owner"}),
        json!({"user": "job_catalog", "catalog": "src_[a-z0-9_]+", "allow": "owner"}),
        json!({"user": "bi-superset", "catalog": "gold", "allow": "read-only"}),
    ];
    // One rule per subject and catalog: `all` if any grant there writes.
    let mut access = std::collections::BTreeMap::new();
    for g in grants {
        let writes = g.privileges.iter().any(|p| *p != Privilege::Select);
        let entry = access
            .entry((g.subject.clone(), g.place().0.to_string()))
            .or_insert(false);
        *entry |= writes;
    }
    for ((user, catalog), writes) in access {
        let allow = if writes { "all" } else { "read-only" };
        rules.push(json!({"user": user, "catalog": catalog, "allow": allow}));
    }
    rules.push(json!({"allow": "none"}));
    rules
}
