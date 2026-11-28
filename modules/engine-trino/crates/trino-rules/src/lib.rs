//! Trino's access rules for engine-trino (decision 0011; core decision 0090,
//! point 7.2, and 0089, point 3.4): the core serves **neutral grants** for
//! people and job users, never in Trino's syntax; this crate turns them,
//! with the module's fixed rules, into the one file-based rules document
//! the coordinator reads. Pure: no I/O.
//!
//! The document's table rules, first match wins:
//!
//! 1. `qd-admin`, owner of everything;
//! 2. people and `bi-superset` refused every `$` metadata table;
//! 3. job users: a filter-free `SELECT` on `$refs` tables, then each job
//!    grant (a report user also reads its tables' `$snapshots`; a
//!    dataset's maintenance user also owns the tables of its own bronze
//!    and silver schemas, to create and alter them);
//! 4. people: each grant on `gold`, with its row filter;
//! 5. `bi-superset`: gold's tables listed, no row read (`filter: false`);
//! 6. the engine modules' own grants;
//! 7. nothing for anyone else.
//!
//! Plus catalog rules from the grants, `queries` (everyone `execute`,
//! `qd-admin` `view` and `kill`; owners always see and kill their own),
//! session properties for `qd-admin` only, functions of `system.builtin`
//! only (so connectors' `query()` is refused) and no procedure.

mod document;

pub use document::document;

use serde::Deserialize;

/// A privilege a grant gives on its tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Privilege {
    Select,
    Insert,
    Delete,
}

impl Privilege {
    fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Delete => "DELETE",
        }
    }
}

/// Which rows of a table a grant gives: those whose `column` is one of
/// `in` (none: no row).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rows {
    pub column: String,
    #[serde(rename = "in")]
    pub values: Vec<String>,
}

/// A neutral grant, as the core serves it: `subject` may use `privileges`
/// on `tables` of `namespace`, limited to `rows`.
///
/// `namespace` is a schema of the warehouse (`quality_gold`), whose
/// catalog follows from the subject (people: `gold`; `job_maint_*`:
/// `lake_maint`; other job users: `lake`), or `<catalog>.<schema>` for
/// another catalog (a source's `src_<source>`, schema `*` for all).
/// `tables` is `["*"]` or table names.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub subject: String,
    pub namespace: String,
    pub tables: Vec<String>,
    pub privileges: Vec<Privilege>,
    #[serde(default)]
    pub rows: Option<Rows>,
}

/// Why a grant is not taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid {
    /// The grant's subject, which names no person by more than an id.
    pub subject: String,
    pub why: &'static str,
}

/// A name built into a rule: `[a-z0-9_]`, 1 to 63, so that no text of a
/// grant can change a rule's meaning.
pub(crate) fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// A table a grant names: a name, or a name and its `$refs` metadata
/// table (the only one a grant may name, core ruling of 2026-11-25).
pub(crate) fn is_table(s: &str) -> bool {
    match s.split_once('$') {
        Some((base, "refs")) => is_name(base),
        Some(_) => false,
        None => is_name(s),
    }
}

/// Who a subject is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Subject {
    Person,
    Job { maint: bool },
}

pub(crate) fn subject(s: &str) -> Option<Subject> {
    if let Some(id) = s.strip_prefix("person-") {
        let uuid = id.len() == 36
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || b == b'-');
        return uuid.then_some(Subject::Person);
    }
    let rest = s.strip_prefix("job_")?;
    if rest == "catalog" {
        return Some(Subject::Job { maint: false });
    }
    let (kind, name) = rest.split_once('_')?;
    let known = match kind {
        "rules" | "report" | "extract" | "sample" | "maint" | "source" => is_name(name),
        // One run's own user (rulings 84 and 102): `r` and the run's 32 hex
        // digits, as Trino's user mapping takes it.
        "console" | "lookup" => is_run(name),
        _ => false,
    };
    known.then_some(Subject::Job {
        maint: kind == "maint",
    })
}

/// A run's target: `r` and 32 lower-case hex digits (core decision 0090,
/// rulings 84 and 102).
fn is_run(s: &str) -> bool {
    s.strip_prefix('r').is_some_and(|hex| {
        hex.len() == 32
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

impl Grant {
    /// Checks every name the rule will carry.
    pub fn check(&self) -> Result<(), Invalid> {
        let bad = |why| {
            Err(Invalid {
                subject: self.subject.clone(),
                why,
            })
        };
        let Some(who) = subject(&self.subject) else {
            return bad("subject: person-<uuid> or job_<kind>_<name>");
        };
        let (catalog, schema) = self.place();
        if !is_name(catalog) || !(schema == "*" || is_name(schema)) {
            return bad("namespace: [a-z0-9_] names");
        }
        // People never reach silver or bronze (core decision 0090, 7.4).
        let gold = catalog == "gold" && schema == "quality_gold";
        if who == Subject::Person && (!gold || self.privileges != [Privilege::Select]) {
            return bad("a person reads gold.quality_gold only");
        }
        if self.tables.is_empty() || !self.tables.iter().all(|t| t == "*" || is_table(t)) {
            return bad("tables: * or [a-z0-9_] names, or a name with `$refs`");
        }
        if self.privileges.is_empty() {
            return bad("privileges: at least one");
        }
        match &self.rows {
            Some(r) if !is_name(&r.column) || !r.values.iter().all(|v| is_name(v)) => {
                bad("rows: [a-z0-9_] column and values")
            }
            _ => Ok(()),
        }
    }

    /// Whether the grant is a dataset's maintenance user's on its own
    /// bronze or silver schema (`job_maint_<d>` on `<d>_bronze` or
    /// `<d>_silver` of `lake_maint`): there it also owns the tables, which
    /// Trino's file rules require to create a table or add a column (core
    /// decision 0090, ruling 66: the `prepare` job).
    pub(crate) fn owns_tables(&self) -> bool {
        let Some(d) = self.subject.strip_prefix("job_maint_") else {
            return false;
        };
        let (catalog, schema) = self.place();
        let own = schema
            .strip_prefix(d)
            .is_some_and(|l| l == "_bronze" || l == "_silver");
        catalog == "lake_maint" && own
    }

    /// The catalog and schema the grant is on.
    pub(crate) fn place(&self) -> (&str, &str) {
        if let Some((c, s)) = self.namespace.split_once('.') {
            return (c, s);
        }
        let catalog = match subject(&self.subject) {
            Some(Subject::Person) => "gold",
            Some(Subject::Job { maint: true }) => "lake_maint",
            _ => "lake",
        };
        (catalog, &self.namespace)
    }
}
