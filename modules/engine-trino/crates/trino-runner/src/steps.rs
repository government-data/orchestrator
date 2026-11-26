//! The two step forms of core decision 0090's amendment A1, and the run's
//! state between steps:
//!
//! - `only_if_rows {after}` on a `DELETE`: it runs only if the earlier
//!   one-row `bigint` count it names was not 0, else it is skipped;
//! - `{{name}}` placeholders, declared with a type and a source column of
//!   an earlier one-row answer, filled with that value's decimal digits.
//!
//! A bundle is refused at load when a form points at a later or another
//! kind of statement, or a statement holds an undeclared token. The values
//! and counts a later step needs travel in the run's progress detail
//! (`done:<n>|<key>=<digits>…`), so a repeated attempt resumes with them.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::bundle::{Bundle, Expects, PlaceholderType};
use crate::check::Answer;

/// The longest progress detail the engine port takes.
const MAX_DETAIL: usize = 200;

fn is_count(e: &Expects) -> bool {
    matches!(e, Expects::Rows { columns, max_rows: 1 }
        if columns.len() == 1 && columns[0].1 == "bigint")
}

fn one_row_with(e: &Expects, column: &str) -> bool {
    matches!(e, Expects::Rows { columns, max_rows: 1 } if columns.iter().any(|c| c.0 == column))
}

fn is_delete(e: &Expects) -> bool {
    matches!(e, Expects::Count { update_types } if update_types.iter().any(|t| t == "DELETE"))
}

/// The `{{…}}` tokens of `sql`, in order; `None` if a `{{` is not closed.
fn tokens(sql: &str) -> Option<Vec<&str>> {
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let end = after.find("}}")?;
        out.push(&after[..end]);
        rest = &after[end + 2..];
    }
    Some(out)
}

/// The load checks of amendment A1.
pub(crate) fn check(bundle: &Bundle) -> Result<(), &'static str> {
    let index: BTreeMap<&str, usize> = bundle
        .statements
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.as_str(), i))
        .collect();
    let mut declared = BTreeMap::new();
    for p in &bundle.placeholders {
        let source = index
            .get(p.from.statement.as_str())
            .ok_or("unknown source")?;
        let ok_name = !p.name.is_empty()
            && p.name.len() <= 64
            && p.name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !ok_name || !one_row_with(&bundle.statements[*source].expects, &p.from.column) {
            return Err("bad placeholder");
        }
        if declared.insert(p.name.as_str(), *source).is_some() {
            return Err("placeholder declared twice");
        }
    }
    for (i, st) in bundle.statements.iter().enumerate() {
        if let Some(o) = &st.only_if_rows {
            let after = index.get(o.after.as_str()).ok_or("unknown count")?;
            let earlier_count = *after < i && is_count(&bundle.statements[*after].expects);
            if !earlier_count || !is_delete(&st.expects) {
                return Err("bad only_if_rows");
            }
        }
        for t in tokens(&st.sql).ok_or("unclosed token")? {
            match declared.get(t) {
                Some(source) if *source < i => {}
                _ => return Err("undeclared or later token"),
            }
        }
    }
    Ok(())
}

/// What a run knows between steps: filled placeholders and the counts an
/// `only_if_rows` reads, by name or statement id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Known(pub(crate) BTreeMap<String, String>);

impl Known {
    /// The detail after `n` statements, with what later steps need; only
    /// `done:<n>` if it would not fit.
    pub(crate) fn detail(&self, n: usize) -> String {
        let mut d = format!("done:{n}");
        for (k, v) in &self.0 {
            d.push_str(&format!("|{k}={v}"));
        }
        if d.len() > MAX_DETAIL {
            return format!("done:{n}");
        }
        d
    }

    /// The statements done and what was known then, from a detail.
    pub(crate) fn parse(detail: Option<&str>) -> (usize, Self) {
        let Some(rest) = detail.and_then(|d| d.strip_prefix("done:")) else {
            return (0, Self::default());
        };
        let mut parts = rest.split('|');
        let n = parts.next().and_then(|n| n.parse().ok()).unwrap_or(0);
        let known = parts
            .filter_map(|p| p.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        (n, Self(known))
    }
}

/// A value checked against its type: decimal digits, at most 19, a `-`
/// first only for a `bigint`.
pub(crate) fn checked(value: &Value, kind: PlaceholderType) -> Option<String> {
    let text = match value {
        Value::Number(n) if n.is_i64() || n.is_u64() => n.to_string(),
        Value::String(s) => s.clone(),
        _ => return None,
    };
    let digits = match (kind, text.strip_prefix('-')) {
        (PlaceholderType::Bigint, Some(d)) => d,
        (_, Some(_)) => return None,
        (_, None) => text.as_str(),
    };
    let ok = !digits.is_empty() && digits.len() <= 19 && digits.bytes().all(|b| b.is_ascii_digit());
    ok.then_some(text)
}

/// The value of `column` in a one-row answer.
pub(crate) fn column<'a>(answer: &'a Answer, column: &str) -> Option<&'a Value> {
    let i = answer.columns.iter().position(|c| c.0 == column)?;
    match answer.rows.as_slice() {
        [row] => row.get(i),
        _ => None,
    }
}

/// `sql` with each token replaced by its known value; `None` if one is
/// not known.
pub(crate) fn fill(sql: &str, known: &Known) -> Option<String> {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find("}}")?;
        out.push_str(known.0.get(&after[..end])?);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Some(out)
}
