//! What the runner decides from Trino's answers (core decision 0090,
//! points 6.2.5 and 7.3): whether an answer has the shape its statement
//! expects, and how an error is classed, by Trino's error name only.

use serde_json::Value;

use crate::bundle::Expects;

/// A statement's answer, as the runner reads it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Answer {
    pub query_id: Option<String>,
    pub update_type: Option<String>,
    /// Each column's name and Trino type.
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<Value>>,
}

/// The rows a write counted, when its answer is what `expects` says;
/// `Some(0)` for a read or a `done` that fits; `None` when it does not fit,
/// and the job fails with its data discarded.
#[must_use]
pub fn fits(answer: &Answer, expects: &Expects) -> Option<u64> {
    match expects {
        Expects::Count { update_types } => {
            let kind = answer.update_type.as_deref()?;
            let one_bigint = answer.columns.len() == 1 && answer.columns[0].1 == "bigint";
            if !update_types.iter().any(|t| t == kind) || !one_bigint || answer.rows.len() != 1 {
                return None;
            }
            match answer.rows[0].as_slice() {
                [n] => n.as_u64(),
                _ => None,
            }
        }
        Expects::Rows { columns, max_rows } => {
            let n = u64::try_from(answer.rows.len()).ok()?;
            (answer.columns == *columns && n <= *max_rows).then_some(0)
        }
        Expects::Done => answer.rows.iter().all(|r| r.len() <= 1).then_some(0),
    }
}

/// How a failed statement ends its run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Class {
    /// The run's error class (`[a-z0-9_]`).
    pub class: &'static str,
    /// Whether it is cut by the system and may be repeated (from the
    /// statement it reached), or failed for good.
    pub transient: bool,
}

/// The class of Trino's error by its kind (`trino_client::TrinoError::
/// kind`: the error name, or `unavailable`, `refused`, `gone`).
#[must_use]
pub fn class(kind: &str) -> Class {
    let (class, transient) = match kind {
        "unavailable" => ("trino_unavailable", true),
        "gone" => ("trino_query_gone", true),
        "SERVER_STARTING_UP" => ("trino_starting", true),
        "CLUSTER_OUT_OF_MEMORY" => ("trino_out_of_memory", true),
        "QUERY_QUEUE_FULL" => ("trino_queue_full", true),
        "REMOTE_TASK_ERROR"
        | "REMOTE_HOST_GONE"
        | "TOO_MANY_REQUESTS_FAILED"
        | "PAGE_TRANSPORT_TIMEOUT"
        | "PAGE_TRANSPORT_ERROR" => ("trino_remote_task", true),
        // A commit conflict leaves orphan files: not repeated blindly.
        "TRANSACTION_CONFLICT" => ("commit_conflict", false),
        "refused" => ("token_refused", false),
        "PERMISSION_DENIED" => ("permission_denied", false),
        k if k.starts_with("EXCEEDED_") && k.ends_with("_MEMORY_LIMIT") => {
            ("too_big_for_tier", false)
        }
        "SYNTAX_ERROR" => ("syntax_error", false),
        _ => ("statement_failed", false),
    };
    Class { class, transient }
}
