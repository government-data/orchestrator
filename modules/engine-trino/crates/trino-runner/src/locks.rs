//! The per-table mutex (core decision 0090, point 4; ruling 92(b)): the
//! core marks each statement that needs one writer per table with a signed
//! `lock`, the table's name `<catalog>.<schema>.<table>` (for gold,
//! `lake.quality_gold.<table>`). A run takes every lock it still needs at
//! once, in name order, before the first statement that carries one, and
//! gives each back after the last statement that carries it; what it holds
//! when it fails or is cancelled is given back at its end. Taken in one
//! order, two runs never wait on each other's locks. A lock not granted is
//! asked again after a wait, the run's lease renewed meanwhile.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine_port::{is_lock_name, LogStream};
use engine_runner::{CallError, Outcome, RunContext};

use crate::bundle::Bundle;
use crate::engine::transient;

/// How long a run waits before it asks again for a lock not granted.
pub(crate) const LOCK_WAIT: Duration = Duration::from_secs(5);

/// Whether `name` is a statement's lock: three parts `[a-z0-9_]` of 1 to
/// 63 bytes joined by `.`, a lock name the engine port takes.
pub(crate) fn is_table_lock(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    is_lock_name(name)
        && parts.len() == 3
        && parts.iter().all(|p| {
            (1..=63).contains(&p.len())
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
}

/// A run's context and the locks its bundle names.
pub(crate) struct Locks<'a> {
    pub(crate) ctx: &'a RunContext,
    wait: Duration,
    /// Each lock's first and last statement, by name (so in name order).
    spans: BTreeMap<String, (usize, usize)>,
    /// The locks asked for and not given back, shared with the run's end.
    held: Arc<Mutex<BTreeSet<String>>>,
    taken: bool,
}

impl<'a> Locks<'a> {
    pub(crate) fn new(
        bundle: &Bundle,
        ctx: &'a RunContext,
        wait: Duration,
        held: Arc<Mutex<BTreeSet<String>>>,
    ) -> Self {
        let mut spans = BTreeMap::new();
        for (i, st) in bundle.statements.iter().enumerate() {
            if let Some(name) = &st.lock {
                spans
                    .entry(name.clone())
                    .and_modify(|s: &mut (usize, usize)| s.1 = i)
                    .or_insert((i, i));
            }
        }
        Self {
            ctx,
            wait,
            spans,
            held,
            taken: false,
        }
    }

    /// Before statement `i`: if it is within a lock's statements and none
    /// were taken yet, takes every lock still needed, in name order (a
    /// repeated attempt resumes within or after a lock's statements).
    pub(crate) async fn before(&mut self, i: usize) -> Result<(), Outcome> {
        let within = self.spans.values().any(|(f, l)| *f <= i && i <= *l);
        if self.taken || !within {
            return Ok(());
        }
        self.taken = true;
        let needed: Vec<String> = self
            .spans
            .iter()
            .filter(|(_, (_, last))| *last >= i)
            .map(|(name, _)| name.clone())
            .collect();
        for name in needed {
            self.take(&name).await?;
        }
        Ok(())
    }

    async fn take(&self, name: &str) -> Result<(), Outcome> {
        // Recorded before asking: one granted to an ask a cancel cut short
        // is given back too.
        lock(&self.held).insert(name.to_string());
        let mut logged = false;
        loop {
            match self.ctx.lock(name).await {
                Ok(true) => return Ok(()),
                Ok(false) | Err(CallError::Unreachable(_)) => {
                    if !logged {
                        self.ctx
                            .log(LogStream::System, &format!("waiting for lock {name}"));
                        logged = true;
                    }
                    tokio::time::sleep(self.wait).await;
                }
                Err(CallError::Lease) => return Err(transient("lease_lost")),
                Err(CallError::Refused(..)) => return Err(Outcome::failed("lock_refused")),
            }
        }
    }

    /// After statement `i` (done or skipped): gives back each lock whose
    /// last statement it was. One not given back now is at the run's end.
    pub(crate) async fn after(&self, i: usize) {
        for (name, (_, last)) in &self.spans {
            if *last == i && lock(&self.held).contains(name) && self.ctx.unlock(name).await.is_ok()
            {
                lock(&self.held).remove(name);
            }
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// At the run's end: gives back every lock it still holds.
pub(crate) async fn give_back(ctx: &RunContext, held: &Mutex<BTreeSet<String>>) {
    let names = std::mem::take(&mut *lock(held));
    for name in names {
        let _ = ctx.unlock(&name).await;
    }
}
