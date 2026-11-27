//! Sends the outbox to the core's events (decision 0011, step 6): the
//! run-outcome events, in order, each with the id it was written with, so
//! a send repeated after a crash is stored once by the core. A refused
//! event is kept (refused) and the next goes; no answer stops the pass,
//! and the next pass starts again from the same event.

use chrono::{DateTime, Duration, Utc};
use orchestrator_postgres::{PgStore, StoreError};

use crate::{CoreEvents, Event, Published};

/// The most events one pass reads at a time.
const BATCH: u32 = 100;

/// How long a sent event stays in the outbox.
const KEPT_SENT: Duration = Duration::days(7);

/// What a pass did, in counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutboxSent {
    pub sent: u32,
    pub refused: u32,
    /// The core did not answer: events are left for the next pass.
    pub waiting: bool,
}

/// One pass over the outbox at `now`.
pub async fn send_outbox(
    store: &PgStore,
    events: &dyn CoreEvents,
    now: DateTime<Utc>,
) -> Result<OutboxSent, StoreError> {
    let mut done = OutboxSent::default();
    loop {
        let pending = store.pending_events(BATCH).await?;
        if pending.is_empty() {
            break;
        }
        for e in pending {
            let event = Event {
                id: e.id,
                topic: e.topic,
                version: e.version,
                occurred_at: e.occurred_at,
                data: e.data,
            };
            match events.publish(&event).await {
                Published::Stored => {
                    store.event_sent(e.seq, now).await?;
                    done.sent += 1;
                }
                Published::Refused(status) => {
                    store.event_refused(e.seq, status).await?;
                    done.refused += 1;
                }
                Published::Unavailable => {
                    done.waiting = true;
                    return Ok(done);
                }
            }
        }
    }
    store.forget_sent_events(now - KEPT_SENT).await?;
    Ok(done)
}
