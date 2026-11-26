//! The engine port (decision 0011), version 1: the messages between the
//! orchestrator and an engine module's runner, their JSON Schemas and the
//! version rules. Pure: no I/O; the server and `engine-runner` carry them
//! over HTTP.
//!
//! | Call | Body | Answer |
//! | --- | --- | --- |
//! | `PUT /internal/engines/{engine}/{line}` | [`Descriptor`] | [`Registered`] |
//! | `POST /internal/engines/{engine}/{line}/claim` | [`Claim`] | [`Claimed`], or 204 |
//! | `POST /internal/runs/{id}/renew` | [`Renew`] | [`Renewed`] |
//! | `POST /internal/runs/{id}/logs` | [`Logs`] | 204 |
//! | `POST /internal/runs/{id}/finish` | [`Finish`] | 204 |
//! | `POST /internal/engines/{engine}/hold` | [`Hold`] | [`HoldAnswer`] |
//! | `POST /internal/engines/{engine}/release` | [`Release`] | 204 |
//! | `PUT /internal/engines/{engine}/{line}/found` | [`Found`] | [`FoundAnswer`] |
//! | `PUT /internal/engines/{engine}/{line}/inventory` | [`Inventory`] | [`InventoryAnswer`] |
//! | `POST /internal/engines/{engine}/{line}/deploys/claim` | [`Claim`] | [`DeployClaimed`], or 204 |
//! | `POST /internal/deploys/{id}/finish` | [`DeployFinish`] | 204 |

mod deploys;
mod descriptor;
mod found;
mod inventory;
mod runs;
mod schema;

pub use deploys::{DeployClaimed, DeployFinish, DeployState, MAX_VERSION_BYTES};
pub use descriptor::{
    check_port, is_kind_name, Descriptor, Refused, Registered, RunKind, LEASE_SECS,
    MAX_CONCURRENCY, MAX_KINDS, PORT, RENEW_EVERY_SECS, SERVED_PORTS,
};
pub use found::{
    is_sha256, Flag, FlagKind, Found, FoundAnswer, FoundArtefact, FoundCalendar, FoundSchedule,
    ScheduleSource, MAX_FLAGS, MAX_FOUND, MAX_PATH_BYTES,
};
pub use inventory::{
    ArtefactRecord, FolderRecord, Inventory, InventoryAnswer, InventoryItem, SkipReason, Skipped,
    RECORD_FILE, RECORD_FORMAT,
};
pub use runs::{
    Claim, Claimed, Finish, Hold, HoldAnswer, HoldState, LogLine, LogStream, Logs, Progress,
    Release, Renew, RenewAction, Renewed, Resume, MAX_COUNTS, MAX_LOG_LINES, MAX_LOG_LINE_BYTES,
};
pub use schema::schemas;
