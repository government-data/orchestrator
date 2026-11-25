//! The engine port (decision 0011), version 1: the messages between the
//! orchestrator and an engine module's runner and the version rules. Pure:
//! no I/O; the server and `engine-runner` carry them over HTTP.
//!
//! | Call | Body | Answer |
//! | --- | --- | --- |
//! | `PUT /internal/engines/{engine}/{line}` | [`Descriptor`] | [`Registered`] |

mod descriptor;

pub use descriptor::{
    check_port, is_kind_name, Descriptor, Refused, Registered, RunKind, LEASE_SECS,
    MAX_CONCURRENCY, MAX_KINDS, PORT, RENEW_EVERY_SECS, SERVED_PORTS,
};
