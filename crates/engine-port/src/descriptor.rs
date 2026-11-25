//! What a runner says of its engine when it starts, and the port versions
//! the orchestrator serves.

use std::collections::BTreeSet;
use std::fmt;

use orchestration_model::{EngineName, Line};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The engine port's current version.
pub const PORT: u32 = 1;

/// The port versions the orchestrator serves: the current one and, for one
/// platform version after a new one, the previous.
pub const SERVED_PORTS: &[u32] = &[PORT];

/// The most run kinds one engine declares.
pub const MAX_KINDS: usize = 64;

/// The most runs one runner says it runs at once.
pub const MAX_CONCURRENCY: u32 = 64;

/// A kind of run an engine runs, with the JSON Schema of its parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunKind {
    /// `[a-z][a-z0-9_]`, at most 40 bytes.
    pub name: String,
    /// The JSON Schema (an object) a run's parameters of this kind meet.
    pub parameters: Value,
}

/// A runner's descriptor of its engine, sent when it starts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    #[schemars(schema_with = "crate::schema::engine_name")]
    pub engine: EngineName,
    /// The engine's major line, or `-` for an engine with one.
    #[schemars(schema_with = "crate::schema::line")]
    pub line: Line,
    /// The engine's exact version.
    pub engine_version: String,
    /// The port version the runner speaks.
    pub port: u32,
    pub kinds: Vec<RunKind>,
    /// How many runs this runner runs at once.
    pub concurrency: u32,
}

/// Why the orchestrator refuses a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// A port version it does not serve.
    Port { asked: u32, served: Vec<u32> },
    /// The message is not valid (and why).
    Invalid(String),
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Port { asked, served } => {
                write!(f, "engine port {asked} is not served (served: {served:?})")
            }
            Self::Invalid(why) => f.write_str(why),
        }
    }
}

impl std::error::Error for Refused {}

/// Whether `port` is one the orchestrator serves.
pub fn check_port(port: u32) -> Result<(), Refused> {
    if SERVED_PORTS.contains(&port) {
        Ok(())
    } else {
        Err(Refused::Port {
            asked: port,
            served: SERVED_PORTS.to_vec(),
        })
    }
}

/// Whether a kind's name is well formed.
#[must_use]
pub fn is_kind_name(name: &str) -> bool {
    name.len() <= 40
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

impl Descriptor {
    /// Checks it as the orchestrator does at registration: a port it
    /// serves, an exact engine version, kinds well formed and unique, each
    /// with a schema object, and a concurrency in range.
    pub fn validate(&self) -> Result<(), Refused> {
        check_port(self.port)?;
        let invalid = |why: String| Err(Refused::Invalid(why));
        let version = self.engine_version.trim();
        if version.is_empty() || version.len() > 64 || version != self.engine_version {
            return invalid("engine_version: the exact version, at most 64 bytes".into());
        }
        if self.kinds.is_empty() || self.kinds.len() > MAX_KINDS {
            return invalid(format!("kinds: 1 to {MAX_KINDS}"));
        }
        let mut seen = BTreeSet::new();
        for kind in &self.kinds {
            if !is_kind_name(&kind.name) {
                return invalid(format!("kind {:?}: [a-z][a-z0-9_], at most 40", kind.name));
            }
            if !seen.insert(kind.name.as_str()) {
                return invalid(format!("kind {:?} is declared twice", kind.name));
            }
            if !kind.parameters.is_object() {
                return invalid(format!(
                    "kind {:?}: parameters is a JSON Schema object",
                    kind.name
                ));
            }
        }
        if !(1..=MAX_CONCURRENCY).contains(&self.concurrency) {
            return invalid(format!("concurrency: 1 to {MAX_CONCURRENCY}"));
        }
        Ok(())
    }

    /// The kind named `name`, if it declares it.
    #[must_use]
    pub fn kind(&self, name: &str) -> Option<&RunKind> {
        self.kinds.iter().find(|k| k.name == name)
    }
}

/// The orchestrator's answer to a registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Registered {
    /// The port version both speak.
    pub port: u32,
    /// How long a claimed run's lease lasts.
    pub lease_secs: u32,
    /// How often the runner renews a lease while it runs.
    pub renew_every_secs: u32,
}

/// The lease of a claimed run: two minutes, renewed every 30 seconds.
pub const LEASE_SECS: u32 = 120;

/// How often a runner renews a lease.
pub const RENEW_EVERY_SECS: u32 = 30;

impl Registered {
    /// The answer for a descriptor that passed [`Descriptor::validate`].
    #[must_use]
    pub fn for_port(port: u32) -> Self {
        Self {
            port,
            lease_secs: LEASE_SECS,
            renew_every_secs: RENEW_EVERY_SECS,
        }
    }
}
