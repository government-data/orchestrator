//! The JSON Schemas of the port's messages, and of the model's enums they
//! carry (the model stays free of schema code).

use orchestration_model::{FinalState, Interruption, RunState};
use schemars::{json_schema, Schema, SchemaGenerator};
use serde_json::{json, Value};

use crate::{
    ArtefactRecord, Claim, Claimed, DeployClaimed, DeployFinish, Descriptor, Finish, Found,
    FoundAnswer, Hold, HoldAnswer, Inventory, InventoryAnswer, Logs, Registered, Release, Renew,
    Renewed,
};

pub(crate) fn engine_name(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": "string", "pattern": "^[a-z][a-z0-9-]{0,39}$" })
}

pub(crate) fn line(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": "string", "pattern": "^(-|[a-z0-9.]{1,16})$" })
}

pub(crate) fn final_state(_: &mut SchemaGenerator) -> Schema {
    let names: Vec<&str> = RunState::ALL
        .into_iter()
        .filter(|s| FinalState::parse(s.as_str()).is_ok())
        .map(RunState::as_str)
        .collect();
    json_schema!({ "type": "string", "enum": names })
}

pub(crate) fn interruption(_: &mut SchemaGenerator) -> Schema {
    let mut names: Vec<Value> = Interruption::ALL
        .into_iter()
        .map(|i| json!(i.as_str()))
        .collect();
    names.push(Value::Null);
    json_schema!({ "enum": names })
}

/// Every message of the port, by name, with its JSON Schema: the contract
/// an engine module's runner is checked against.
#[must_use]
pub fn schemas() -> Value {
    let mut all = serde_json::Map::new();
    let mut add = |name: &str, schema: Schema| {
        all.insert(name.to_string(), schema.to_value());
    };
    add("Descriptor", schemars::schema_for!(Descriptor));
    add("Registered", schemars::schema_for!(Registered));
    add("Claim", schemars::schema_for!(Claim));
    add("Claimed", schemars::schema_for!(Claimed));
    add("Renew", schemars::schema_for!(Renew));
    add("Renewed", schemars::schema_for!(Renewed));
    add("Logs", schemars::schema_for!(Logs));
    add("Finish", schemars::schema_for!(Finish));
    add("Hold", schemars::schema_for!(Hold));
    add("HoldAnswer", schemars::schema_for!(HoldAnswer));
    add("Release", schemars::schema_for!(Release));
    add("Found", schemars::schema_for!(Found));
    add("FoundAnswer", schemars::schema_for!(FoundAnswer));
    add("Inventory", schemars::schema_for!(Inventory));
    add("InventoryAnswer", schemars::schema_for!(InventoryAnswer));
    add("ArtefactRecord", schemars::schema_for!(ArtefactRecord));
    add("DeployClaimed", schemars::schema_for!(DeployClaimed));
    add("DeployFinish", schemars::schema_for!(DeployFinish));
    json!({ "port": crate::PORT, "messages": all })
}
