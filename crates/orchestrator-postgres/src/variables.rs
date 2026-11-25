//! Flow variables and the secrets any flow may name. Secret values arrive
//! sealed by the server and leave sealed: this adapter never sees them in
//! clear, and no listing returns them.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;

use crate::{PgStore, StoreError};

/// A variable's value as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableValue {
    /// A value in clear (not secret).
    Plain(Value),
    /// A secret value, sealed.
    Sealed(Vec<u8>),
    /// A secret of the installation, by id.
    Secret(String),
}

/// A flow's variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Variable {
    pub name: String,
    pub value: VariableValue,
}

/// A secret, without its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretInfo {
    pub id: String,
    pub name: String,
    pub description: String,
    pub updated_at: DateTime<Utc>,
}

impl PgStore {
    /// Sets a flow's variable, replacing its value.
    pub async fn set_variable(
        &self,
        flow_id: &str,
        v: &Variable,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let (secret, plain, sealed, secret_id) = match &v.value {
            VariableValue::Plain(value) => (false, Some(value), None, None),
            VariableValue::Sealed(bytes) => (true, None, Some(bytes.as_slice()), None),
            VariableValue::Secret(id) => (true, None, None, Some(id.as_str())),
        };
        sqlx::query(
            "INSERT INTO flow_variables (flow_id, name, secret, value, sealed, secret_id, updated_at)
             VALUES ($1::uuid, $2, $3, $4, $5, $6::uuid, $7)
             ON CONFLICT (flow_id, name) DO UPDATE
                SET secret = EXCLUDED.secret, value = EXCLUDED.value, sealed = EXCLUDED.sealed,
                    secret_id = EXCLUDED.secret_id, updated_at = EXCLUDED.updated_at",
        )
        .bind(flow_id)
        .bind(&v.name)
        .bind(secret)
        .bind(plain)
        .bind(sealed)
        .bind(secret_id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// A flow's variables, by name, sealed values still sealed.
    pub async fn variables(&self, flow_id: &str) -> Result<Vec<Variable>, StoreError> {
        let rows = sqlx::query(
            "SELECT name, value, sealed, secret_id::text FROM flow_variables
              WHERE flow_id = $1::uuid ORDER BY name",
        )
        .bind(flow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let value = match (
                    r.try_get::<Option<Value>, _>("value")?,
                    r.try_get::<Option<Vec<u8>>, _>("sealed")?,
                    r.try_get::<Option<String>, _>("secret_id")?,
                ) {
                    (Some(v), _, _) => VariableValue::Plain(v),
                    (_, Some(b), _) => VariableValue::Sealed(b),
                    (_, _, Some(id)) => VariableValue::Secret(id),
                    // Its secret was deleted: the variable stays, empty.
                    (None, None, None) => VariableValue::Plain(Value::Null),
                };
                Ok(Variable {
                    name: r.try_get("name")?,
                    value,
                })
            })
            .collect()
    }

    /// Removes a flow's variable; false if it had none of that name.
    pub async fn delete_variable(&self, flow_id: &str, name: &str) -> Result<bool, StoreError> {
        let done = sqlx::query("DELETE FROM flow_variables WHERE flow_id = $1::uuid AND name = $2")
            .bind(flow_id)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Creates a secret with its sealed value; its name is unique.
    pub async fn create_secret(
        &self,
        name: &str,
        description: &str,
        sealed: &[u8],
        now: DateTime<Utc>,
    ) -> Result<String, StoreError> {
        let id = sqlx::query_scalar(
            "INSERT INTO secrets (name, description, sealed, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $4) RETURNING id::text",
        )
        .bind(name)
        .bind(description)
        .bind(sealed)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    /// Every secret, without its value.
    pub async fn secrets(&self) -> Result<Vec<SecretInfo>, StoreError> {
        let rows = sqlx::query(
            "SELECT id::text, name, description, updated_at FROM secrets ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(SecretInfo {
                    id: r.try_get("id")?,
                    name: r.try_get("name")?,
                    description: r.try_get("description")?,
                    updated_at: r.try_get("updated_at")?,
                })
            })
            .collect()
    }

    /// A secret's sealed value, for the server to open when it hands a run
    /// out.
    pub async fn sealed_secret(&self, id: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let sealed = sqlx::query_scalar("SELECT sealed FROM secrets WHERE id = $1::uuid")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(sealed)
    }
}
