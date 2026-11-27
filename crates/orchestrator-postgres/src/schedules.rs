//! A flow's schedules, and the installation's settings.

use chrono::{DateTime, Utc};
use orchestration_model::{MissedRuns, OnInterrupt, Settings};
use schedule::{ScheduleSpec, ScheduleTrigger};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::flows::{secs, signed};
use crate::{corrupt, PgStore, StoreError};

/// A flow's schedule.
#[derive(Debug, Clone, PartialEq)]
pub struct Schedule {
    pub id: String,
    pub flow_id: String,
    pub spec: ScheduleSpec,
    /// Over the flow's parameters on each fire.
    pub parameters: Value,
    pub timeout_secs: Option<u32>,
    pub enabled: bool,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub last_fire_at: Option<DateTime<Utc>>,
    /// The core's id of a schedule the core keeps (core decision 0090,
    /// point 2): its fires start the chain the core says.
    pub core_id: Option<String>,
}

/// A schedule to create, checked and with its first fire computed.
#[derive(Debug, Clone, PartialEq)]
pub struct NewSchedule {
    pub flow_id: String,
    pub spec: ScheduleSpec,
    pub parameters: Value,
    pub timeout_secs: Option<u32>,
    /// `None` creates it paused.
    pub next_fire_at: Option<DateTime<Utc>>,
    pub created_by: Option<String>,
}

const SCHEDULE_COLUMNS: &str = "id::text, flow_id::text, cron, timezone, trigger, parameters,
    timeout_secs, enabled, next_fire_at, last_fire_at, core_id";

fn schedule(r: &PgRow) -> Result<Schedule, StoreError> {
    let trigger: Option<Value> = r.try_get("trigger")?;
    let trigger: Option<ScheduleTrigger> = match trigger {
        Some(t) => Some(serde_json::from_value(t).map_err(|e| corrupt(&e.to_string()))?),
        None => None,
    };
    let timeout: Option<i32> = r.try_get("timeout_secs")?;
    Ok(Schedule {
        id: r.try_get("id")?,
        flow_id: r.try_get("flow_id")?,
        spec: ScheduleSpec {
            cron: r.try_get("cron")?,
            timezone: r.try_get("timezone")?,
            trigger,
        },
        parameters: r.try_get("parameters")?,
        timeout_secs: timeout.map(secs).transpose()?,
        enabled: r.try_get("enabled")?,
        next_fire_at: r.try_get("next_fire_at")?,
        last_fire_at: r.try_get("last_fire_at")?,
        core_id: r.try_get("core_id")?,
    })
}

impl PgStore {
    pub async fn create_schedule(&self, new: &NewSchedule) -> Result<Schedule, StoreError> {
        let trigger = new
            .spec
            .trigger
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StoreError::Invalid(e.to_string()))?;
        let sql = format!(
            "INSERT INTO schedules (flow_id, cron, timezone, trigger, parameters, timeout_secs,
                                    enabled, next_fire_at, created_by)
             VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)
             RETURNING {SCHEDULE_COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(&new.flow_id)
            .bind(&new.spec.cron)
            .bind(&new.spec.timezone)
            .bind(trigger)
            .bind(&new.parameters)
            .bind(new.timeout_secs.map(signed).transpose()?)
            .bind(new.next_fire_at.is_some())
            .bind(new.next_fire_at)
            .bind(&new.created_by)
            .fetch_one(&self.pool)
            .await?;
        schedule(&row)
    }

    pub async fn schedules(&self, flow_id: &str) -> Result<Vec<Schedule>, StoreError> {
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE flow_id = $1::uuid ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(flow_id)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(schedule).collect()
    }

    /// The enabled schedules due at `now`, the earliest first.
    pub async fn due_schedules(&self, now: DateTime<Utc>) -> Result<Vec<Schedule>, StoreError> {
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules
              WHERE enabled AND next_fire_at <= $1 ORDER BY next_fire_at LIMIT 500"
        );
        let rows = sqlx::query(&sql).bind(now).fetch_all(&self.pool).await?;
        rows.iter().map(schedule).collect()
    }

    /// Moves a schedule on after it fired at `fired`: its next fire, `None`
    /// when it fires no more (it is disabled then). Only if its next fire is
    /// still `was`, so two schedulers never move it twice.
    pub async fn advance_schedule(
        &self,
        id: &str,
        was: DateTime<Utc>,
        fired: Option<DateTime<Utc>>,
        next: Option<DateTime<Utc>>,
    ) -> Result<bool, StoreError> {
        let done = sqlx::query(
            "UPDATE schedules
                SET next_fire_at = $3, last_fire_at = COALESCE($4, last_fire_at),
                    enabled = enabled AND $3 IS NOT NULL
              WHERE id = $1::uuid AND next_fire_at = $2",
        )
        .bind(id)
        .bind(was)
        .bind(next)
        .bind(fired)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Keeps a schedule of the core on `flow_id`, enabled while `next` (its
    /// next fire) is set: created, or replaced when its spec, flow or
    /// state changed; else its own next fire is kept. `true` if anything
    /// changed.
    pub async fn keep_core_schedule(
        &self,
        flow_id: &str,
        core_id: &str,
        spec: &ScheduleSpec,
        next: Option<DateTime<Utc>>,
    ) -> Result<bool, StoreError> {
        let trigger = spec
            .trigger
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StoreError::Invalid(e.to_string()))?;
        let done = sqlx::query(
            "INSERT INTO schedules (flow_id, cron, timezone, trigger, enabled, next_fire_at,
                                    core_id, created_by)
             VALUES ($1::uuid, $2, $3, $4, $5 IS NOT NULL, $5, $6, 'core')
             ON CONFLICT (core_id) DO UPDATE
                SET cron = EXCLUDED.cron, timezone = EXCLUDED.timezone,
                    trigger = EXCLUDED.trigger, enabled = EXCLUDED.enabled,
                    next_fire_at = EXCLUDED.next_fire_at, flow_id = EXCLUDED.flow_id
              WHERE schedules.cron IS DISTINCT FROM EXCLUDED.cron
                 OR schedules.timezone IS DISTINCT FROM EXCLUDED.timezone
                 OR schedules.trigger IS DISTINCT FROM EXCLUDED.trigger
                 OR schedules.flow_id IS DISTINCT FROM EXCLUDED.flow_id
                 OR schedules.enabled IS DISTINCT FROM EXCLUDED.enabled",
        )
        .bind(flow_id)
        .bind(&spec.cron)
        .bind(&spec.timezone)
        .bind(trigger)
        .bind(next)
        .bind(core_id)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Disables the core's schedules it no longer lists; how many.
    pub async fn drop_core_schedules(&self, listed: &[&str]) -> Result<u64, StoreError> {
        let done = sqlx::query(
            "UPDATE schedules SET enabled = false, next_fire_at = NULL
              WHERE core_id IS NOT NULL AND enabled AND core_id <> ALL($1)",
        )
        .bind(listed)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    pub async fn settings(&self) -> Result<Settings, StoreError> {
        let r = sqlx::query(
            "SELECT missed_runs, missed_max_hours, missed_max_runs, on_interrupt,
                    queued_wait_minutes FROM settings",
        )
        .fetch_one(&self.pool)
        .await?;
        let wait: Option<i32> = r.try_get("queued_wait_minutes")?;
        Ok(Settings {
            missed_runs: MissedRuns::parse(r.try_get("missed_runs")?)
                .ok_or_else(|| corrupt("missed_runs"))?,
            missed_max_hours: secs(r.try_get("missed_max_hours")?)?,
            missed_max_runs: secs(r.try_get("missed_max_runs")?)?,
            on_interrupt: OnInterrupt::parse(r.try_get("on_interrupt")?)
                .ok_or_else(|| corrupt("on_interrupt"))?,
            queued_wait_minutes: wait.map(secs).transpose()?,
        })
    }

    /// Saves the settings, checked first.
    pub async fn save_settings(&self, s: &Settings) -> Result<(), StoreError> {
        s.validate().map_err(StoreError::Invalid)?;
        sqlx::query(
            "UPDATE settings SET missed_runs = $1, missed_max_hours = $2, missed_max_runs = $3,
                    on_interrupt = $4, queued_wait_minutes = $5, updated_at = now()",
        )
        .bind(s.missed_runs.as_str())
        .bind(signed(s.missed_max_hours)?)
        .bind(signed(s.missed_max_runs)?)
        .bind(s.on_interrupt.as_str())
        .bind(s.queued_wait_minutes.map(signed).transpose()?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
