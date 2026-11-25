//! Maintenance windows per cluster.

use orchestration_model::{MaintenanceWindow, OnSchedule, WindowTime};
use sqlx::Row;

use crate::{corrupt, PgStore, StoreError};

impl PgStore {
    /// Saves a window (checked by the caller with the calendar), new or
    /// changed by its id.
    pub async fn save_window(&self, w: &MaintenanceWindow) -> Result<String, StoreError> {
        let time = serde_json::to_value(&w.time).map_err(|e| StoreError::Invalid(e.to_string()))?;
        let on_schedule = match w.on_schedule {
            OnSchedule::Queue => "queue",
            OnSchedule::Skip => "skip",
        };
        let id = sqlx::query_scalar(
            "INSERT INTO maintenance_windows (id, name, time, on_schedule, enabled, clusters)
             VALUES (COALESCE(NULLIF($1, '')::uuid, gen_random_uuid()), $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name, time = EXCLUDED.time, on_schedule = EXCLUDED.on_schedule,
                    enabled = EXCLUDED.enabled, clusters = EXCLUDED.clusters
             RETURNING id::text",
        )
        .bind(&w.id)
        .bind(&w.name)
        .bind(time)
        .bind(on_schedule)
        .bind(w.enabled)
        .bind(&w.clusters)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    /// Every window, by name.
    pub async fn maintenance_windows(&self) -> Result<Vec<MaintenanceWindow>, StoreError> {
        let rows = sqlx::query(
            "SELECT id::text, name, time, on_schedule, enabled, clusters
               FROM maintenance_windows ORDER BY name, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let time: WindowTime = serde_json::from_value(r.try_get("time")?)
                    .map_err(|e| corrupt(&e.to_string()))?;
                let on_schedule = match r.try_get::<String, _>("on_schedule")?.as_str() {
                    "queue" => OnSchedule::Queue,
                    "skip" => OnSchedule::Skip,
                    _ => return Err(corrupt("on_schedule")),
                };
                Ok(MaintenanceWindow {
                    id: r.try_get("id")?,
                    name: r.try_get("name")?,
                    time,
                    on_schedule,
                    enabled: r.try_get("enabled")?,
                    clusters: r.try_get("clusters")?,
                })
            })
            .collect()
    }
}
