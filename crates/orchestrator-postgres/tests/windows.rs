//! Maintenance windows in the store.

mod common;

use common::at;
use orchestration_model::{MaintenanceWindow, OnSchedule, WindowTime};

#[tokio::test]
async fn a_window_is_saved_and_changed_by_its_id() {
    let db = common::database().await;
    let store = db.store();
    let mut w = MaintenanceWindow {
        id: String::new(),
        name: "upgrade".into(),
        time: WindowTime::Once {
            starts_at: at("2026-11-25T02:00:00Z"),
            ends_at: at("2026-11-25T04:00:00Z"),
        },
        on_schedule: OnSchedule::Skip,
        enabled: true,
        clusters: vec!["machine-1".into()],
    };
    w.id = store.save_window(&w).await.unwrap();
    assert_eq!(
        store.maintenance_windows().await.unwrap(),
        std::slice::from_ref(&w)
    );
    w.time = WindowTime::Recurrent {
        cron: "0 22 * * 0".into(),
        timezone: "UTC".into(),
        duration_minutes: 120,
    };
    w.on_schedule = OnSchedule::Queue;
    assert_eq!(store.save_window(&w).await.unwrap(), w.id);
    assert_eq!(store.maintenance_windows().await.unwrap(), [w]);
}
