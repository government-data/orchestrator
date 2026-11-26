//! The runner's guard (core decision 0072, point 14) over a fake probe: it
//! pauses claims for a low disk or low memory, stops a run only for the
//! disk, and claims on when a reading fails.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use engine_runner::{Guard, Pause};
use engine_trino_service::guard::{cgroup_free_pct, host_free_pct, MachineGuard, Probe};

/// The disk's and the memory's readings.
type Readings = (Result<u64, String>, Result<f64, String>);

#[derive(Clone)]
struct Fake(Arc<Mutex<Readings>>);

impl Fake {
    fn set(&self, disk: Result<u64, String>, memory: Result<f64, String>) {
        *self.0.lock().unwrap() = (disk, memory);
    }
}

impl Probe for Fake {
    fn free_bytes(&self, _: &Path) -> Result<u64, String> {
        self.0.lock().unwrap().0.clone()
    }
    fn free_memory_pct(&self) -> Result<f64, String> {
        self.0.lock().unwrap().1.clone()
    }
}

#[test]
fn claims_pause_under_either_minimum_and_a_run_stops_only_for_the_disk() {
    let fake = Fake(Arc::new(Mutex::new((Ok(0), Ok(0.0)))));
    let guard = MachineGuard::new(fake.clone(), vec![PathBuf::from("/")], 5_000, 10.0);
    fake.set(Ok(10_000), Ok(50.0));
    assert_eq!(guard.claim_paused(), None);
    assert!(!guard.disk_low());
    fake.set(Ok(4_999), Ok(50.0));
    assert_eq!(guard.claim_paused(), Some(Pause::LowDisk));
    assert!(guard.disk_low());
    fake.set(Ok(10_000), Ok(9.5));
    assert_eq!(guard.claim_paused(), Some(Pause::LowMemory));
    assert!(!guard.disk_low(), "low memory does not stop a run");
    fake.set(Err("no statvfs".into()), Err("no meminfo".into()));
    assert_eq!(guard.claim_paused(), None, "a failed reading claims on");
}

#[test]
fn memory_is_the_lower_of_host_and_cgroup() {
    let meminfo = "MemTotal:       16000000 kB\nMemFree: 1 kB\nMemAvailable:    4000000 kB\n";
    assert_eq!(host_free_pct(meminfo), Some(25.0));
    assert_eq!(host_free_pct("MemFree: 1 kB\n"), None);
    assert_eq!(cgroup_free_pct("1000\n", "900\n"), Some(10.0));
    assert_eq!(cgroup_free_pct("max\n", "900\n"), None, "no limit");
    assert_eq!(cgroup_free_pct("1000", "1200"), Some(0.0));
}
