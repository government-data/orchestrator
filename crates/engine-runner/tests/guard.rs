//! A runner's own guard: no claim while disk or memory is low.

use engine_runner::{Guard, NoGuard, Pause};

struct Low;

impl Guard for Low {
    fn claim_paused(&self) -> Option<Pause> {
        Some(Pause::LowDisk)
    }

    fn disk_low(&self) -> bool {
        true
    }
}

#[test]
fn a_guard_says_why_it_pauses() {
    assert_eq!(NoGuard.claim_paused(), None);
    assert!(!NoGuard.disk_low());
    assert_eq!(Low.claim_paused(), Some(Pause::LowDisk));
    assert!(Low.disk_low());
}
