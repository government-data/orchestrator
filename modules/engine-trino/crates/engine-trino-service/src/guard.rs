//! The job runner guards itself (decision 0011 point 4; core decision
//! 0072, point 14): before each claim it checks the free disk of its
//! container's root (Docker's data) and its free memory, and while either
//! is under its minimum it claims nothing. A disk under its minimum while
//! a run goes on stops that run, repeated elsewhere (engine-runner's rule).
//!
//! - **Disk**: `statvfs` of each path, against `QD_WORKER_MIN_FREE_BYTES`
//!   (default 5 GB).
//! - **Memory**: the lower of the host's `MemAvailable` / `MemTotal` and the
//!   cgroup's headroom (`memory.max` − `memory.current`, when limited),
//!   against `QD_WORKER_MIN_FREE_MEMORY_PCT` (default 10 %).
//!
//! On a change it logs one line. A reading that fails does not stop the
//! claim: the line says so once.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use engine_runner::{Guard, Pause};

/// The default minimum free disk, bytes.
pub const DEFAULT_MIN_FREE_BYTES: u64 = 5_000_000_000;
/// The default minimum free memory, %.
pub const DEFAULT_MIN_FREE_MEMORY_PCT: f64 = 10.0;

/// What the guard reads.
pub trait Probe: Send + Sync + 'static {
    /// Free bytes for an unprivileged user on the filesystem of `path`.
    fn free_bytes(&self, path: &Path) -> Result<u64, String>;
    /// Free memory, %: the lower of the host's and the cgroup's.
    fn free_memory_pct(&self) -> Result<f64, String>;
}

/// The runner's guard over a [`Probe`].
pub struct MachineGuard<P> {
    paths: Vec<PathBuf>,
    min_free_bytes: u64,
    min_free_memory_pct: f64,
    probe: P,
    /// The last pause, and whether a failed reading was logged.
    last: Mutex<(Option<Pause>, bool)>,
}

impl<P> std::fmt::Debug for MachineGuard<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MachineGuard")
            .field("paths", &self.paths)
            .field("min_free_bytes", &self.min_free_bytes)
            .field("min_free_memory_pct", &self.min_free_memory_pct)
            .finish_non_exhaustive()
    }
}

impl<P: Probe> MachineGuard<P> {
    #[must_use]
    pub fn new(
        probe: P,
        paths: Vec<PathBuf>,
        min_free_bytes: u64,
        min_free_memory_pct: f64,
    ) -> Self {
        Self {
            paths,
            min_free_bytes,
            min_free_memory_pct,
            probe,
            last: Mutex::new((None, false)),
        }
    }

    fn failed(&self, what: &str, e: &str) {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if !last.1 {
            tracing::warn!(reading = what, error = %e, "the guard could not read; claiming anyway");
            last.1 = true;
        }
    }

    fn disk_is_low(&self) -> bool {
        self.paths.iter().any(|p| match self.probe.free_bytes(p) {
            Ok(free) => free < self.min_free_bytes,
            Err(e) => {
                self.failed("disk", &e);
                false
            }
        })
    }

    fn memory_is_low(&self) -> bool {
        match self.probe.free_memory_pct() {
            Ok(pct) => pct < self.min_free_memory_pct,
            Err(e) => {
                self.failed("memory", &e);
                false
            }
        }
    }
}

impl<P: Probe> Guard for MachineGuard<P> {
    fn claim_paused(&self) -> Option<Pause> {
        let now = if self.disk_is_low() {
            Some(Pause::LowDisk)
        } else if self.memory_is_low() {
            Some(Pause::LowMemory)
        } else {
            None
        };
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if last.0 != now {
            match now {
                Some(Pause::LowDisk) => tracing::warn!(
                    reason = "disk",
                    "free disk under its minimum: claiming nothing"
                ),
                Some(Pause::LowMemory) => tracing::warn!(
                    reason = "memory",
                    "free memory under its minimum: claiming nothing"
                ),
                None => tracing::info!("under its limits again: claiming runs"),
            }
            last.0 = now;
        }
        now
    }

    fn disk_low(&self) -> bool {
        self.disk_is_low()
    }
}

/// This host, as the container sees it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Host;

impl Probe for Host {
    fn free_bytes(&self, path: &Path) -> Result<u64, String> {
        let st = rustix::fs::statvfs(path).map_err(|e| e.to_string())?;
        Ok(st.f_bavail.saturating_mul(st.f_frsize))
    }

    fn free_memory_pct(&self) -> Result<f64, String> {
        let meminfo = std::fs::read_to_string("/proc/meminfo").map_err(|e| e.to_string())?;
        let host = host_free_pct(&meminfo).ok_or("no MemTotal or MemAvailable")?;
        let read = |f: &str| std::fs::read_to_string(Path::new("/sys/fs/cgroup").join(f)).ok();
        let cgroup = match (read("memory.max"), read("memory.current")) {
            (Some(max), Some(current)) => cgroup_free_pct(&max, &current),
            _ => None,
        };
        Ok(cgroup.map_or(host, |c| c.min(host)))
    }
}

/// `MemAvailable` / `MemTotal`, %, from `/proc/meminfo`.
#[must_use]
pub fn host_free_pct(meminfo: &str) -> Option<f64> {
    let field = |name: &str| -> Option<f64> {
        let line = meminfo.lines().find(|l| l.starts_with(name))?;
        line[name.len()..].split_whitespace().next()?.parse().ok()
    };
    let (total, available) = (field("MemTotal:")?, field("MemAvailable:")?);
    (total > 0.0).then(|| available / total * 100.0)
}

/// The cgroup's headroom, %: `None` without a limit (`max`).
#[must_use]
pub fn cgroup_free_pct(max: &str, current: &str) -> Option<f64> {
    let max: f64 = max.trim().parse().ok()?;
    let current: f64 = current.trim().parse().ok()?;
    (max > 0.0).then(|| (max - current).max(0.0) / max * 100.0)
}

/// The guard from the environment, over this host: the container's root,
/// and the two minimums.
pub fn from_env() -> Result<MachineGuard<Host>, String> {
    let number = |name: &str| std::env::var(name).ok().map(|v| v.trim().to_string());
    let bytes = match number("QD_WORKER_MIN_FREE_BYTES") {
        Some(v) => v
            .parse()
            .map_err(|_| "QD_WORKER_MIN_FREE_BYTES: not a number")?,
        None => DEFAULT_MIN_FREE_BYTES,
    };
    let pct = match number("QD_WORKER_MIN_FREE_MEMORY_PCT") {
        Some(v) => v
            .parse()
            .map_err(|_| "QD_WORKER_MIN_FREE_MEMORY_PCT: not a number")?,
        None => DEFAULT_MIN_FREE_MEMORY_PCT,
    };
    Ok(MachineGuard::new(
        Host,
        vec![PathBuf::from("/")],
        bytes,
        pct,
    ))
}
