//! Static caps the planner works within. These are process-lifetime
//! constants (derived once from the file-descriptor rlimit); the live,
//! moment-to-moment pressure lives in [`crate::SorterSnapshot`].

/// Default in-memory ceiling: sorts estimated at or below this never spill
/// when the budget has headroom.
const DEFAULT_IN_MEMORY_CEILING_BYTES: u64 = 8 * 1024 * 1024;
/// Default hard cap on concurrently-open merge readers per sort.
const DEFAULT_MAX_FAN_IN: u32 = 64;
/// Default cap on concurrent external sorts in the process.
const DEFAULT_MAX_CONCURRENT_EXTERNAL: u32 = 8;
/// Default file descriptors reserved for everything that is not sorting
/// (sockets, the block store, journals, …).
const DEFAULT_FD_SAFETY_MARGIN: u32 = 96;
/// Default floor for a spill run buffer.
const DEFAULT_MIN_RUN_BUFFER_BYTES: u64 = 1024 * 1024;
/// Default ceiling for a spill run buffer. Deliberately generous: under fd
/// pressure the planner raises the buffer to keep the run count — and thus
/// the merge-pass depth — bounded.
const DEFAULT_MAX_RUN_BUFFER_BYTES: u64 = 256 * 1024 * 1024;

/// Process-lifetime caps for the Sorter's planner.
#[derive(Debug, Clone)]
pub struct SorterConfig {
    in_memory_ceiling_bytes: u64,
    max_fan_in: u32,
    max_concurrent_external: u32,
    fd_safety_margin: u32,
    min_run_buffer_bytes: u64,
    max_run_buffer_bytes: u64,
}

impl Default for SorterConfig {
    fn default() -> Self {
        Self {
            in_memory_ceiling_bytes: DEFAULT_IN_MEMORY_CEILING_BYTES,
            max_fan_in: DEFAULT_MAX_FAN_IN,
            max_concurrent_external: DEFAULT_MAX_CONCURRENT_EXTERNAL,
            fd_safety_margin: DEFAULT_FD_SAFETY_MARGIN,
            min_run_buffer_bytes: DEFAULT_MIN_RUN_BUFFER_BYTES,
            max_run_buffer_bytes: DEFAULT_MAX_RUN_BUFFER_BYTES,
        }
    }
}

impl SorterConfig {
    /// Derive caps from the process's soft file-descriptor limit, clamping
    /// the per-sort fan-in ceiling so it can never approach the limit even
    /// before the live snapshot rations it further.
    #[must_use]
    pub fn from_fd_limit(soft_fd_limit: u32) -> Self {
        let usable = soft_fd_limit.saturating_sub(DEFAULT_FD_SAFETY_MARGIN);
        let max_fan_in = (usable / DEFAULT_MAX_CONCURRENT_EXTERNAL).clamp(2, DEFAULT_MAX_FAN_IN);
        Self {
            max_fan_in,
            ..Self::default()
        }
    }

    /// In-memory ceiling in bytes.
    #[must_use]
    pub fn in_memory_ceiling_bytes(&self) -> u64 {
        self.in_memory_ceiling_bytes
    }

    /// Hard cap on concurrently-open merge readers per sort.
    #[must_use]
    pub fn max_fan_in(&self) -> u32 {
        self.max_fan_in
    }

    /// Cap on concurrent external sorts in the process.
    #[must_use]
    pub fn max_concurrent_external(&self) -> u32 {
        self.max_concurrent_external
    }

    /// File descriptors reserved for non-sort work.
    #[must_use]
    pub fn fd_safety_margin(&self) -> u32 {
        self.fd_safety_margin
    }

    /// Floor for a spill run buffer.
    #[must_use]
    pub fn min_run_buffer_bytes(&self) -> u64 {
        self.min_run_buffer_bytes
    }

    /// Ceiling for a spill run buffer.
    #[must_use]
    pub fn max_run_buffer_bytes(&self) -> u64 {
        self.max_run_buffer_bytes
    }
}
