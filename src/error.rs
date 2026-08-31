//! Failures the sort governor and its execution engine can surface.

/// An error raised while admitting, spilling, reading, or merging a sort.
#[derive(Debug, thiserror::Error)]
pub enum SorterError {
    /// A spill file or scratch directory could not be created, written,
    /// read, or removed.
    #[error("sort I/O error: {0}")]
    Io(#[from] async_fs_io::FsError),
    /// A run row could not be encoded for spilling.
    #[error("sort row encode failed: {0}")]
    Encode(String),
    /// A run row could not be decoded from a spill file.
    #[error("sort row decode failed: {0}")]
    Decode(String),
    /// The governor actor is no longer running (its task ended or every
    /// handle was dropped), so the sort could not be admitted.
    #[error("sort governor is not running")]
    Gone,
}
