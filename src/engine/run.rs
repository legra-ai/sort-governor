//! Framed async spill/replay I/O with operation-scoped file ownership.
//!
//! Files contain a u64 row count followed by frames: u32 encoded length,
//! u64 declared decoded retained bytes, and CBOR. Both lengths are checked
//! against shared reservations before allocating a frame or decoding a row.
//!
//! Frames move through budget-charged chunks: a writer packs many frames into
//! one append and a reader decodes many frames from one positioned read. Each
//! chunk transfer is still a single descriptor-scoped operation, so no file
//! or permit outlives it and cancellation keeps the existing ownership rules.

mod reader;
mod writer;

pub(crate) use reader::RunReader;
pub(crate) use writer::RunWriter;
