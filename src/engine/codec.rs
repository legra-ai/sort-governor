//! Measure serialized frames before allocating their exact storage.

use std::io::{
    self,
    Write,
};
use std::sync::Arc;

use serde::Serialize;

use crate::engine::budget::MemoryBudget;
use crate::error::SorterError;

struct Counter {
    bytes: usize,
    limit: usize,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("encoded record length overflow"))?;
        if self.bytes > self.limit {
            return Err(io::Error::other("encoded record exceeds byte budget"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Encoded size of `value`, refusing values larger than the budget.
pub(crate) fn encoded_len<T: Serialize>(
    value: &T,
    budget: &Arc<MemoryBudget>,
) -> Result<usize, SorterError> {
    let mut count = Counter {
        bytes: 0,
        limit: usize::try_from(budget.limit()).unwrap_or(usize::MAX),
    };
    ciborium::ser::into_writer(value, &mut count)
        .map_err(|error| SorterError::Encode(error.to_string()))?;
    Ok(count.bytes)
}

/// Append `value` to `buffer` without growing it past its charged capacity.
/// `false` when it does not fit; `buffer` may then hold a partial tail the
/// caller truncates.
pub(crate) fn encode_into<T: Serialize>(
    value: &T,
    buffer: &mut Vec<u8>,
) -> Result<bool, SorterError> {
    let mut writer = CapacityWriter {
        bytes: buffer,
        overflow: false,
    };
    match ciborium::ser::into_writer(value, &mut writer) {
        Ok(()) => Ok(true),
        Err(_) if writer.overflow => Ok(false),
        Err(error) => Err(SorterError::Encode(error.to_string())),
    }
}

/// Appends into a pre-charged buffer, failing instead of reallocating.
struct CapacityWriter<'a> {
    bytes: &'a mut Vec<u8>,
    overflow: bool,
}
impl Write for CapacityWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.bytes.capacity() - self.bytes.len() {
            self.overflow = true;
            return Err(io::Error::other("frame exceeds charged chunk"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
