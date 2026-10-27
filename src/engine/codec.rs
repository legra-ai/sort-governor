//! Measure serialized frames before allocating their exact storage.

use std::io::{
    self,
    Write,
};
use std::sync::Arc;

use serde::Serialize;

use crate::engine::budget::{
    MemoryBudget,
    MemoryCharge,
};
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

pub(crate) fn encode<T: Serialize>(
    value: &T,
    budget: &Arc<MemoryBudget>,
) -> Result<(Vec<u8>, MemoryCharge), SorterError> {
    let mut count = Counter {
        bytes: 0,
        limit: usize::try_from(budget.limit()).unwrap_or(usize::MAX),
    };
    ciborium::ser::into_writer(value, &mut count)
        .map_err(|error| SorterError::Encode(error.to_string()))?;
    let charge = budget.reserve(count.bytes)?;
    let mut writer = FrameWriter {
        bytes: Vec::with_capacity(count.bytes),
        limit: count.bytes,
    };
    ciborium::ser::into_writer(value, &mut writer)
        .map_err(|error| SorterError::Encode(error.to_string()))?;
    let bytes = writer.bytes;
    if bytes.len() != count.bytes || bytes.capacity() > count.bytes {
        return Err(SorterError::Encode(
            "serialization changed between sizing and encoding".into(),
        ));
    }
    Ok((bytes, charge))
}

struct FrameWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for FrameWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other("serialized row changed after sizing"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
