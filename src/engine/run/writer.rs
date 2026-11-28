//! Buffered run writer: frames are packed into one charged chunk per append.

use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::Semaphore;

use crate::engine::budget::{
    MemoryBudget,
    MemoryCharge,
};
use crate::engine::codec;
use crate::engine::operation::{
    IoScope,
    perform,
};
use crate::engine::row::RunRow;
use crate::error::SorterError;

/// Frame header: u32 encoded length then u64 retained weight.
pub(super) const FRAME_HEADER_BYTES: usize = 12;

/// Pending frames awaiting one append, with the charge for its capacity.
struct PendingChunk {
    bytes: Vec<u8>,
    _charge: MemoryCharge,
}

impl PendingChunk {
    /// Encode one frame in place. `false` (chunk unchanged) when it does not
    /// fit the remaining charged capacity.
    fn append<T: Serialize>(&mut self, row: &T, weight: u64) -> Result<bool, SorterError> {
        let start = self.bytes.len();
        if self.bytes.capacity() - start < FRAME_HEADER_BYTES {
            return Ok(false);
        }
        self.bytes.extend_from_slice(&[0; FRAME_HEADER_BYTES]);
        let body = start + FRAME_HEADER_BYTES;
        if !codec::encode_into(row, &mut self.bytes)? {
            self.bytes.truncate(start);
            return Ok(false);
        }
        let len = u32::try_from(self.bytes.len() - body)
            .map_err(|error| SorterError::Encode(error.to_string()))?;
        self.bytes[start..start + 4].copy_from_slice(&len.to_le_bytes());
        self.bytes[start + 4..body].copy_from_slice(&weight.to_le_bytes());
        Ok(true)
    }
}

pub(crate) struct RunWriter {
    fds: Arc<Semaphore>,
    path: PathBuf,
    count: u64,
    pending: Option<PendingChunk>,
    budget: Arc<MemoryBudget>,
    io: Arc<IoScope>,
}

impl RunWriter {
    pub(crate) async fn create(
        path: PathBuf,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        io: Arc<IoScope>,
    ) -> Result<Self, SorterError> {
        let output = path.clone();
        perform(
            Arc::clone(&fds),
            Arc::clone(&io),
            Arc::clone(&budget),
            output.capacity(),
            async move {
                async_fs_io::create_with_bytes(&output, 0u64.to_le_bytes().to_vec()).await?;
                Ok(())
            },
        )
        .await?;
        Ok(Self {
            fds,
            path,
            count: 0,
            pending: None,
            budget,
            io,
        })
    }

    /// Queue one frame. It reaches the file at the next chunk boundary,
    /// [`Self::flush`], or [`Self::finish`]. The row encodes straight into
    /// the charged chunk; only a row that overflows a fresh chunk is
    /// measured first to size a dedicated one.
    pub(crate) async fn write_row<K: Serialize, V: Serialize>(
        &mut self,
        row: &RunRow<K, V>,
    ) -> Result<(), SorterError> {
        let weight = u64::try_from(row.retained_bytes).map_err(|_| SorterError::RunLimit)?;
        for _ in 0..2 {
            if self.pending.is_none() {
                self.pending = Some(self.open_chunk(FRAME_HEADER_BYTES + 1)?);
            }
            let chunk = self.pending.as_mut().expect("pending chunk");
            if chunk.append(row, weight)? {
                self.count = self.count.checked_add(1).ok_or(SorterError::RunLimit)?;
                return Ok(());
            }
            if chunk.bytes.is_empty() {
                break;
            }
            self.flush().await?;
        }
        // The row exceeds a whole default chunk: size one exactly for it.
        let encoded = codec::encoded_len(row, &self.budget)?;
        let frame = FRAME_HEADER_BYTES
            .checked_add(encoded)
            .ok_or(SorterError::RunLimit)?;
        self.pending = None;
        let mut chunk = self.open_chunk(frame)?;
        if !chunk.append(row, weight)? {
            return Err(SorterError::Encode(
                "serialization changed between sizing and encoding".into(),
            ));
        }
        self.pending = Some(chunk);
        self.count = self.count.checked_add(1).ok_or(SorterError::RunLimit)?;
        Ok(())
    }

    fn open_chunk(&self, minimum: usize) -> Result<PendingChunk, SorterError> {
        let capacity = self.budget.io_chunk_bytes(minimum);
        let charge = self.budget.reserve(capacity)?;
        let bytes = Vec::with_capacity(capacity);
        assert_eq!(bytes.capacity(), capacity, "exact chunk reservation");
        Ok(PendingChunk {
            bytes,
            _charge: charge,
        })
    }

    /// Append every queued frame in one descriptor-scoped operation.
    pub(crate) async fn flush(&mut self) -> Result<(), SorterError> {
        let Some(chunk) = self.pending.take() else {
            return Ok(());
        };
        let path = self.path.clone();
        perform(
            Arc::clone(&self.fds),
            Arc::clone(&self.io),
            Arc::clone(&self.budget),
            path.capacity(),
            async move {
                let PendingChunk {
                    bytes,
                    _charge: charge,
                } = chunk;
                async_fs_io::append_bytes(&path, bytes).await?;
                drop(charge);
                Ok(())
            },
        )
        .await
    }

    pub(crate) async fn finish(mut self) -> Result<PathBuf, SorterError> {
        self.flush().await?;
        let path = self.path.clone();
        let count = self.count;
        perform(
            self.fds,
            self.io,
            Arc::clone(&self.budget),
            path.capacity(),
            async move {
                async_fs_io::write_bytes_at(&path, 0, count.to_le_bytes().to_vec()).await?;
                Ok(())
            },
        )
        .await?;
        Ok(self.path)
    }
}
