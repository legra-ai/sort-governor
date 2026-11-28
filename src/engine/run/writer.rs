//! Buffered run writer: frames are packed into one charged chunk per append.

use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use async_fs_io::AsyncFile;
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
                let mut file = AsyncFile::create(&output).await?;
                file.write_all(&0u64.to_le_bytes()).await?;
                file.flush().await?;
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
    /// [`Self::flush`], or [`Self::finish`].
    pub(crate) async fn write_row<K: Serialize, V: Serialize>(
        &mut self,
        row: &RunRow<K, V>,
    ) -> Result<(), SorterError> {
        let (bytes, charge) = codec::encode(row, &self.budget)?;
        let len =
            u32::try_from(bytes.len()).map_err(|error| SorterError::Encode(error.to_string()))?;
        let weight = u64::try_from(row.retained_bytes).map_err(|_| SorterError::RunLimit)?;
        let frame = FRAME_HEADER_BYTES
            .checked_add(bytes.len())
            .ok_or(SorterError::RunLimit)?;
        if self
            .pending
            .as_ref()
            .is_some_and(|chunk| chunk.bytes.len() + frame > chunk.bytes.capacity())
        {
            self.flush().await?;
        }
        if self.pending.is_none() {
            let capacity = self.budget.io_chunk_bytes(frame);
            let charge = self.budget.reserve(capacity)?;
            let bytes = Vec::with_capacity(capacity);
            assert_eq!(bytes.capacity(), capacity, "exact chunk reservation");
            self.pending = Some(PendingChunk {
                bytes,
                _charge: charge,
            });
        }
        let chunk = self.pending.as_mut().expect("pending chunk");
        chunk.bytes.extend_from_slice(&len.to_le_bytes());
        chunk.bytes.extend_from_slice(&weight.to_le_bytes());
        chunk.bytes.extend_from_slice(&bytes);
        drop(bytes);
        drop(charge);
        self.count = self.count.checked_add(1).ok_or(SorterError::RunLimit)?;
        Ok(())
    }

    /// Append every queued frame in one descriptor-scoped operation.
    pub(crate) async fn flush(&mut self) -> Result<(), SorterError> {
        let Some(chunk) = self.pending.take() else {
            return Ok(());
        };
        // bounded: Tokio's file buffer copies at most one chunk per write.
        let copy = self.budget.reserve(chunk.bytes.len())?;
        let path = self.path.clone();
        perform(
            Arc::clone(&self.fds),
            Arc::clone(&self.io),
            Arc::clone(&self.budget),
            path.capacity(),
            async move {
                let _copy = copy;
                let mut file = AsyncFile::open_append(&path).await?;
                file.write_all(&chunk.bytes).await?;
                file.flush().await?;
                drop(chunk);
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
                let mut file = AsyncFile::open_write_at_end(&path).await?;
                file.seek(SeekFrom::Start(0)).await?;
                file.write_all(&count.to_le_bytes()).await?;
                file.flush().await?;
                Ok(())
            },
        )
        .await?;
        Ok(self.path)
    }
}
