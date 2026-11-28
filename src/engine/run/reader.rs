//! Buffered run reader: one positioned read serves many decoded frames.

use std::io::SeekFrom;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use async_fs_io::AsyncFile;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use super::writer::FRAME_HEADER_BYTES;
use crate::engine::budget::{
    MemoryBudget,
    MemoryCharge,
};
use crate::engine::operation::{
    IoScope,
    perform,
};
use crate::engine::row::RunRow;
use crate::error::SorterError;

/// Bytes read from the file starting at the reader's logical offset.
struct ReadChunk {
    bytes: Vec<u8>,
    consumed: usize,
    _charge: MemoryCharge,
}

impl ReadChunk {
    fn available(&self) -> &[u8] {
        &self.bytes[self.consumed..]
    }
}

pub(crate) struct RunReader<K, V> {
    fds: Arc<Semaphore>,
    path: PathBuf,
    offset: u64,
    remaining: u64,
    chunk: Option<ReadChunk>,
    budget: Arc<MemoryBudget>,
    io: Arc<IoScope>,
    marker: std::marker::PhantomData<(K, V)>,
}

impl<K: DeserializeOwned, V: DeserializeOwned> RunReader<K, V> {
    pub(crate) async fn open(
        path: &Path,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        io: Arc<IoScope>,
    ) -> Result<Self, SorterError> {
        let input = path.to_path_buf();
        let remaining = perform(
            Arc::clone(&fds),
            Arc::clone(&io),
            Arc::clone(&budget),
            input.capacity(),
            async move {
                let mut file = AsyncFile::open(&input).await?;
                let mut count = [0; 8];
                file.read_exact(&mut count).await?;
                Ok(u64::from_le_bytes(count))
            },
        )
        .await?;
        io.work().read(8);
        Ok(Self::from_position(path, 8, remaining, fds, budget, io))
    }

    pub(crate) fn from_position(
        path: &Path,
        offset: u64,
        remaining: u64,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        io: Arc<IoScope>,
    ) -> Self {
        Self {
            fds,
            path: path.to_path_buf(),
            offset,
            remaining,
            chunk: None,
            budget,
            io,
            marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn position(&self) -> (u64, u64) {
        (self.offset, self.remaining)
    }

    pub(crate) async fn next_row(&mut self) -> Result<Option<RunRow<K, V>>, SorterError> {
        if self.remaining == 0 {
            self.chunk = None;
            return Ok(None);
        }
        let header = self.buffered(FRAME_HEADER_BYTES).await?;
        let len = usize::try_from(u32::from_le_bytes(
            header[..4].try_into().expect("u32 length"),
        ))
        .map_err(|_| SorterError::RunLimit)?;
        let retained_bytes = usize::try_from(u64::from_le_bytes(
            header[4..FRAME_HEADER_BYTES]
                .try_into()
                .expect("u64 weight"),
        ))
        .map_err(|_| SorterError::RunLimit)?;
        let row_inline = std::mem::size_of::<RunRow<K, V>>();
        let row_charge = self.budget.reserve(retained_bytes.max(row_inline))?;
        let frame = FRAME_HEADER_BYTES
            .checked_add(len)
            .ok_or(SorterError::RunLimit)?;
        let bytes = self.buffered(frame).await?;
        let mut row: RunRow<K, V> = ciborium::de::from_reader(&bytes[FRAME_HEADER_BYTES..frame])
            .map_err(|error| {
                SorterError::Decode(format!("{error} (run {})", self.path.display()))
            })?;
        let chunk = self.chunk.as_mut().expect("buffered chunk");
        chunk.consumed += frame;
        self.offset = self
            .offset
            .checked_add(u64::try_from(frame).map_err(|_| SorterError::RunLimit)?)
            .ok_or(SorterError::RunLimit)?;
        self.remaining -= 1;
        if self.remaining == 0 {
            self.chunk = None;
        }
        row.retained_bytes = retained_bytes;
        row.charge = Some(row_charge);
        Ok(Some(row))
    }

    /// At least `needed` unconsumed bytes, refilling from the logical offset.
    async fn buffered(&mut self, needed: usize) -> Result<&[u8], SorterError> {
        if self
            .chunk
            .as_ref()
            .is_none_or(|chunk| chunk.available().len() < needed)
        {
            // Release the old chunk before admitting its successor; the
            // unconsumed tail (under one frame) is read again from the file.
            self.chunk = None;
            self.chunk = Some(self.fill(needed).await?);
        }
        Ok(self.chunk.as_ref().expect("filled chunk").available())
    }

    async fn fill(&mut self, needed: usize) -> Result<ReadChunk, SorterError> {
        // A single remaining frame (replay checkpoints) reads exactly; merge
        // and replay-to-end readers read ahead by a budget-sized chunk.
        let target = if self.remaining > 1 {
            self.budget.io_chunk_bytes(needed)
        } else {
            needed
        };
        let charge = self.budget.reserve_frame(target)?;
        let path = self.path.clone();
        let offset = self.offset;
        let bytes = perform(
            Arc::clone(&self.fds),
            Arc::clone(&self.io),
            Arc::clone(&self.budget),
            path.capacity(),
            async move {
                let mut file = AsyncFile::open(&path).await?;
                let on_disk = file.metadata().await?.len().saturating_sub(offset);
                let size = usize::try_from(on_disk)
                    .map_or(target, |on_disk| on_disk.min(target))
                    .max(needed);
                file.seek(SeekFrom::Start(offset)).await?;
                let mut bytes = vec![0; size];
                file.read_exact(&mut bytes).await?;
                Ok(bytes)
            },
        )
        .await?;
        self.io.work().read(bytes.len());
        Ok(ReadChunk {
            bytes,
            consumed: 0,
            _charge: charge,
        })
    }
}
