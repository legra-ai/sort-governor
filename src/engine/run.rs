//! Framed async spill/replay I/O with operation-scoped file ownership.
//!
//! Files contain a u64 row count followed by frames: u32 encoded length,
//! u64 declared decoded retained bytes, and CBOR. Both lengths are checked
//! against shared reservations before allocating a frame or decoding a row.

use std::io::SeekFrom;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use async_fs_io::AsyncFile;
use serde::Serialize;
use serde::de::DeserializeOwned;
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

pub(crate) struct RunWriter {
    fds: Arc<Semaphore>,
    path: PathBuf,
    count: u64,
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
            budget,
            io,
        })
    }

    pub(crate) async fn write_row<K: Serialize, V: Serialize>(
        &mut self,
        row: &RunRow<K, V>,
    ) -> Result<(), SorterError> {
        let (bytes, charge) = codec::encode(row, &self.budget)?;
        let len =
            u32::try_from(bytes.len()).map_err(|error| SorterError::Encode(error.to_string()))?;
        let weight = u64::try_from(row.retained_bytes).map_err(|_| SorterError::RunLimit)?;
        let path = self.path.clone();
        perform(
            Arc::clone(&self.fds),
            Arc::clone(&self.io),
            Arc::clone(&self.budget),
            path.capacity(),
            async move {
                let _charge = charge;
                let mut file = AsyncFile::open_append(&path).await?;
                file.write_all(&len.to_le_bytes()).await?;
                file.write_all(&weight.to_le_bytes()).await?;
                file.write_all(&bytes).await?;
                file.flush().await?;
                Ok(())
            },
        )
        .await?;
        self.count = self.count.checked_add(1).ok_or(SorterError::RunLimit)?;
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<PathBuf, SorterError> {
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

pub(crate) struct RunReader<K, V> {
    fds: Arc<Semaphore>,
    path: PathBuf,
    offset: u64,
    remaining: u64,
    budget: Arc<MemoryBudget>,
    io: Arc<IoScope>,
    marker: std::marker::PhantomData<(K, V)>,
}

struct Frame {
    bytes: Vec<u8>,
    retained_bytes: usize,
    row_charge: MemoryCharge,
    _frame_charge: MemoryCharge,
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
            return Ok(None);
        }
        let path = self.path.clone();
        let offset = self.offset;
        let budget = Arc::clone(&self.budget);
        let row_inline = std::mem::size_of::<RunRow<K, V>>();
        let frame = perform(
            Arc::clone(&self.fds),
            Arc::clone(&self.io),
            Arc::clone(&self.budget),
            path.capacity(),
            async move {
                let mut file = AsyncFile::open(&path).await?;
                file.seek(SeekFrom::Start(offset)).await?;
                let mut length = [0; 4];
                let mut weight = [0; 8];
                file.read_exact(&mut length).await?;
                file.read_exact(&mut weight).await?;
                let len = usize::try_from(u32::from_le_bytes(length))
                    .map_err(|_| SorterError::RunLimit)?;
                let retained_bytes = usize::try_from(u64::from_le_bytes(weight))
                    .map_err(|_| SorterError::RunLimit)?;
                let row_charge = budget.reserve(retained_bytes.max(row_inline))?;
                let frame_charge = budget.reserve_frame(len)?;
                let mut bytes = vec![0; len];
                file.read_exact(&mut bytes).await?;
                Ok(Frame {
                    bytes,
                    retained_bytes,
                    row_charge,
                    _frame_charge: frame_charge,
                })
            },
        )
        .await?;
        let mut row: RunRow<K, V> =
            ciborium::de::from_reader(frame.bytes.as_slice()).map_err(|error| {
                SorterError::Decode(format!("{error} (run {})", self.path.display()))
            })?;
        let len = u64::try_from(frame.bytes.len()).map_err(|_| SorterError::RunLimit)?;
        self.offset = self
            .offset
            .checked_add(12 + len)
            .ok_or(SorterError::RunLimit)?;
        self.remaining -= 1;
        row.retained_bytes = frame.retained_bytes;
        row.charge = Some(frame.row_charge);
        Ok(Some(row))
    }
}
