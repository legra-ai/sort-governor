//! Framed, async spill-run I/O over [`async_fs_io::AsyncFile`].
//!
//! A run file is `[u64 LE record_count]` followed by `record_count`
//! length-prefixed CBOR rows (`[u32 LE len][cbor]`). The count header is
//! written last via a seek-back so a merge that dedups (and so does not
//! know its output length up front) can still produce a well-formed run.
//!
//! Readers and writers acquire a shared descriptor permit only during an
//! actual operation; no descriptor remains open while a consumer is parked.
//! Readers read one row at a time — the read path never buffers a whole run, so
//! memory stays bounded regardless of run size.

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

use crate::engine::budget::MemoryBudget;
use crate::engine::codec;
use crate::engine::row::RunRow;
use crate::error::SorterError;

/// Writes a sorted run to disk, counting rows and back-patching the header.
pub(crate) struct RunWriter {
    fds: Arc<Semaphore>,
    path: PathBuf,
    count: u64,
    /// Shared budget charged before allocating each serialized frame.
    budget: Arc<MemoryBudget>,
}

impl RunWriter {
    /// Create a fresh run file with a placeholder count header.
    pub(crate) async fn create(
        path: PathBuf,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
    ) -> Result<Self, SorterError> {
        let _permit = Arc::clone(&fds)
            .acquire_owned()
            .await
            .map_err(|_| SorterError::Gone)?;
        let mut file = AsyncFile::create(&path).await?;
        file.write_all(&0u64.to_le_bytes()).await?;
        file.flush().await?;
        Ok(Self {
            fds,
            path,
            count: 0,
            budget,
        })
    }

    /// Append one row.
    pub(crate) async fn write_row<K, V>(&mut self, row: &RunRow<K, V>) -> Result<(), SorterError>
    where
        K: Serialize,
        V: Serialize,
    {
        let (bytes, _charge) = codec::encode(row, &self.budget)?;
        let len =
            u32::try_from(bytes.len()).map_err(|error| SorterError::Encode(error.to_string()))?;
        let _permit = Arc::clone(&self.fds)
            .acquire_owned()
            .await
            .map_err(|_| SorterError::Gone)?;
        let mut file = AsyncFile::open_append(&self.path).await?;
        file.write_all(&len.to_le_bytes()).await?;
        file.write_all(&(row.retained_bytes as u64).to_le_bytes())
            .await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        self.count = self.count.checked_add(1).ok_or(SorterError::RunLimit)?;
        Ok(())
    }

    /// Flush, back-patch the count header, and return the run path.
    pub(crate) async fn finish(self) -> Result<PathBuf, SorterError> {
        let _permit = Arc::clone(&self.fds)
            .acquire_owned()
            .await
            .map_err(|_| SorterError::Gone)?;
        let mut file = AsyncFile::open_write_at_end(&self.path).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&self.count.to_le_bytes()).await?;
        file.flush().await?;
        Ok(self.path)
    }
}

/// Reads a run file back one row at a time, holding a single fd.
pub(crate) struct RunReader<K, V> {
    fds: Arc<Semaphore>,
    path: PathBuf,
    offset: u64,
    remaining: u64,
    /// Shared budget charged before reading and decoding each row.
    budget: Arc<MemoryBudget>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> RunReader<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    /// Open a run and read its row count.
    pub(crate) async fn open(
        path: &Path,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
    ) -> Result<Self, SorterError> {
        let _permit = Arc::clone(&fds)
            .acquire_owned()
            .await
            .map_err(|_| SorterError::Gone)?;
        let mut file = AsyncFile::open(path).await?;
        let mut header = [0u8; 8];
        file.read_exact(&mut header).await?;
        Ok(Self {
            fds,
            path: path.to_path_buf(),
            offset: 8,
            remaining: u64::from_le_bytes(header),
            budget,
            _marker: std::marker::PhantomData,
        })
    }

    pub(crate) fn from_position(
        path: &Path,
        offset: u64,
        remaining: u64,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
    ) -> Self {
        Self {
            fds,
            path: path.to_path_buf(),
            offset,
            remaining,
            budget,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn position(&self) -> (u64, u64) {
        (self.offset, self.remaining)
    }

    /// Read the next row, or `None` at end of run.
    pub(crate) async fn next_row(&mut self) -> Result<Option<RunRow<K, V>>, SorterError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let _permit = Arc::clone(&self.fds)
            .acquire_owned()
            .await
            .map_err(|_| SorterError::Gone)?;
        let mut file = AsyncFile::open(&self.path).await?;
        file.seek(SeekFrom::Start(self.offset)).await?;
        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes).await?;
        let len = usize::try_from(u32::from_le_bytes(len_bytes))
            .map_err(|err| SorterError::Decode(format!("run row length overflow: {err}")))?;
        let mut weight = [0u8; 8];
        file.read_exact(&mut weight).await?;
        let retained_bytes =
            usize::try_from(u64::from_le_bytes(weight)).map_err(|_| SorterError::RunLimit)?;
        let row_charge = self
            .budget
            .reserve(retained_bytes.max(std::mem::size_of::<RunRow<K, V>>()))?;
        let _frame_charge = self.budget.reserve(len)?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes).await?;
        let mut row: RunRow<K, V> = ciborium::de::from_reader(bytes.as_slice())
            .map_err(|err| SorterError::Decode(format!("{err} (run {})", self.path.display())))?;
        self.offset = self
            .offset
            .checked_add(12 + len as u64)
            .ok_or(SorterError::RunLimit)?;
        row.retained_bytes = retained_bytes;
        row.charge = Some(row_charge);
        self.remaining -= 1;
        Ok(Some(row))
    }
}
