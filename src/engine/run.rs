//! Framed, async spill-run I/O over [`async_fs_io::AsyncFile`].
//!
//! A run file is `[u64 LE record_count]` followed by `record_count`
//! length-prefixed CBOR rows (`[u32 LE len][cbor]`). The count header is
//! written last via a seek-back so a merge that dedups (and so does not
//! know its output length up front) can still produce a well-formed run.
//!
//! Each [`RunReader`] holds exactly one open file descriptor and reads one
//! row at a time — the read path never buffers a whole run, so memory stays
//! bounded regardless of run size.

use std::io::SeekFrom;
use std::path::{
    Path,
    PathBuf,
};

use async_fs_io::AsyncFile;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::engine::row::RunRow;
use crate::error::SorterError;

/// Writes a sorted run to disk, counting rows and back-patching the header.
pub(crate) struct RunWriter {
    file: AsyncFile,
    path: PathBuf,
    count: u64,
    /// Encode buffer reused across rows so a spill allocates once, not per row.
    scratch: Vec<u8>,
}

impl RunWriter {
    /// Create a fresh run file with a placeholder count header.
    pub(crate) async fn create(path: PathBuf) -> Result<Self, SorterError> {
        let mut file = AsyncFile::create(&path).await?;
        file.write_all(&0u64.to_le_bytes()).await?;
        Ok(Self {
            file,
            path,
            count: 0,
            scratch: Vec::new(),
        })
    }

    /// Append one row.
    pub(crate) async fn write_row<K, V>(&mut self, row: &RunRow<K, V>) -> Result<(), SorterError>
    where
        K: Serialize,
        V: Serialize,
    {
        self.scratch.clear();
        ciborium::ser::into_writer(row, &mut self.scratch)
            .map_err(|err| SorterError::Encode(err.to_string()))?;
        let len = u32::try_from(self.scratch.len())
            .map_err(|err| SorterError::Encode(format!("run row too large: {err}")))?;
        self.file.write_all(&len.to_le_bytes()).await?;
        self.file.write_all(&self.scratch).await?;
        self.count += 1;
        Ok(())
    }

    /// Flush, back-patch the count header, and return the run path.
    pub(crate) async fn finish(mut self) -> Result<PathBuf, SorterError> {
        self.file.flush().await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.write_all(&self.count.to_le_bytes()).await?;
        self.file.flush().await?;
        Ok(self.path)
    }
}

/// Reads a run file back one row at a time, holding a single fd.
pub(crate) struct RunReader<K, V> {
    file: AsyncFile,
    path: String,
    remaining: u64,
    /// Decode buffer reused across rows; it grows to the largest row seen.
    buf: Vec<u8>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> RunReader<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    /// Open a run and read its row count.
    pub(crate) async fn open(path: &Path) -> Result<Self, SorterError> {
        let mut file = AsyncFile::open(path).await?;
        let mut header = [0u8; 8];
        file.read_exact(&mut header).await?;
        Ok(Self {
            file,
            path: path.display().to_string(),
            remaining: u64::from_le_bytes(header),
            buf: Vec::new(),
            _marker: std::marker::PhantomData,
        })
    }

    /// Read the next row, or `None` at end of run.
    pub(crate) async fn next_row(&mut self) -> Result<Option<RunRow<K, V>>, SorterError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut len_bytes = [0u8; 4];
        self.file.read_exact(&mut len_bytes).await?;
        let len = usize::try_from(u32::from_le_bytes(len_bytes))
            .map_err(|err| SorterError::Decode(format!("run row length overflow: {err}")))?;
        self.buf.resize(len, 0);
        self.file.read_exact(&mut self.buf).await?;
        let row = ciborium::de::from_reader(self.buf.as_slice())
            .map_err(|err| SorterError::Decode(format!("{err} (run {})", self.path)))?;
        self.remaining -= 1;
        Ok(Some(row))
    }
}
