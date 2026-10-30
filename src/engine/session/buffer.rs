//! Charged run-buffer capacity and the bounded session frontier.

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::SortSession;
use crate::engine::row::RunRow;
use crate::error::SorterError;
use crate::plan::SortPlan;

impl<K, V> SortSession<K, V>
where
    K: Ord + Clone + Serialize + DeserializeOwned + Send + 'static,
    V: Serialize + DeserializeOwned + Send + 'static,
{
    pub(super) fn reserve_metadata(&mut self) -> Result<(), SorterError> {
        if !self.metadata_reserved {
            // bounded: 64 frontier paths and their maximum session-relative
            // name, plus the fixed merge frontier and session struct itself.
            let path_bytes = self
                .dir
                .capacity()
                .checked_add(128)
                .ok_or(SorterError::RunLimit)?;
            let bytes = path_bytes
                .checked_mul(256)
                .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
                .ok_or(SorterError::RunLimit)?;
            self.cleanup.retain(self.budget.reserve(bytes)?);
            self.metadata_reserved = true;
        }
        Ok(())
    }

    pub(super) fn grow_buffer(&mut self) -> Result<(), SorterError> {
        if self.buffer.len() < self.buffer.capacity() {
            return Ok(());
        }
        let capacity = self
            .buffer
            .capacity()
            .max(1)
            .checked_mul(2)
            .ok_or(SorterError::RunLimit)?;
        let bytes = capacity
            .checked_mul(std::mem::size_of::<RunRow<K, V>>())
            .ok_or(SorterError::RunLimit)?;
        let charge = self.budget.reserve(bytes)?;
        self.buffer.reserve_exact(capacity - self.buffer.len());
        assert_eq!(
            self.buffer.capacity(),
            capacity,
            "exact row capacity reservation"
        );
        self.buffer_charge = Some(charge);
        Ok(())
    }

    pub(super) fn buffer_limit(&self) -> usize {
        let shared = usize::try_from(self.budget.limit() / 8)
            .unwrap_or(usize::MAX)
            .max(1);
        match self.plan {
            SortPlan::InMemory => shared.min(8 * 1024 * 1024),
            SortPlan::External {
                run_buffer_bytes, ..
            } => shared.min(run_buffer_bytes.max(1)),
        }
    }
}
