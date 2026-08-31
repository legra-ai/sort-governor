//! One key/value pair as it lives in an in-memory run and on disk.

use serde::{
    Deserialize,
    Serialize,
};

/// A single row threaded through the sort: the ordering key and its
/// payload value. Serialized with the key first so a spilled run is
/// self-describing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunRow<K, V> {
    /// The ordering key.
    pub key: K,
    /// The payload carried alongside the key.
    pub value: V,
}

impl<K, V> RunRow<K, V> {
    /// Pair a key with its value.
    pub(crate) fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}
