//! Retained array capacity remains charged after merge heads drain.

use std::mem::size_of;
use std::sync::Arc;

use super::{
    HeapEntry,
    MergeSource,
    MergeState,
};
use crate::engine::budget::MemoryBudget;

type WideKey = [[u8; 32]; 32];

#[test]
fn retained_merge_array_capacity_is_not_released_with_occupied_heads() {
    let budget = MemoryBudget::new(1024 * 1024);
    // bounded: one fixed 64-source merge frontier with a 1KiB inline key.
    let state: MergeState<WideKey, u32> = MergeState::new(64, false, Arc::clone(&budget)).unwrap();
    let arrays = state.sources.capacity() * size_of::<MergeSource<WideKey, u32>>()
        + state.heap.capacity() * size_of::<std::cmp::Reverse<HeapEntry<WideKey>>>();
    assert!(
        budget.used() >= u64::try_from(arrays).unwrap(),
        "unoccupied source/head slots still retain their array allocation"
    );
    drop(state);
    assert_eq!(budget.used(), 0);
}
