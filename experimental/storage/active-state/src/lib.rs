// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]
#![allow(dead_code)]
#![allow(unused_variables)]
use crate::atomic_bitmap::AtomicBitmap;
use aptos_crypto::{hash::CryptoHash, HashValue};
use aptos_storage_interface::Result;
use aptos_types::{
    state_store::{state_key::StateKey, state_value::StateValue},
    transaction::Version,
};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::{channel, Receiver, Sender},
    Arc,
};
pub mod atomic_bitmap;
#[cfg(test)]
pub mod tests;

const MAX_ITEMS: usize = 1 << 26; // about 64M leaf nodes
const ITEM_SIZE: usize = 48;
const MAX_BYTES: usize = 1 << 35; // 32 GB
const MAX_BYTES_PER_ITEM: usize = 1 << 10; // 1KB per item

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct StateKeyHash(HashValue);

//TODO(bowu) check the order is correct after hashing
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct LeafNodeId {
    pub version: Version,
    pub slot: u32, // slot with range [0, MAX_ITEMS)
    pub state_key_hash: StateKeyHash,
}

struct LeafNode {
    id: LeafNodeId,
    last_used: AtomicU64,
    value: Value,
}

enum Value {
    InMemory { bytes: Bytes },
    OnDisk { size: u16 },
}

// Wrapper of updates to be persisted to active state tree and jmt repsectively
pub struct TreeDbUpdates {
    active_state_tree_updates: Vec<(LeafNodeId, LeafNode)>,
    jmt_updates: Vec<(StateKey, StateValue)>,
}

// Active State Tree Proof, used to prove the existence of a leaf node in the active state tree
struct ActiveStateTreeProof {
    leaf_node: Option<LeafNode>,
    siblings: Vec<HashValue>,
}

// used to prove the existence of a range of leaf nodes in the active state tree
struct ActiveStateTreeRangeProof {
    right_siblings: Vec<HashValue>,
}

// ActiveStateTree (ast) is a complete binary tree
// It provides a conconcurrent LRU cache for a dense state merkel tree.
// It garantees the num of leaf nodes is always less than MAX_ITEMS.
// It will garentee the total size of tree in mem is less than MAX_BYTES after background maintenance done.
struct ActiveStateTree {
    items: DashMap<HashValue, LeafNode>,
    internal_nodes: [HashValue; MAX_ITEMS],
    used_slots_cnt: AtomicU64,
    slot_bitmap: Arc<AtomicBitmap>,
    max_occupied_slots: u64, // in case we don't want to completely fill the tree
    global_usage_count: AtomicU64, // track the most recent usage count
    oldest_usage_count_in_mem_value: AtomicU64, // track the oldest usage count of in-memory value
    oldest_usage_count: AtomicU64, // track the oldest usage count
    tree_maintainer: Sender<ActiveStateTreeUpdate>, // notify the maintainer to update the cache
}

impl ActiveStateTree {
    pub fn new(tree_maintainer: Sender<ActiveStateTreeUpdate>) -> Self {
        ActiveStateTree {
            items: DashMap::new(),
            internal_nodes: [HashValue::zero(); MAX_ITEMS],
            used_slots_cnt: AtomicU64::new(0),
            slot_bitmap: Arc::new(AtomicBitmap::new(64_000_000)),
            max_occupied_slots: (MAX_ITEMS / 2) as u64,
            global_usage_count: AtomicU64::new(0),
            oldest_usage_count_in_mem_value: AtomicU64::new(0),
            oldest_usage_count: AtomicU64::new(0),
            tree_maintainer,
        }
    }

    // Do we need to distinguish between evict vs adding new element? different usecase?
    pub fn batch_put_value_set(
        &mut self,
        value_set: Vec<(StateKey, StateValue)>,
    ) -> Result<TreeDbUpdates> {
        unimplemented!()
    }

    fn add_leaf_node(&mut self, key: StateKey, value: StateValue, version: Version) -> Result<()> {
        let state_key_hash = key.hash();
        if self.items.contains_key(&state_key_hash) {
            let mut leaf_node = self.items.get_mut(&state_key_hash).unwrap_or_else(|| {
                panic!("active state tree leaf node not found {}", state_key_hash)
            });
            // skip if the newer updates are already recorded
            if leaf_node.id.version > version {
                return Ok(());
            }

            // Update the value
            if leaf_node.last_used.load(Ordering::SeqCst)
                < self.oldest_usage_count_in_mem_value.load(Ordering::SeqCst)
            {
                // move the value to memory
                leaf_node.value = Value::InMemory {
                    bytes: value.bytes().clone(),
                };

                //TODO(bowu): update the oldest in-mem timestamp in a separate thread
            } else {
                leaf_node.value = Value::InMemory {
                    bytes: value.bytes().clone(),
                };
            }
            // update the timestamp
            leaf_node.last_used.store(
                self.global_usage_count.fetch_add(1, Ordering::SeqCst),
                Ordering::SeqCst,
            );
            return Ok(());
        }
        if self.used_slots_cnt.load(Ordering::SeqCst) >= self.max_occupied_slots {
            self.evict_oldest_leaf_node()?;
        }

        // Add new leaf to the tree

        // If tree is full, we revert the newly added leaf node

        Ok(())
    }

    fn evict_oldest_leaf_node(&mut self) -> Result<LeafNodeId> {
        unimplemented!()
    }

    // reset the gloabl usage count
    // evict old leaf nodes in backgroup jobs
    fn refresh_cache(&mut self) -> Result<()> {
        unimplemented!()
    }

    pub fn get_with_proof(&self, key: HashValue, version: Version) -> Result<ActiveStateTreeProof> {
        unimplemented!()
    }

    pub fn get_with_proof_ext(
        &self,
        key: HashValue,
        version: Version,
    ) -> Result<ActiveStateTreeProof> {
        unimplemented!()
    }

    pub fn get_range_proof(
        &self,
        rightmost_key_to_prove: HashValue,
        version: Version,
    ) -> Result<ActiveStateTreeRangeProof> {
        unimplemented!()
    }
}

// find the next in-mem usage count
pub struct TimestampUpdate {}

pub enum ActiveStateTreeUpdate {
    TimestampUpdate(TimestampUpdate),
    ResetGlobalUsageCount(u64),
    PersistTreeUpdates(TreeDbUpdates),
}
struct ActiveStateTreeMaintainer {
    active_state_tree: Arc<ActiveStateTree>,
    updates_receiver: Receiver<ActiveStateTreeUpdate>,
}
