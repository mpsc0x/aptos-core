// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]
#![allow(dead_code)]
#![allow(unused_variables)]
use aptos_crypto::{HashValue, hash::CryptoHash};
use aptos_jellyfish_merkle::{node_type::Node, Key, TreeUpdateBatch};
use aptos_storage_interface::Result;
use aptos_types::{
    proof::{SparseMerkleProof, SparseMerkleProofExt, SparseMerkleRangeProof},
    transaction::Version,
};
use aptos_types::state_store::{
    state_key::StateKey, state_value::StateValue,
};
use bytes::Bytes;
use std::{
    collections::HashMap, marker::PhantomData
};
#[cfg(test)]
pub mod tests;

const MAX_ITEMS: usize = 1<<26; // about 64M leaf nodes
const ITEM_SIZE: usize = 48;
const MAX_BYTES: usize = 1<<35; // 32 GB
const MAX_BYTES_PER_ITEM: usize = 1<<10; // 1KB per item

struct StateKeyHash(HashValue);

struct ItemId {
    slot: u32, // slot with range [0, MAX_ITEMS)
    state_key_hash: StateKeyHash,
}

struct Item {
    id_num: ItemId,
    prev: ItemId,
    next: ItemId,
    value: Value,
}

enum Value {
    InMemory { bytes: Bytes },
    OnDisk { size: u16 },
}

// ActiveStateTree (ast) is a complete binary tree
struct ActiveStateTree<K> {
    items: HashMap<StateKeyHash, Item>,
    internal_nodes: [HashValue; MAX_ITEMS],
    latest_item: ItemId,
    oldest_item_with_in_mem_value: ItemId,
    oldest_item: ItemId,
    phantom_value: PhantomData<K>,
}

impl<K> ActiveStateTree<K>
where
    K: Key,
{

    // Do we need to distinguish between evict vs adding new element? different usecase?
    pub fn batch_put_value_set(
        &mut self,
        value_set: Vec<(StateKey, StateValue)>,
    ) -> Result<()> {
        for (key, value) in value_set {
            // check if the key exists in items
            if self.items.contains_key(&StateKeyHash(CryptoHash::hash(&key))) {
                // check if we need evict element from the tree

                // update the value

            } else {
                // add new element to the tree
            }
        }
        Ok(())
    }

    fn update_active_state_tree(&mut self, key: StateKey, value: StateValue) -> Result<()> {
        unimplemented!()
    }



    pub fn put_top_levels_nodes(
        &self,
        shard_root_nodes: Vec<Node<K>>,
        persisted_version: Option<Version>,
        version: Version,
    ) -> Result<(HashValue, TreeUpdateBatch<K>)> {
        unimplemented!()
    }

    pub fn get_with_proof(
        &self,
        key: HashValue,
        version: Version,
    ) -> Result<(Option<(HashValue, (K, Version))>, SparseMerkleProof)> {
        unimplemented!()
    }

    pub fn get_with_proof_ext(
        &self,
        key: HashValue,
        version: Version,
    ) -> Result<(Option<(HashValue, (K, Version))>, SparseMerkleProofExt)> {
        unimplemented!()
    }

    pub fn get_range_proof(
        &self,
        rightmost_key_to_prove: HashValue,
        version: Version,
    ) -> Result<SparseMerkleRangeProof> {
        unimplemented!()
    }
}
