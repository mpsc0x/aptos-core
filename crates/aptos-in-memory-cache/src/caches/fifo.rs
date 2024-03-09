// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{Cache, Incrementable, Ordered, Weighted};
use dashmap::DashMap;
use std::hash::Hash;

#[derive(Debug, Clone, Copy)]
struct CacheMetadata<K> {
    max_size_in_bytes: u64,
    total_size_in_bytes: u64,
    last_key: Option<K>,
    first_key: Option<K>,
}

/// FIFO is a simple in-memory cache with a deterministic FIFO eviction policy.
pub struct FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<Self, K, V> + Send + Sync + Clone,
    V: Weighted + Send + Sync + Clone,
{
    /// Cache maps the cache key to the deserialized Transaction.
    items: DashMap<K, V>,
    cache_metadata: CacheMetadata<K>,
}

impl<K, V> FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<Self, K, V> + Send + Sync + Clone,
    V: Weighted + Send + Sync + Clone,
{
    pub fn new(max_size_in_bytes: u64) -> Self {
        FIFOCache {
            items: DashMap::new(),
            cache_metadata: CacheMetadata {
                max_size_in_bytes,
                total_size_in_bytes: 0,
                last_key: None,
                first_key: None,
            },
        }
    }

    fn pop(&mut self) -> Option<u64> {
        if let Some(first_key) = self.cache_metadata.first_key.clone() {
            let next_key = first_key.next(&self);
            return self.items.remove(&first_key).map(|(_, v)| {
                let weight = v.weight();
                self.cache_metadata.first_key = Some(next_key);
                self.cache_metadata.total_size_in_bytes -= weight;
                weight
            });
        }
        None
    }

    fn evict(&mut self, new_value_weight: u64) -> (u64, u64) {
        let mut garbage_collection_count = 0;
        let mut garbage_collection_size = 0;
        while self.cache_metadata.total_size_in_bytes + new_value_weight
            > self.cache_metadata.max_size_in_bytes
        {
            if let Some(weight) = self.pop() {
                garbage_collection_count += 1;
                garbage_collection_size += weight;
            }
        }
        (garbage_collection_count, garbage_collection_size)
    }

    fn insert_impl(&mut self, key: K, value: V) {
        self.cache_metadata.last_key = Some(key.clone());
        self.cache_metadata.total_size_in_bytes += value.weight();
        self.items.insert(key, value);
    }
}

impl<K, V> Cache<K, V> for FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<Self, K, V> + Send + Sync + Clone,
    V: Weighted + Send + Sync + Clone,
{
    fn get(&self, key: &K) -> Option<V> {
        self.items.get(key).map(|v| v.value().clone())
    }

    fn insert(&mut self, key: K, value: V) -> (u64, u64) {
        // If cache is empty, set the first to the new key.
        if self.items.is_empty() {
            self.cache_metadata.first_key = Some(key.clone());
        }

        // Evict until enough space is available for next value.
        let (garbage_collection_count, garbage_collection_size) = self.evict(value.weight());
        self.insert_impl(key, value);

        return (garbage_collection_count, garbage_collection_size);
    }
}

impl<K, V> Ordered<K, V> for FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<Self, K, V> + Send + Sync + Clone,
    V: Weighted + Send + Sync + Clone,
{
    fn first_key(&self) -> Option<K> {
        self.cache_metadata.first_key.clone()
    }

    fn last_key(&self) -> Option<K> {
        self.cache_metadata.last_key.clone()
    }
}
