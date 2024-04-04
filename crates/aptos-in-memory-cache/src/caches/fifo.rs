// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{Cache, Incrementable, Ordered};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::{hash::Hash, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::info;

const IN_MEMORY_CACHE_GC_INTERVAL_MS: u64 = 100;

#[derive(Debug, Clone, Copy)]
struct CacheMetadata<K> {
    eviction_trigger_size_in_bytes: u64,
    target_size_in_bytes: u64,
    total_size_in_bytes: u64,
    last_key: Option<K>,
    first_key: Option<K>,
}

/// A simple in-memory cache with a deterministic FIFO eviction policy.
pub struct FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<V> + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    /// Cache maps the cache key to the deserialized Transaction.
    items: DashMap<K, V>,
    cache_metadata: Arc<RwLock<CacheMetadata<K>>>,
    _cancellation_token_drop_guard: tokio_util::sync::DropGuard,
}

impl<K, V> FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<V> + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    pub fn new(target_size_in_bytes: u64, eviction_trigger_size_in_bytes: u64) -> Self {
        let cancellation_token = CancellationToken::new();
        let items = DashMap::new();
        let cache_metadata = Arc::new(RwLock::new(CacheMetadata {
            eviction_trigger_size_in_bytes,
            target_size_in_bytes,
            total_size_in_bytes: 0,
            last_key: None,
            first_key: None,
        }));

        Self::spawn_cleanup_task(
            items.clone(),
            cache_metadata.clone(),
            cancellation_token.clone(),
        );

        Self {
            items,
            cache_metadata,
            _cancellation_token_drop_guard: cancellation_token.drop_guard(),
        }
    }

    /// Perform cache eviction on a separate task.
    fn spawn_cleanup_task(
        items: DashMap<K, V>,
        cache_metadata: Arc<RwLock<CacheMetadata<K>>>,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) {
        tokio::spawn(async move {
            loop {
                if cancellation_token.is_cancelled() {
                    info!("In-memory cache cleanup task is cancelled.");
                    return;
                }

                // Check if we should evict items from the cache.
                let should_evict = {
                    let current_cache_metadata = cache_metadata.read();
                    current_cache_metadata
                        .total_size_in_bytes
                        .saturating_sub(current_cache_metadata.eviction_trigger_size_in_bytes)
                        > 0
                };

                // If we don't need to evict, sleep for 100 ms.
                if !should_evict {
                    tokio::time::sleep(Duration::from_millis(IN_MEMORY_CACHE_GC_INTERVAL_MS)).await;
                    continue;
                }

                // Evict items from the cache.
                let mut current_cache_metadata = cache_metadata.write();
                let mut actual_bytes_removed = 0;
                let mut bytes_to_remove = current_cache_metadata
                    .total_size_in_bytes
                    .saturating_sub(current_cache_metadata.target_size_in_bytes);
                while bytes_to_remove > 0 {
                    if let Some(key_to_remove) = current_cache_metadata.first_key.clone() {
                        let (_k, v) = items
                            .remove(&key_to_remove)
                            .expect("Failed to remove the key");
                        let size_of_v = std::mem::size_of_val(&v) as u64;
                        bytes_to_remove = bytes_to_remove.saturating_sub(size_of_v);
                        actual_bytes_removed += size_of_v;
                        current_cache_metadata.first_key = Some(key_to_remove.next(&v));
                    } else {
                        break;
                    }
                }

                current_cache_metadata.total_size_in_bytes -= actual_bytes_removed;
            }
        });
    }
}

impl<K, V> Cache<K, V> for FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<V> + Send + Sync + Clone,
    V: Send + Sync + Clone,
{
    fn get(&self, key: &K) -> Option<V> {
        self.items.get(key).map(|v| v.value().clone())
    }

    fn insert(&self, key: K, value: V) {
        // If cache is empty, set the first to the new key.
        if self.items.is_empty() {
            let mut cache_metadata = self.cache_metadata.write();
            cache_metadata.first_key = Some(key.clone());
        }

        let mut cache_metadata = self.cache_metadata.write();
        cache_metadata.last_key = Some(key.clone());
        cache_metadata.total_size_in_bytes += std::mem::size_of_val(&value) as u64;
        self.items.insert(key, value);
    }

    fn total_size(&self) -> u64 {
        let cache_metadata = self.cache_metadata.read();
        cache_metadata.total_size_in_bytes
    }
}

impl<K, V> Ordered<K> for FIFOCache<K, V>
where
    K: Hash + Eq + PartialEq + Incrementable<V> + Send + Sync + Clone,
    V: Send + Sync + Clone,
{
    fn first_key(&self) -> Option<K> {
        let cache_metadata = self.cache_metadata.read();
        cache_metadata.first_key.clone()
    }

    fn last_key(&self) -> Option<K> {
        let cache_metadata = self.cache_metadata.read();
        cache_metadata.last_key.clone()
    }
}
