// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{Cache, Ordered};
use quick_cache::{sync::Cache as S3FIFOCache, Lifecycle, Weighter};
use std::hash::{BuildHasher, Hash};

impl<K, V, We, B, L> Cache<K, V> for S3FIFOCache<K, V, We, B, L>
where
    K: Eq + Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
    We: Weighter<K, V> + Clone + Send + Sync,
    B: BuildHasher + Clone + Send + Sync,
    L: Lifecycle<K, V> + Clone + Send + Sync,
{
    fn get(&self, key: &K) -> Option<V> {
        S3FIFOCache::get(self, key)
    }

    fn insert(&mut self, key: K, value: V) -> (u64, u64) {
        S3FIFOCache::insert(self, key, value);
        (0, 0)
    }
}

impl<K, V, We, B, L> Ordered<K> for S3FIFOCache<K, V, We, B, L>
where
    K: Eq + Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
    We: Weighter<K, V> + Clone + Send + Sync,
    B: BuildHasher + Clone + Send + Sync,
    L: Lifecycle<K, V> + Clone + Send + Sync,
{
    fn first_key(&self) -> Option<K> {
        unimplemented!();
    }

    fn last_key(&self) -> Option<K> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_cache::sync::Cache as S3FIFOCache;

    fn get_s3fifo_cache() -> S3FIFOCache<i32, i32> {
        S3FIFOCache::<i32, i32>::new(10)
    }

    #[test]
    fn test_s3fifo_cache() {
        let mut cache: Box<dyn Cache<i32, i32>> = Box::new(get_s3fifo_cache());
        cache.insert(1, 1);
        assert_eq!(cache.get(&1), Some(1));
        assert_eq!(cache.get(&2), None);
    }
}
