// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use std::{
    sync::atomic::{AtomicU64, Ordering},
    vec::Vec,
};

pub struct AtomicBitmap {
    segments: Vec<AtomicU64>,
}

impl AtomicBitmap {
    pub fn new(size_in_bits: usize) -> Self {
        let segment_count = (size_in_bits + 63) / 64; // Round up to cover all bits
        let segments = (0..segment_count).map(|_| AtomicU64::new(0)).collect();
        AtomicBitmap { segments }
    }

    pub fn set_bit(&self, bit_index: usize) {
        let segment_index = bit_index / 64;
        let bit_position = bit_index % 64;
        let mask = 1u64 << bit_position;
        self.segments[segment_index].fetch_or(mask, Ordering::SeqCst);
    }

    pub fn clear_bit(&self, bit_index: usize) {
        let segment_index = bit_index / 64;
        let bit_position = bit_index % 64;
        let mask = !(1u64 << bit_position);
        self.segments[segment_index].fetch_and(mask, Ordering::SeqCst);
    }

    pub fn try_set_bit(&self, bit_index: usize) -> bool {
        let array_index = bit_index / 64;
        let bit_position = bit_index % 64;
        let mask = 1u64 << bit_position;

        let current_value = self.segments[array_index].load(Ordering::Acquire);
        if current_value & mask == 0 {
            // The bit is not set, try to set it.
            let new_value = current_value | mask;
            // Only set the bit if the current value hasn't changed since we last checked.
            self.segments[array_index]
                .compare_exchange(
                    current_value,
                    new_value,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
        } else {
            // The bit was already set.
            false
        }
    }
}

#[test]
fn test_64m_bitmap() {
    use std::sync::Arc;
    let bitmap = Arc::new(AtomicBitmap::new(64_000_000));
    // Example usage
    bitmap.set_bit(1_000_000);
    bitmap.clear_bit(1_000_000);
}
