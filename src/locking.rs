use crate::serializer::Serializable;
use anyhow::{anyhow, Context, Result};
use crossbeam::utils::CachePadded;
use parking_lot::RwLock;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::default::Default;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

pub struct StripedLock<T> {
    locks: Vec<CachePadded<RwLock<T>>>,
}

impl<T: Default> StripedLock<T> {
    pub fn init(capacity: usize) -> Self {
        let mut locks = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let padded_mutex = CachePadded::new(RwLock::new(Default::default()));
            locks.push(padded_mutex);
        }
        Self { locks }
    }

    // Retrieves a RwLock from the
    pub fn get(&self, key: u32) -> &RwLock<T> {
        let i = key as usize % self.locks.len();
        let cached_lock = &self
            .locks
            .get(i)
            .with_context(|| {
                format!(
                    "Attemptint to get locked value for key {} at index {}",
                    key, i
                )
            })
            .unwrap(); // This shouldn't ever be possible
        &cached_lock
    }
}

pub struct SegmentNode {
    bucket_locks: [RwLock<()>; 64],
}

impl SegmentNode {
    pub fn get_bucket_lock(&self, bucket_index: u32) -> Result<&RwLock<()>> {
        if bucket_index > 64 {
            return Err(anyhow!("Somehow got number larger than 64. This is a bug."));
        }
        return Ok(&self.bucket_locks[bucket_index as usize]);
    }
}

impl Default for SegmentNode {
    fn default() -> Self {
        let mut locks = Vec::with_capacity(64);
        for _ in 0..64 {
            locks.push(RwLock::new(()));
        }
        Self {
            bucket_locks: locks.try_into().unwrap_or_else(|v| {
                panic!("Unable to cast vec of locks to array when initializing a new SegmentNode.")
            }),
        }
    }
}
