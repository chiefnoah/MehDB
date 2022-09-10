use crate::serializer::Serializable;
use cache_padded::CachePadded;
use parking_lot::RwLock;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::default::Default;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use anyhow::{Result, Context};

pub struct StripedRWLock<T> {
    locks: Arc<Vec<CachePadded<RwLock<T>>>>,
}

/// Striped lock implements a memory-efficient RWLock over a set of limited resources that
/// represent a subset of the total possible locked values. That is, if you have N resources, all
/// of which could be locked independently but would be inefficient or impossible to have a
/// syncronization primative over all of them in-memory.
/// It's recommended that you have at least 1.5x locks for each thread expected to request a lock
/// simultaneously.

impl<T> StripedRWLock<T> {
    pub fn init(mut locked_resources: Vec<T>) -> Self {
        let capacity = locked_resources.len();
        let mut locks = Vec::with_capacity(capacity);
        for r in locked_resources.drain(..) {
            let padded_mutex = CachePadded::new(RwLock::new(r));
            locks.push(padded_mutex);
        }
        Self {
            locks: Arc::new(locks),
        }
    }

    // Retrieves a RwLock from the
    pub fn get(&self, key: &[u8]) -> &RwLock<T> {
        let mut hasher: DefaultHasher = Default::default();
        hasher.write(key);
        let hash_key = hasher.finish();
        let len = self.locks.len();
        let lock = &self.locks.get(hash_key as usize % len)
            .context("Reading lock from striped lock.").unwrap();
        &lock
    }
}

// A `StripedRWLock` is thread safe as it has a read-only hashmap wrapping a Send primative (RWLock).
//unsafe impl<T> Send for StripedRWLock<T> {}
