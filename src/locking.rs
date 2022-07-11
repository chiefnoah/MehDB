use crate::serializer::Serializable;
use cache_padded::CachePadded;
use parking_lot::RwLock;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::default::Default;
use std::hash::{BuildHasher, Hasher};

pub struct StripedLock {
    locks: Vec<CachePadded<RwLock<()>>>,
}

impl StripedLock {
    fn init(capacity: usize) -> Self {
        let mut locks = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let padded_mutex = CachePadded::new(RwLock::new(()));
            locks.push(padded_mutex);
        }
        Self { locks }
    }

    // Retrieves a RwLock from the
    fn get(&self, key: &[u8]) -> Option<&RwLock<()>> {
        let mut hasher: DefaultHasher = Default::default();
        hasher.write(key);
        let hash_key = hasher.finish();
        let len = self.locks.len();
        let cached_lock = &self.locks.get(hash_key as usize % &len)?;
        Some(&cached_lock)
    }
}
