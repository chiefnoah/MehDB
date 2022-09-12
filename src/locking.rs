use crate::serializer::Serializable;
use anyhow::{anyhow, Context, Result};
use cache_padded::CachePadded;
use object_pool::{Pool, Reusable};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::cmp;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::default::Default;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;
use std::mem::{swap, replace};

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
        let lock = &self
            .locks
            .get(hash_key as usize % len)
            .context("Reading lock from striped lock.")
            .unwrap();
        &lock
    }
}

// Partially inspired by object-pool crate
// https://crates.io/crates/object-pool
pub struct RWFilePool {
    dummy: File,
    path: PathBuf,
    ro_files: Pool<File>,
    rw_files: Pool<File>,
    locks: StripedRWLock<()>,
}

impl<'a> RWFilePool {
    fn open_ro_file(path: &PathBuf) -> Result<File> {
        OpenOptions::new()
            .create(false)
            .truncate(false)
            .read(true)
            .write(false)
            .open(&path)
            .context("Opening ro file.")
    }

    fn open_rw_file(path: &PathBuf) -> Result<File> {
        OpenOptions::new()
            .create(false)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .context("Opening rw file.")
    }

    pub fn init(path: PathBuf, ro_capacity: usize, rw_capacity: usize) -> Result<Self> {
        let dummy = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(false)
            .open(&path)?;

        let ro_files = Pool::new(ro_capacity, || RWFilePool::open_ro_file(&path).unwrap());
        let rw_files = Pool::new(rw_capacity, || RWFilePool::open_rw_file(&path).unwrap());
        let locks_vec = vec![(); (1.5 * ro_capacity as f64).round() as usize];
        Ok(Self {
            dummy,
            path,
            ro_files,
            rw_files,
            locks: StripedRWLock::init(locks_vec),
        })
    }

    /// A thread-safe way to pull a read-only File-like proxy according to StripedRWLock semantics.
    pub fn ro_file(&'a self, i: u32) -> LockFileProxy<'a> {
        let k = &i.to_le_bytes()[..];
        let guard = self.locks.get(k).read();
        let file = self.ro_files.pull(|| {
            RWFilePool::open_ro_file(&self.path)
                .context("Opening new ro file because pool empty.")
                .unwrap()
        });
        LockFileProxy::<'a> {
            pool: self,
            guard: AnyRWLockGuard::RO(guard),
            file,
        }
    }

    /// A thread-safe way to pull a read-write File-like proxy according to StripedRWLock semantics.
    pub fn rw_file(&'a self, i: u32) -> LockFileProxy<'a> {
        let k = &i.to_le_bytes()[..];
        let guard = self.locks.get(k).write();
        let file = self.rw_files.pull(|| {
            RWFilePool::open_rw_file(&self.path)
                .context("Opening new rw file because pool empty.")
                .unwrap()
        });
        LockFileProxy {
            pool: self,
            guard: AnyRWLockGuard::RW(guard),
            file,
        }
    }

    /// A thread-safe way to pull a read-write File-like proxy according to StripedRWLock semantics.
    pub fn upgradeable_rw_file(&'a self, i: u32) -> LockFileProxy<'a> {
        let k = &i.to_le_bytes()[..];
        let guard = self.locks.get(k).upgradable_read();
        let file = self.ro_files.pull(|| {
            RWFilePool::open_ro_file(&self.path)
                .context("Opening ro file because pool empty in upgradable.")
                .unwrap()
        });
        LockFileProxy {
            pool: self,
            guard: AnyRWLockGuard::UPGRADABLE(guard),
            file,
        }
    }
}

enum AnyRWLockGuard<'a, T> {
    RO(RwLockReadGuard<'a, T>),
    RW(RwLockWriteGuard<'a, T>),
    UPGRADABLE(RwLockUpgradableReadGuard<'a, T>),
    EMPTY,
}
pub struct LockFileProxy<'a> {
    pool: &'a RWFilePool,
    guard: AnyRWLockGuard<'a, ()>,
    file: Reusable<'a, File>,
}

impl<'a> Deref for LockFileProxy<'a> {
    type Target = File;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl<'a> DerefMut for LockFileProxy<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl<'a> LockFileProxy<'a> {
    /// Upgrades the internal RwLockUpgradeableReadGuard to a RwLockWriteGuard, replacing the
    /// internal file handle and lock with the the equivalents with the correct permissions
    fn upgrade(&mut self) -> Result<()> {
        match &self.guard {
            AnyRWLockGuard::UPGRADABLE(g) => {
                // Replace the existing guard with a temporary value
                if let AnyRWLockGuard::UPGRADABLE(guard) = replace(&mut self.guard, AnyRWLockGuard::EMPTY) {
                    // Upgrade the internal lock
                    let upgraded = AnyRWLockGuard::RW(RwLockUpgradableReadGuard::upgrade(guard));
                    // Finally, store the upgraded lock in self
                    self.guard = upgraded;
                };
                // Replace the existing read-only file with one with write permissions
                // This should return the read-only file to the ro_pool
                // We default to opening a new file handle to gurantee we never get stuck
                // On an empty rw_pool
                self.file = self.pool.rw_files.pull(|| {
                    OpenOptions::new()
                        .create(false)
                        .truncate(false)
                        .read(true)
                        .write(true)
                        .open(&self.pool.path)
                        .expect("Unable to open new file handle.")
                });
            }
            _ => return Err(anyhow!("Attempting to upgrade non-upgradable lock.")),
        }
        Ok(())
    }
}
