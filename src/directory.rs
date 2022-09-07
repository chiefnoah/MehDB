use anyhow::{anyhow, Context, Result};
use log::{info, warn};
use memmap::{Mmap, MmapMut};
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub trait Directory<T: Sized = Self> {
    type Config;
    fn init(config: Self::Config) -> Self;
    fn segment_index(&self, i: u64) -> Result<u32>;
    fn set_segment_index(&self, i: u64, index: u32) -> Result<()>;
    /// Doubles the size of the directory and returns the new size (not the global_depth)
    fn grow(&self) -> Result<u32>;
    fn global_depth(&self) -> Result<u8>;
}

pub struct MemoryDirectory {
    // dir contains the directory and the global depth in a tuple
    dir: RwLock<(Vec<u32>, u8)>,
}

impl Directory for MemoryDirectory {
    type Config = u32;

    fn init(initial_index: Self::Config) -> Self {
        info!("Initializing new MemoryDirectory");
        let mut dir: Vec<u32> = Vec::with_capacity(1);
        dir.push(initial_index);
        MemoryDirectory {
            dir: RwLock::new((dir, 0)),
        }
    }

    fn segment_index(&self, i: u64) -> Result<u32> {
        let unlocked = self.dir.read();
        let dir: &Vec<u32> = &unlocked.0;
        let global_depth = unlocked.1;
        let index = if global_depth == 0 {
            // Lazy way to get out of overflowing bitshift
            0
        } else {
            i >> 64 - global_depth
        };
        info!("Index for {}: {}", i, index);
        let r = dir.get(index as usize);
        match r {
            Some(r) => Ok(*r),
            None => Err(anyhow!("Segment index out of bounds: {}", i)),
        }
    }
    fn set_segment_index(&self, i: u64, index: u32) -> Result<()> {
        let mut dir = self.dir.write();
        dir.0[i as usize] = index;
        Ok(())
    }

    fn grow(&self) -> Result<u32> {
        let mut dir = self.dir.write();
        let len = dir.0.len();
        warn!("Growing directory. New size: {}", len * 2);
        // Copy so we can use `iter()` later
        // You shouldn't do this if we're backing to disk
        let dir_copy = dir.0.clone();
        dir.0.resize(len * 2, 0);
        for (i, r) in dir_copy.iter().enumerate() {
            dir.0[i * 2] = *r;
            dir.0[(i * 2) + 1] = *r;
        }
        (*dir).1 += 1;
        Ok(len as u32 * 2)
    }

    fn global_depth(&self) -> Result<u8> {
        // Get a read-only handle on the directory
        let dir = self.dir.read();
        Ok(dir.1)
    }
}

pub struct MMapDirectory {
    // We do not actually use the mutable nature of RwLock, just
    // as a guard to turn Mmap into a MmapMut
    map: RwLock<MmapMut>,
}

pub struct MMapDirectoryConfig {
    path: Box<Path>,
}

impl Directory for MMapDirectory {
    type Config = MMapDirectoryConfig;
    fn init(config: Self::Config) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false) // Don't clear the file, we need it!
            .create(true)
            .open(&config.path)
            .context("Opening up mmap file")
            .expect("Unable to initialize mmap file.");
        // mmaps are unsafe!
        let mut map = unsafe { MmapMut::map_mut(&file).expect("Unable to initialize mmap") };
        // If the mmap is empty or new, make it writeable, populate the global_
        if map.len() < 1 {
            // Global depth is 0
            map[0] = 0;
            // The 1 entry is segment index 0
            map[1..5].copy_from_slice(&[0; 4][..]);
        }
        Self {
            map: RwLock::new(map),
        }
    }

    fn segment_index(&self, i: u64) -> Result<u32> {
        let unlocked = self.map.read();
        let offset = ((i * 4) + 1) as usize;
        match unlocked.get(offset..offset + 4) {
            Some(i) => {
                Ok(u32::from_be_bytes(i.try_into()?))
            },
            None => Err(anyhow!("Unable to find segment index in directory at location {}", i))
        }
    }

    fn set_segment_index(&self, i: u64, index: u32) -> Result<()> {
        let mut unlocked = self.map.write();
        // We have a RW lock now
        let offset = ((i * 4) + 1) as usize;
        unlocked[offset..offset + 4].copy_from_slice(&index.to_le_bytes()[..]);
        Ok(())
    }

    fn grow(&self) -> Result<u32> {
        let mut unlocked = self.map.upgradable_read();
        todo!("Finish implementing this");
    }

    fn global_depth(&self) -> Result<u8> {
        todo!("Implement this");
    }
}
