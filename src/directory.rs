use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, trace, warn};
use memmap::{Mmap, MmapMut};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use tempfile::NamedTempFile;

pub trait Directory<T: Sized = Self>: Sized {
    type Config;
    fn init(config: Self::Config) -> Result<Self>;
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

    fn init(initial_index: Self::Config) -> Result<Self> {
        info!("Initializing new MemoryDirectory");
        let mut dir: Vec<u32> = Vec::with_capacity(1);
        dir.push(initial_index);
        Ok(MemoryDirectory {
            dir: RwLock::new((dir, 0)),
        })
    }

    fn segment_index(&self, i: u64) -> Result<u32> {
        let unlocked = self.dir.read();
        let dir: &Vec<u32> = &unlocked.0;
        let global_depth = unlocked.1;
        // TODO: just bounds check, we shouldn't take in an un global_depth normalized key here
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
        info!("Growing directory. New size: {}", len * 2);
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
    config: MMapDirectoryConfig,
}

pub struct MMapDirectoryConfig {
    pub path: PathBuf,
}

impl Directory for MMapDirectory {
    type Config = MMapDirectoryConfig;
    fn init(config: Self::Config) -> Result<Self> {
        // TODO: better error handling
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false) // Don't clear the file, we need it!
            .create(true)
            .open(&config.path)
            .context("Opening up mmap file")
            .expect("Unable to initialize mmap file.");
        if file.metadata().unwrap().len() == 0 {
            file.set_len(5)
                .context("Setting initial mmap file size to 5")?;
        }
        // mmaps are unsafe!
        let map = unsafe { MmapMut::map_mut(&file).context("Initializing mmap")? };
        // If the mmap is empty or new, make it writeable, populate the global_
        let global_depth = map[0];
        Ok(Self {
            map: RwLock::new(map),
            config,
        })
    }

    fn segment_index(&self, i: u64) -> Result<u32> {
        let unlocked = self.map.read();
        let global_depth = unlocked[0];
        let index = if global_depth == 0 {
            // Lazy way to get out of overflowing bitshift
            0
        } else {
            i >> 64 - global_depth
        };
        debug!("Retrieving segment index from dir index: {}", index);
        let offset = ((index * 4) + 1) as usize;
        match unlocked.get(offset..offset + 4) {
            Some(i) => {
                let mut buf: [u8; 4] = [0; 4];
                buf.copy_from_slice(i);
                Ok(u32::from_le_bytes(buf))
            }
            None => Err(anyhow!(
                "Unable to find segment index in directory at location {}",
                index
            )),
        }
    }

    fn set_segment_index(&self, i: u64, index: u32) -> Result<()> {
        let mut unlocked = self.map.write();
        // We have a RW lock now
        let offset = ((i * 4) + 1) as usize;
        info!("Setting dir index {} to segment index {}", i, index);
        unlocked[offset..offset + 4].copy_from_slice(&index.to_le_bytes()[..]);
        unlocked.flush().context("Flushing directory file")?;
        Ok(())
    }

    fn grow(&self) -> Result<u32> {
        let unlocked = self.map.upgradable_read();
        // Create a temporary file, we'll fill this with the contents of the current map, but
        // duplicated per the rules of a MSP extendible hashing directory
        let mut temporary_file = NamedTempFile::new()?;
        let f = temporary_file.as_file_mut();
        let global_depth = unlocked[0];
        warn!(
            "Increase global_depth from {} to {}",
            global_depth,
            global_depth + 1
        );
        debug!(
            "Old # of dir entries {} increasing to {}",
            1 << global_depth,
            1 << (global_depth + 1)
        );
        f.write(&[global_depth + 1])?;
        for i in 0..1 << global_depth {
            let offset = ((i * 4) + 1) as usize;
            trace!("Reading old map at offsets[{}:{}]", offset, offset + 4);
            let data = unlocked
                .get(offset..offset + 4)
                .expect("Somehow mmap file is smaller than expected, or this is a bug");
            // Write exactly twice
            f.write(data)?;
            f.write(data)?;
        }
        f.flush().context("Flushing new dir tempfile")?;
        // We're done preparing our file, tine to upgrade our handle on the memorymap, copy over
        // the new file and re-load the mmap reference
        let f = temporary_file
            .persist(&self.config.path)
            .context("Copying over new grown directory file over existing filepath.")?;
        let new_map = unsafe { MmapMut::map_mut(&f)? };
        let mut unlocked = RwLockUpgradableReadGuard::upgrade(unlocked);
        *unlocked = new_map;
        Ok(0)
    }

    fn global_depth(&self) -> Result<u8> {
        let unlocked = self.map.read();
        match unlocked.get(0) {
            None => Err(anyhow!("Unable to read global depth from mmap file.")),
            Some(g) => Ok(*g),
        }
    }
}
