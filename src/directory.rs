use anyhow::{anyhow, Context, Result};
use crossbeam::sync::{ShardedLock, ShardedLockWriteGuard};
use log::{debug, info, trace};
use memmap::MmapMut;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use tempfile::NamedTempFile;

pub trait Directory<T: Sized = Self>: Sized {
    type Config;
    fn init(config: Self::Config) -> Result<Self>;
    fn segment_index(&self, i: u64) -> Result<u32>;
    fn set_segment_index(&self, i: u64, index: u32, global_depth: &mut GlobalDepth) -> Result<()>;
    /// Doubles the size of the directory and returns the new size (not the global_depth)
    fn grow(&self) -> Result<u32>;
    fn global_depth(&self) -> Result<GlobalDepth>;
    fn grow_if_eq(&self, local_depth: u8) -> Result<u8>;
}

pub struct MMapDirectory {
    // We do not actually use the mutable nature of RwLock, just
    // as a guard to turn Mmap into a MmapMut
    map: ShardedLock<MmapMut>,
    config: PathBuf, // We only care about the path for now
}

pub struct GlobalDepth<'a> {
    global_depth: u8,
    lock: ShardedLockWriteGuard<'a, MmapMut>,
}

impl Directory for MMapDirectory {
    type Config = PathBuf;
    fn init(config: Self::Config) -> Result<Self> {
        // TODO: better error handling
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false) // Don't clear the file, we need it!
            .create(true)
            .open(&config)
            .context("Opening up mmap file")
            .expect("Unable to initialize mmap file.");
        if file.metadata().unwrap().len() == 0 {
            file.set_len(5)
                .context("Setting initial mmap file size to 5")?;
        }
        // mmaps are unsafe!
        let map = unsafe { MmapMut::map_mut(&file).context("Initializing mmap")? };
        // If the mmap is empty or new, make it writeable, populate the global_
        Ok(Self {
            map: ShardedLock::new(map),
            config,
        })
    }

    fn segment_index(&self, i: u64) -> Result<u32> {
        let unlocked = match self.map.read() {
            Err(_) => return Err(anyhow!("Directory lock is probably poisoned.")),
            Ok(l) => l,
        };
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

    fn set_segment_index(&self, i: u64, index: u32, gd: &mut GlobalDepth) -> Result<()> {
        // We have a RW lock now
        let offset = ((i * 4) + 1) as usize;
        info!("Setting dir index {} to segment index {}", i, index);
        gd.lock[offset..offset + 4].copy_from_slice(&index.to_le_bytes()[..]);
        gd.lock.flush().context("Flushing directory file")?;
        Ok(())
    }

    fn grow(&self) -> Result<u32> {
        let unlocked = match self.map.read() {
            Err(_) => return Err(anyhow!("Directory lock is probably poisoned.")),
            Ok(l) => l,
        };
        // Create a temporary file, we'll fill this with the contents of the current map, but
        // duplicated per the rules of a MSP extendible hashing directory
        let mut dir_path = self.config.clone();
        dir_path.pop();
        let mut temporary_file = NamedTempFile::new_in(dir_path)?;
        let f = temporary_file.as_file_mut();
        let global_depth = unlocked[0];
        info!(
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
            .persist(&self.config)
            .context("Copying over new grown directory file over existing filepath.")?;
        let new_map = unsafe { MmapMut::map_mut(&f)? };
        drop(unlocked);
        let mut unlocked = match self.map.write() {
            Err(_) => return Err(anyhow!("Directory lock is probably poisoned.")),
            Ok(l) => l,
        };
        *unlocked = new_map;
        Ok(0)
    }

    fn global_depth(&self) -> Result<GlobalDepth> {
        let unlocked = match self.map.write() {
            Err(_) => return Err(anyhow!("Directory lock is probably poisoned.")),
            Ok(l) => l,
        };
        match unlocked.get(0) {
            None => Err(anyhow!("Unable to read global depth from mmap file.")),
            Some(g) => Ok(GlobalDepth {
                global_depth: *g,
                lock: unlocked,
            }),
        }
    }

    fn grow_if_eq(&self, local_depth: u8) -> Result<u8> {
        let mut unlocked = match self.map.write() {
            Err(_) => return Err(anyhow!("Directory lock is probably poisoned.")),
            Ok(l) => l,
        };
        let global_depth = match unlocked.get(0) {
            None => return Err(anyhow!("Unable to read global depth from mmap file.")),
            Some(g) => {
                if *g > local_depth {
                    return Ok(*g);
                }
                *g
            }
        };
        // Create a temporary file, we'll fill this with the contents of the current map, but
        // duplicated per the rules of a MSP extendible hashing directory
        let mut dir_path = self.config.clone();
        dir_path.pop();
        let mut temporary_file = NamedTempFile::new_in(dir_path)?;
        let f = temporary_file.as_file_mut();
        info!(
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
            .persist(&self.config)
            .context("Copying over new grown directory file over existing filepath.")?;
        let new_map = unsafe { MmapMut::map_mut(&f)? };
        *unlocked = new_map;
        Ok(global_depth + 1)
    }
}

impl Deref for GlobalDepth<'_> {
    type Target = u8;
    fn deref(&self) -> &Self::Target {
        &self.global_depth
    }
}
