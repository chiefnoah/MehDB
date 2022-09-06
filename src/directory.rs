use log::{info, warn};
use parking_lot::RwLock;
use std::io;
use std::fs::{File, OpenOptions};
use memmap::{Mmap, MmapMut};


pub trait Directory {
    type Config;
    fn init(config: Self::Config) -> Self;
    fn segment_index(&self, i: u64) -> io::Result<u64>;
    fn set_segment_index(&self, i: u64, index: u64) -> io::Result<()>;
    fn grow(&mut self) -> io::Result<u64>;
    fn global_depth(&mut self) -> io::Result<u32>;
}

pub struct MemoryDirectory {
    // dir contains the directory and the global depth in a tuple
    dir: RwLock<(Vec<u64>, u32)>,
}


impl Directory for MemoryDirectory {
    type Config = u64;

    fn init(initial_index: Self::Config) -> Self {
        info!("Initializing new MemoryDirectory");
        let mut dir: Vec<u64> = Vec::with_capacity(1);
        dir.push(initial_index);
        MemoryDirectory {
            dir: RwLock::new((dir, 0)),
        }
    }

    fn segment_index(&self, i: u64) -> io::Result<u64> {
        let unlocked = self.dir.read();
        let dir: &Vec<u64> = &unlocked.0;
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
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Segment index out of bounds.",
            )),
        }
    }
    fn set_segment_index(&self, i: u64, index: u64) -> io::Result<()> {
        let mut dir = self.dir.write();
        dir.0[i as usize] = index;
        Ok(())
    }

    fn grow(&mut self) -> io::Result<u64> {
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
        Ok(len as u64 * 2)
    }

    fn global_depth(&mut self) -> io::Result<u32> {
        // Get a read-only handle on the directory
        let dir = self.dir.read();
        Ok(dir.1)
    }

}

pub struct MMapDirectory {
    map: Mmap,
}

impl MemoryDirectory {
    pub fn init(path: Option<Path>) -> Self {

    }
}
