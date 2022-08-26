use log::info;
use parking_lot::RwLock;
use std::io;

pub trait Directory<C> {
    fn segment_index(&self, i: u64) -> io::Result<u32>;
    fn set_segment_index(&self, i: u64, index: u32) -> io::Result<()>;
    fn grow(&mut self) -> io::Result<u64>;
    fn global_depth(&mut self) -> io::Result<u32>;
}

pub struct MemoryDirectory {
    // dir contains the directory and the global depth in a tuple
    dir: RwLock<(Vec<u32>, u32)>,
    global_depth: u32,
}

pub struct MemoryDirectoryConfig {}

impl MemoryDirectory {
    pub fn init(config: Option<MemoryDirectoryConfig>, initial_index: u32) -> Self {
        info!("Initializing new MemoryDirectory");
        let mut dir: Vec<u32> = Vec::with_capacity(1);
        dir.push(initial_index);
        MemoryDirectory {
            dir: RwLock::new((dir, 0)),
            global_depth: 0
        }
    }
}

impl Directory<MemoryDirectoryConfig> for MemoryDirectory {
    fn segment_index(&self, i: u64) -> io::Result<u32> {
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
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Segment index out of bounds.",
            )),
        }
    }
    fn set_segment_index(&self, i: u64, index: u32) -> io::Result<()> {
        let mut dir = self.dir.write();
        dir.0[i as usize] = index;
        Ok(())
    }

    fn grow(&mut self) -> io::Result<u64> {
        let mut dir = self.dir.write();
        let len = dir.0.len();
        let dir_copy = dir.0.clone();
        dir.0.resize(len * 2, 0);
        for (i, r) in dir_copy.iter().enumerate() {
            dir.0[i * 2] = *r;
            dir.0[(i * 2) + 1] = *r;
        }
        dir.1 += 1;
        Ok(len as u64 * 2)
    }

    fn global_depth(&mut self) -> io::Result<u32> {
        Ok(self.global_depth)
    }

}

