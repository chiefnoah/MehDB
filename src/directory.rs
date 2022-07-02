use std::io;
use log::{info};
use parking_lot::RwLock;


pub trait Directory<C> {
    fn init(config: Option<C>) -> Self;
    fn segment_offset(&self, i: u64) -> io::Result<u64>;
    fn set_segment_offset(&self, i: u64, offset: u64) -> io::Result<()>;
    fn grow(&mut self) -> io::Result<u64>;
}

pub struct MemoryDirectory {
    dir: RwLock<Vec<u64>>,
}

pub struct MemoryDirectoryConfig {}

impl Directory<MemoryDirectoryConfig> for MemoryDirectory {

    fn init(config: Option<MemoryDirectoryConfig>) -> Self {
        info!("Initializing new MemoryDirectory");
        MemoryDirectory { dir: RwLock::new(Vec::with_capacity(1))}
    }

    fn segment_offset(&self, i: u64) -> io::Result<u64> {
        let dir = self.dir.read();
        let r = dir.get(i as usize);
        match r {
            Some(r) => Ok(*r),
            None => Err(io::Error::new(io::ErrorKind::InvalidInput, "Segment index out of bounds."))
        }
    }
    fn set_segment_offset(&self, i: u64, offset: u64) -> io::Result<()> {
        let mut dir = self.dir.write();
        dir[i as usize] = offset;
        Ok(())
    }

    fn grow(&mut self) -> io::Result<u64> {
        let mut dir = self.dir.write();
        let len = dir.len();
        let dir_copy = dir.clone();
        dir.resize(len * 2, 0);
        for (i, r) in dir_copy.iter().enumerate() {
            dir[i*2] = *r;
            dir[(i*2) + 1] = *r;
        }
        Ok(len as u64 * 2)
    }
}
