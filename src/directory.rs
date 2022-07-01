use std::io;
use log::{info};


pub trait Directory<C> {
    fn init(config: Option<C>) -> Self;
    fn segment_offset(&self, i: u64) -> Result<u64, io::Error>;
    fn set_segment_offset(&self, i: u64, offset: u64) -> Result<u64, io::Error>;
}

pub struct MemoryDirectory {
    dir: Vec<u64>,
}

pub struct MemoryDirectoryConfig {}

impl Directory<MemoryDirectoryConfig> for MemoryDirectory {

    fn init(config: Option<MemoryDirectoryConfig>) -> Self {
        info!("Initializing new MemoryDirectory");
        MemoryDirectory { dir: Vec::with_capacity(4) }
    }

    fn segment_offset(&self, i: u64) -> Result<u64, io::Error> {
        todo!("Implement this");
    }
    fn set_segment_offset(&self, i: u64, offset: u64) -> Result<u64, io::Error> {
        todo!("Implement this");
    }

}
