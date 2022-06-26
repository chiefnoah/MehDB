use log::{info, warn}
use std::path::Path;
use std::io;
use std::collections::hash_map;
use std::fs::File;

use serde::{Serialze, Deserialize};


pub trait Map<K, V> {
    // get_bytes maybe returns the value associated with the key.
    fn get_bytes(&self, key: K) -> Entry<V>;
    // put_bytes inserts a value associated with a key and returns it's
    // relative offset in the data file.
    fn put_bytes(&self, key: K, value: V) -> Result<u64, io::Error>;
}

// My Extendible Hash Database
pub struct MehDB<K, V> {
    header: Header,
}

#[derive(Seiralize, Deserialize)]
struct Header {
    global_depth: u64,
    num_segments: u64,
}

impl MehDB<K, V> {
    fn new(path: Option<Path>) -> Self {
        let path: Path = match path {
            Some(path) => path,
            // Default to the current directory
            None => Path::new("."),
        };
    }
}
