use std::path::Path;
use std::io;
//use std::collections::hash_map;
use std::collections::hash_map::Entry;
//use std::fs::File;

use serde::{Serialize, Deserialize};


pub trait Map<K, V> {
    // get_bytes maybe returns the value associated with the key.
    fn get_bytes(&self, key: &[u8]) -> Entry<&[u8], &[u8]>;
    // put_bytes inserts a value associated with a key and returns it's
    // relative offset in the data file.
    fn put_bytes(&self, key: &[u8], value: &[u8]) -> Result<u64, io::Error>;
    
    fn put(&self, key: K, value: V) -> Result<u64, io::Error>;
    fn get(&self, key: K) -> Entry<K, V>;
}

// My Extendible Hash Database
pub struct MehDB {
    header: Header,
}

#[derive(Serialize, Deserialize)]
struct Header {
    global_depth: u64,
    num_segments: u64,
}

struct Record<K, V> {
    hash_key: u64,
    key: K,
    value: V,
    offset: u64
}

// Bucket 
struct Bucket {
    records: [Record, 16]
}

impl MehDB {
    pub fn new(path: Option<&Path>) -> Self {
        let path: &Path = match path {
            Some(path) => path,
            // Default to the current directory
            None => Path::new("."),
        };
        MehDB {
            header: Header{
                global_depth: 0,
                num_segments: 1,
            }
        }
    }
}

impl<K, V> Map<K, V> for MehDB {

    fn put(&self, key: K, value: V) -> Result<u64, io::Error> {
        panic!("Not implemented");
    }
    fn get(&self, key: K) -> Entry<K, V> {
        panic!("Not implemented");
    }

    fn get_bytes(&self, key: &[u8]) -> Entry<&[u8], &[u8]> {
        panic!("Not implemented");
    }
    // put_bytes inserts a value associated with a key and returns it's
    // relative offset in the data file.
    fn put_bytes(&self, key: &[u8], value: &[u8]) -> Result<u64, io::Error> {
        panic!("Not implemented");
    }
}
