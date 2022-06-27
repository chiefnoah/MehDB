use std::fs::{self, File};
use std::path::Path;
use std::io::{self, Seek};
//use std::collections::hash_map;
use std::collections::hash_map::Entry;
//use std::fs::File;

use serde::{Serialize, Deserialize};


const BUCKET_RECORDS: usize = 16;
const SEGMENT_BUCKETS: usize = 64;

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
    dir_file: File,
    segment_file: File,
}

const HEADER_SIZE: u64 = 16;

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

impl<K, V> Record<K, V> {
    fn pack(&self) -> [u8; 16] {
        // I don't like that we hae to allocate here
        let out: Vec<u8> = [self.hash_key.to_le_bytes(), self.offset.to_le_bytes()]
            .concat();
        out.try_into().unwrap()
    }
}

// Bucket 
struct Bucket<K, V> {
    records: [Record<K, V>; BUCKET_RECORDS]
}

fn initialize_segment(segment_file: &mut File, from_offset: Option<u64>) -> Result<(), io::Error> {
    segment_file.seek(io::SeekFrom::Start(HEADER_SIZE));
    for i in 0..SEGMENT_BUCKETS {
        for z in 0..BUCKET_RECORDS {
            r = Record{hash_key: 0, offset: HEADER_SIZE};
            segment_file.write(&r.pack())?:
        }
    }
}

impl MehDB {
    // New creates a new instance of MehDB with it's data in optional path.
    pub fn new(path: Option<&Path>) -> Result<Self, io::Error> {
        let path: &Path = match path {
            Some(path) => path,
            // Default to the current directory
            None => Path::new("."),
        };
        let dir_file_path = path.join("index.dir");
        let segment_file_path = path.join("index.bin");
        let segment_file = if !segment_file_path.exists() {
            let f = File::create(segment_file_path);
            
            f
        } else {
            File::open(segment_file_path)
        }?;
        let dir_file = if !dir_file_path.exists() {
            File::create(dir_file_path)
        } else {
            File::open(dir_file_path)
        }?;
        Ok(MehDB {
            header: Header{
                global_depth: 0,
                num_segments: 1,
            },
            dir_file: dir_file,
            segment_file: segment_file,
        })
    }

}

impl MehDB {

    fn split_segments<K, V>(&self, hash_key: u64, offset: u64, segment_depth: u8, v: V) {


    }

    fn grow_directory(&self) {

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
