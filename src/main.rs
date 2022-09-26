#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
extern crate pretty_env_logger;

pub mod directory;
mod locking;
pub mod meh;
pub mod segment;
pub mod serializer;

use std::sync::Arc;
use std::sync::Mutex;
use std::thread::{spawn, JoinHandle};
use std::time::Instant;

use crate::directory::{Directory, MMapDirectory};
use crate::locking::StripedLock;
use crate::meh::MehDB;
use crate::segment::ThreadSafeFileSegmenter;
use crate::serializer::{ByteKey, ByteValue};

use anyhow::{Context, Result};
use highway::{self, HighwayHash, HighwayHasher};
use log::info;

fn main() -> Result<()> {
    pretty_env_logger::init();
    let segmenter = ThreadSafeFileSegmenter::init("./segment.bin".into())?;
    let directory = MMapDirectory::init("./directory.bin".into())?;
    let lock = StripedLock::init(32);
    let mut mehdb = MehDB {
        hasher_key: highway::Key([53252, 2352323, 563956259, 234832]),
        directory: Arc::new(directory),
        segmenter: segmenter.clone(),
        lock: Arc::new(lock),
        segment_file_lock: Arc::new(Mutex::new(())),
    };
    let mut threads: Vec<JoinHandle<()>> = Vec::with_capacity(4);
    const RECORDS: usize = 10_000_000;
    const THREADS: usize = 4;
    let start_time = Instant::now();
    for _ in 0..THREADS {
        let mut db = mehdb.clone();
        threads.push(spawn(move || {
            for i in 0..RECORDS / THREADS {
                info!("i: {}", i);
                let i = i as u64;
                let key = ByteKey(i.to_le_bytes().to_vec());
                let value = ByteValue(i * 2);
                db.put(key, value)
                    .context("Error inserting record")
                    .expect("Unable to insert record!");
            }
            for i in 0..RECORDS / THREADS {
                let i = i as u64;
                let key = ByteKey(i.to_le_bytes().to_vec());
                let r = db.get(key).expect(&format!("Missing record for {}", i));
                info!("k: {} v: {:?}", i, r);
            }
        }));
    }
    for thread in threads.into_iter() {
        thread.join().expect("Thread paniced...");
    }
    let end_time = start_time.elapsed().as_secs_f64();
    println!(
        "Elapsed time: {:.2?}\nAvg inserts/us: {:.2}",
        end_time,
        RECORDS as f64 / end_time
    );
    // Verify single-threaded that this all works...
    for i in 0..RECORDS {
        info!("i: {}", i);
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let get_key = key.clone();
        let value = ByteValue(i * 2);
        mehdb
            .put(key, value)
            .context("Error inserting record")
            .expect("Unable to insert record!");
        mehdb
            .get(get_key)
            .with_context(|| format!("Key 0 missing at i {}", i))
            .unwrap();
    }
    let end_time = start_time.elapsed().as_secs_f64();
    println!(
        "Elapsed time: {:.2?}\nAvg inserts/us: {:.2}",
        end_time,
        RECORDS as f64 / end_time
    );
    info!("Done putting, trying to read now...");
    let start_time = Instant::now();
    for i in 0..RECORDS {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let r = mehdb.get(key).expect(&format!("Missing record for {}", i));
        info!("k: {} v: {:?}", i, r);
    }
    let end_time = start_time.elapsed().as_secs_f64();
    println!(
        "Elapsed time: {:.2?}\nAvg gets/s: {:.2}",
        end_time,
        RECORDS as f64 / end_time
    );
    Ok(())
}
