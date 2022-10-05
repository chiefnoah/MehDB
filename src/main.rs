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
use std::thread::{spawn, JoinHandle};
use std::time::Instant;

use crate::directory::{Directory, MMapDirectory};
use crate::locking::StripedLock;
use crate::meh::MehDB;
use crate::segment::ThreadSafeFileSegmenter;
use crate::serializer::{ByteKey, ByteValue};

use anyhow::{Context, Result};
use highway;
use log::{error, info};

fn main() -> Result<()> {
    pretty_env_logger::init();
    let segmenter = ThreadSafeFileSegmenter::init("./segment.bin".into())?;
    let directory = MMapDirectory::init("./directory.bin".into())?;
    const WRITE_THREADS: usize = 16;
    const READ_THREADS: usize = 24;
    let lock = StripedLock::init((WRITE_THREADS * 2) + 10);
    let mehdb = MehDB {
        hasher_key: highway::Key([53252, 2352323, 563956259, 234832]),
        directory: Arc::new(directory),
        segmenter: segmenter.clone(),
        lock: Arc::new(lock),
    };
    let mut write_threads: Vec<JoinHandle<()>> = Vec::with_capacity(4);
    const RECORDS: usize = 100_000_000;
    let start_time = Instant::now();
    for thread_id in 0..WRITE_THREADS {
        let mut db = mehdb.clone();
        write_threads.push(spawn(move || {
            let min = thread_id * (RECORDS / WRITE_THREADS);
            let max = (thread_id + 1) * (RECORDS / WRITE_THREADS);
            for i in min..max {
                let i = i as u64;
                let key = ByteKey(i.to_le_bytes().to_vec());
                let value = ByteValue(i * 2);
                db.put(key, value)
                    .context("Error inserting record")
                    .expect("Unable to insert record!");
            }
        }));
    }
    for (thread_id, thread) in write_threads.into_iter().enumerate() {
        thread.join().expect("Thread paniced...");
        info!("Thread {} finished.", thread_id);
    }
    let end_time = start_time.elapsed().as_secs_f64();
    println!(
        "Elapsed time: {:.2?}\nAvg inserts/us: {:.2}",
        end_time,
        RECORDS as f64 / end_time
    );
    let mut read_threads: Vec<JoinHandle<bool>> = Vec::with_capacity(4);
    let start_time = Instant::now();
    // Read operations
    for thread_id in 0..READ_THREADS {
        let mut db = mehdb.clone();
        read_threads.push(spawn(move || {
            let min = thread_id * (RECORDS / READ_THREADS);
            let max = (thread_id + 1) * (RECORDS / READ_THREADS);
            let mut errors = false;
            for i in min..max {
                let i = i as u64;
                let key = ByteKey(i.to_le_bytes().to_vec());
                match  db.get(key) {
                    None => {
                        //error!("Record missing for {} in thread {}", i, thread_id);
                        errors = true;
                    }
                    Some(r) => {
                        if r.value != i * 2 {
                            error!(
                                "read value does not match: {}. Expected: {}",
                                r.value,
                                i * 2
                            );
                            errors = true;
                        }
                    }
                }
                //println!("k: {} v: {:?}", i, r);
            }
            return errors;
        }));
    }
    for (thread_id, thread) in read_threads.into_iter().enumerate() {
        let errors = thread.join().expect("Thread paniced...");
        if errors {
            error!("Thread {} had errors", thread_id);
        }
    }
    let end_time = start_time.elapsed().as_secs_f64();
    println!(
        "Elapsed time: {:.2?}\nAvg gets/s: {:.2}",
        end_time,
        RECORDS as f64 / end_time
    );
    // Verify single-threaded that this all works...
    //for i in 0..RECORDS {
    //    info!("i: {}", i);
    //    let i = i as u64;
    //    let key = ByteKey(i.to_le_bytes().to_vec());
    //    let get_key = key.clone();
    //    let value = ByteValue(i * 2);
    //    mehdb
    //        .put(key, value)
    //        .context("Error inserting record")
    //        .expect("Unable to insert record!");
    //    mehdb
    //        .get(get_key)
    //        .with_context(|| format!("Key 0 missing at i {}", i))
    //        .unwrap();
    //}
    //let end_time = start_time.elapsed().as_secs_f64();
    //println!(
    //    "Elapsed time: {:.2?}\nAvg inserts/us: {:.2}",
    //    end_time,
    //    RECORDS as f64 / end_time
    //);
    //info!("Done putting, trying to read now...");
    //let start_time = Instant::now();
    //for i in 0..RECORDS {
    //    let i = i as u64;
    //    let key = ByteKey(i.to_le_bytes().to_vec());
    //    let r = mehdb.get(key).expect(&format!("Missing record for {}", i));
    //    info!("k: {} v: {:?}", i, r);
    //}
    //let end_time = start_time.elapsed().as_secs_f64();
    //println!(
    //    "Elapsed time: {:.2?}\nAvg gets/s: {:.2}",
    //    end_time,
    //    RECORDS as f64 / end_time
    //);
    Ok(())
}
