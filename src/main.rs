#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
extern crate pretty_env_logger;

pub mod directory;
pub mod locking;
pub mod meh;
pub mod segment;
pub mod serializer;

use crate::meh::MehDB;
use crate::serializer::{ByteKey, ByteValue};
use anyhow::{Context, Result};
use log::info;
use std::time::Instant;
use std::sync::Arc;
use std::thread;

fn main() -> Result<()> {
    pretty_env_logger::init();
    let db: Arc<MehDB> = Arc::new(MehDB::init(None).unwrap());
    let db0 = db.clone();
    let db1 = db.clone();
    let db2 = db.clone();
    let db3 = db.clone();
    const RECORDS: usize = 1_000_000;
    let start_time = Instant::now();
    let thread0 = thread::spawn(move || {
        for i in 0..RECORDS/4 {
            info!("i: {}", i);
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let value = ByteValue(i * 2);
            db0.put(key, value)
                .context("Error inserting record")
                .expect("Unable to insert record!");
        }
        for i in 0..RECORDS/4 {
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let r = db0.get(key).expect(&format!("Missing record for {}", i));
            info!("k: {} v: {:?}", i, r);
        }
    });
    let thread1 = thread::spawn(move || {
        for i in RECORDS/4..RECORDS/2 {
            info!("i: {}", i);
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let value = ByteValue(i * 2);
            db1.put(key, value)
                .context("Error inserting record")
                .expect("Unable to insert record!");
        }
        for i in RECORDS/4..RECORDS/2 {
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let r = db1.get(key).expect(&format!("Missing record for {}", i));
            info!("k: {} v: {:?}", i, r);
        }
    });
    let thread2 = thread::spawn(move || {
        for i in RECORDS/2..RECORDS*(3/4) {
            info!("i: {}", i);
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let value = ByteValue(i * 2);
            db2.put(key, value)
                .context("Error inserting record")
                .expect("Unable to insert record!");
        }
        for i in RECORDS/2..RECORDS*(3/4) {
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let r = db2.get(key).expect(&format!("Missing record for {}", i));
            info!("k: {} v: {:?}", i, r);
        }
    });
    let thread3 = thread::spawn(move || {
        for i in RECORDS*(3/4)..RECORDS {
            info!("i: {}", i);
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let value = ByteValue(i * 2);
            db3.put(key, value)
                .context("Error inserting record")
                .expect("Unable to insert record!");
        }
        for i in RECORDS*(3/4)..RECORDS {
            let i = i as u64;
            let key = ByteKey(i.to_le_bytes().to_vec());
            let r = db3.get(key).expect(&format!("Missing record for {}", i));
            info!("k: {} v: {:?}", i, r);
        }
    });
    thread0.join().unwrap();
    thread1.join().unwrap();
    thread2.join().unwrap();
    thread3.join().unwrap();
    info!("Done with threaded mode. Double checking queries.");
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
        let r = db.get(key).expect(&format!("Missing record for {}", i));
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
