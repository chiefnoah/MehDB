#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
extern crate pretty_env_logger;


pub mod directory;
pub mod meh;
pub mod segment;
pub mod serializer;
pub mod locking;

use crate::meh::MehDB;
use crate::serializer::{ByteKey, ByteValue};
use anyhow::{Result, Context};
use log::info;
use std::time::Instant;


fn main() -> Result<()>{
    pretty_env_logger::init();
    let mut db: MehDB = MehDB::init(None).unwrap();
    const RECORDS: usize = 10_000_000;
    let start_time = Instant::now();
    for i in 0..RECORDS {
        info!("i: {}", i);
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let value = ByteValue(i * 2);
        db.put(key, value)
            .context("Error inserting record")
            .expect("Unable to insert record!");
        let testkey = ByteKey((0 as u64).to_le_bytes().to_vec());
        db.get(testkey).with_context(|| {
            format!("Key 0 missing at i {}", i)
        }).unwrap();

    }
    let end_time = start_time.elapsed().as_secs_f64();
    println!("Elapsed time: {:.2?}\nAvg inserts/us: {:.2}", end_time, RECORDS as f64 / end_time);
    info!("Done putting, trying to read now...");
    let start_time = Instant::now();
    for i in 0..RECORDS {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let r = db.get(key).expect(&format!("Missing record for {}", i));
        info!("k: {} v: {:?}", i, r);
    }
    let end_time = start_time.elapsed().as_secs_f64();
    println!("Elapsed time: {:.2?}\nAvg gets/s: {:.2}", end_time, RECORDS as f64 / end_time);
    Ok(())
}
