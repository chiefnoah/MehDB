#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
extern crate pretty_env_logger;


pub mod directory;
pub mod meh;
pub mod segment;
pub mod serializer;

use crate::meh::MehDB;
use crate::serializer::{ByteKey, ByteValue};
use anyhow::{Result, Context};
use log::info;
use std::time::{Instant};


fn main() -> Result<()>{
    pretty_env_logger::init();
    let mut db: MehDB = MehDB::init(None).unwrap();
    const RECORDS: usize = 1_000_000;
    let start_time = Instant::now();
    for i in 0..RECORDS {
        info!("i: {}", i);
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let value = ByteValue(i * 2);
        db.put(key, value)
            .context("Error inserting record")
            .expect("Unable to insert record!");
    }
    let end_time = start_time.elapsed();
    println!("Elapsed time: {:.2?}\nAvg inserts/us: {}", end_time, RECORDS as f64 / end_time.as_secs_f64());
    info!("Done putting, trying to read now...");
    let start_time = Instant::now();
    for i in 0..RECORDS {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let _ = db.get(key).expect(&format!("Missing record for {}", i));
        //println!("k: {} v: {:?}", i, r);
        //assert_eq!(r.value, i * 2);
    }
    let end_time = start_time.elapsed();
    println!("Elapsed time: {:.2?}\nAvg gets/s: {}", end_time, RECORDS as f64 / end_time.as_secs_f64());
    Ok(())
}
