use crate::serializer::{DataOrOffset, Serializable};
use crate::segment::bucket::{Bucket, BUCKET_SIZE};
use crate::segment::BasicSegmenter;
use simple_error::SimpleError;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, Write};
use std::mem::size_of;
use std::ops::{Index, IndexMut};
use std::path::Path;
use crate::segment::{Segment, Segmenter};
use anyhow::Result;
use log::info;

// The number of buckets in each segment.
// This may be adapted to be parametrizable on a per-database level
// in the futere.
const BUCKETS_PER_SEGMENT: usize = 64;
// The size on-disk of a segment
const SEGMENT_SIZE: usize = (BUCKET_SIZE * BUCKETS_PER_SEGMENT) + 8;


pub fn file_segmenter(path: Option<&Path>) -> Result<BasicSegmenter<File>> {
    let path: &Path = match path {
        Some(path) => path,
        None => Path::new("index.bin"),
    };
    let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)?;
    BasicSegmenter::init(file)
}
