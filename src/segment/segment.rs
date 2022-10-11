use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::iter::Iterator;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::Arc;
use std::cell::RefCell;

use crate::segment::bucket::{Bucket, BUCKET_SIZE};
use crate::serializer::Serializable;

use anyhow::{Context, Result};
use log::{debug, info};
use parking_lot::Mutex;

// The number of buckets in each segment.
// This may be adapted to be parametrizable on a per-database level
// in the futere.
pub const BUCKETS_PER_SEGMENT: usize = 64;

// The size on-disk of a segment
// ------------------------------------------------------------------👇 offset for local_depth
pub const SEGMENT_SIZE: usize = (BUCKET_SIZE * BUCKETS_PER_SEGMENT) + 1;

pub struct Segment {
    pub depth: u8,
    pub offset: u64,
}

impl Serializable for Segment {
    /// packs a Segment's depth. Assumes buffer has alread Seeked to offset
    fn pack<W: Write + Seek>(&self, buffer: &mut W) -> Result<u64> {
        buffer
            .write_all(&self.depth.to_le_bytes())
            .with_context(|| format!("Error packing segmenter at offset {}", self.offset))?;
        Ok(self.offset)
    }

    /// unpacks a segment's depth. Assumes buffer has already Seeked to proper offset
    fn unpack<R: Read + Seek>(buffer: &mut R) -> Result<Self> {
        let offset = buffer.seek(io::SeekFrom::Current(0))?;
        let mut b: [u8; 1] = [0; 1];
        buffer
            .read_exact(&mut b)
            .with_context(|| format!("Error unpacking segment at offset {}", offset))?;
        let depth = u8::from_le_bytes(b);
        Ok(Self { offset, depth })
    }
}

impl Serializable for u32 {
    fn pack<W: Write + Seek>(&self, buffer: &mut W) -> Result<u64> {
        let buf = self.to_le_bytes();
        buffer.write_all(&buf).context("Writing u32")?;
        Ok(4)
    }
    fn unpack<R: Read + Seek>(buffer: &mut R) -> Result<Self> {
        let mut buf: [u8; 4] = [0; 4];
        buffer.read_exact(&mut buf).context("Reading u32")?;
        Ok(u32::from_le_bytes(buf))
    }
}


/// A Segmenter is a something responsible for allocating and reading segments
pub trait Segmenter {
    /// Header is a Sized type used to identify the offset for reading and writing segments. Some
    /// implementations will have a 0-sized header. The `BasicSegmenter` will have a u64 header for
    /// storing `num_segments`. Implementations can expose a way to read the header in their API,
    /// but it is not strictly necessary.
    type Header;
    /// Attempts to read an existing segment.
    fn segment(&self, index: u32) -> Result<Segment>;
    /// Creates a new segment and returns a tuple containing the index of the newly created segment
    /// and an instance of `Segment`. This increments `num_segments` by one. If the implementation
    /// stores on disk, it should write the new segment then increment the persisted `num_segments`
    /// after, effectively "committing" the change.
    fn allocate_segment(&self, depth: u8) -> Result<(u32, Segment)>;
    /// The same as `Segmenter.allocate_segment` except instead of empty buckets it writes the
    /// provided buckets.
    fn allocate_with_buckets(&self, buckets: Vec<Bucket>, depth: u8) -> Result<(u32, Segment)>;
    /// Retrieves a bucket from segment at bucket index. The resulting bucket can be modified and synced
    /// back to to the Segmenter using `write_bucket`. Implementations that support concurrency are
    /// responsible for providing the appropriate locking mechanism to prevent race-conditions.
    fn bucket(&self, segment: &Segment, index: u32) -> Result<Bucket>;
    /// Overwrites an existing bucket.
    fn write_bucket(&self, bucket: &Bucket) -> Result<()>;
    /// Returns the *current* number of segements allocated. This may or may not be cached
    /// in-memory. Implementations that save segements to non-volatile storage *should* store this
    /// value along with the segments. If the implementation supports concurrency, this should
    /// only be used in a context where a read of this value is guaranteed to be correct for the
    /// duration of it's use. That is, if you support concurrent segement creation, this method
    /// should only be called inside a syncronized block.
    fn num_segments(&self) -> Result<u32>;
    /// Updates the local_depth of `Segment`. Implementations that allow for concurrent access
    /// should take care to prevent race conditions.
    fn update_segment(&self, segment: Segment) -> Result<()>;
}

pub struct ThreadSafeFileSegmenter {
    path: PathBuf,
    file: RefCell<File>,
    segment_file_lock: Arc<Mutex<u32>>,
}

impl Clone for ThreadSafeFileSegmenter {
    fn clone(&self) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(&self.path)
            .expect("Unable to clone ThreadSafeSegmenter<File>");
        Self {
            path: self.path.clone(),
            file: RefCell::new(file),
            segment_file_lock: self.segment_file_lock.clone(),
        }
    }
}

impl Segmenter for ThreadSafeFileSegmenter {
    // For this implementation, the header is simply the u32 num_segments
    type Header = u32;
    fn segment(&self, index: u32) -> Result<Segment> {
        let mut file = self.file.borrow_mut();
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        file.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 1] = [0; 1];
        file.read_exact(&mut buf).with_context(|| {
            format!(
                "Error reading segment local depth for segment index {} with offset {}",
                index, offset
            )
        })?;
        let depth = u8::from_le_bytes(buf);
        Ok(Segment { depth, offset })
    }

    fn allocate_segment(&self, depth: u8) -> Result<(u32, Segment)> {
        debug!("Allocating empty segment with depth {}", depth);
        let mut file = self.file.borrow_mut();
        let mut num_segments = self.segment_file_lock.lock();
        let index = (*num_segments).clone();
        *num_segments += 1;
        // Flush write the current number of segments to the file
        file
            .seek(io::SeekFrom::Start(0))
            .context("Seeking to beginning of segment file.")?;
        file
            .write_all(&num_segments.to_le_bytes())
            .context("Syncing num_segments")?;
        file.flush()?;
        //------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        debug!("New segment offset: {}", offset);
        // Seek to the proper offset
        file
            .seek(io::SeekFrom::Start(offset))
            .with_context(|| format!("Seeking to new segment's offset {}", offset))?;
        let mut buf: [u8; SEGMENT_SIZE] = [0; SEGMENT_SIZE];
        buf[..1].copy_from_slice(&depth.to_le_bytes());
        file
            .write_all(&buf)
            .context("Writing new segment bytes")?;
        file
            .flush()
            .context("Flushing buffer after segment allocate.")?;
        Ok((index, Segment { depth, offset }))
    }

    fn allocate_with_buckets(&self, buckets: Vec<Bucket>, depth: u8) -> Result<(u32, Segment)> {
        // The number of buckets passed in *must* be the entire segment's buckets
        let mut file = self.file.borrow_mut();

        let mut num_segments = self.segment_file_lock.lock();
        let index = (*num_segments).clone();
        *num_segments += 1;
        // Flush write the current number of segments to the file
        file
            .seek(io::SeekFrom::Start(0))
            .context("Seeking to beginning of segment file.")?;
        file
            .write_all(&num_segments.to_le_bytes())
            .context("Syncing num_segments")?;
        // ------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        info!("New segment offset: {}", offset);
        // We create a BufWriter because we're going to be writing a lot and don't want to flush it
        // until we're done.
        file
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to new segment location")?;
        let mut buf: [u8; 1] = [0; 1];
        buf[..].copy_from_slice(&depth.to_le_bytes());
        file
            .write_all(&buf)
            .context("Writing new segment's depth.")?;
        for (i, bucket) in buckets.iter().enumerate() {
            debug!("Writing bucket with index {}", i);
            bucket
                .pack(&mut *file)
                .with_context(|| format!("Writing bucket with index {}", i))?;
        }
        file
            .flush()
            .context("Flushing outer buffer after segment allocate.")?;
        Ok((index, Segment { offset, depth }))
    }

    fn bucket(&self, segment: &Segment, index: u32) -> Result<Bucket> {
        assert!(index < BUCKETS_PER_SEGMENT as u32);
        let mut file = self.file.borrow_mut();
        //----------------------------------------------------------👇 for segment_depth
        let offset = segment.offset + (index as usize * BUCKET_SIZE) as u64 + 1;
        file
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to bucket's offset")?;
        debug!("Reading bucket at offset {}", offset);
        return Bucket::unpack(&mut *file);
    }

    fn write_bucket(&self, bucket: &Bucket) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file
            .seek(io::SeekFrom::Start(bucket.offset))
            .context("Seeking to bucket's offset")?;
        bucket.pack(&mut *file)?;
        file.flush()?;
        Ok(())
    }

    fn num_segments(&self) -> Result<u32> {
        let num_segments = self.segment_file_lock.lock();
        Ok(*num_segments)
    }

    fn update_segment(&self, segment: Segment) -> Result<()> {
        debug!("Updating segment depth to {}", segment.depth);
        let mut file = self.file.borrow_mut();
        file.seek(io::SeekFrom::Start(segment.offset))?;
        file.write_all(&segment.depth.to_le_bytes())?;
        file.flush()?;
        Ok(())
    }
}

impl ThreadSafeFileSegmenter {
    /// Sets up and initializes the
    pub fn init(path: PathBuf) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(&path)
            .expect("Unable to clone ThreadSafeFileSegmenter");
        file.seek(io::SeekFrom::Start(0))
            .context("Seeking to beginning of segment file.")?;
        let mut first_time = false;
        // Attempt to read the header and use it, otherwise initialize as new
        let num_segments = match u32::unpack(&mut file) {
            Ok(n) => n,
            Err(_) => {
                first_time = true;
                0
            }
        };
        let out = Self {
            file: RefCell::new(file),
            path,
            segment_file_lock: Arc::new(Mutex::new(num_segments)),
        };
        if first_time {
            out.allocate_segment(0)
                .context("Error initializing first segment.")?;
        }

        Ok(out)
    }
}

