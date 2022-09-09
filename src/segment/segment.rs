use crate::segment::bucket::{Bucket, BUCKET_SIZE};
use crate::serializer::{DataOrOffset, Serializable};
use anyhow::{Context, Result};
use log::{debug, info, warn, trace};
use simple_error::SimpleError;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Cursor, Read, Seek, Write};
use std::iter::Iterator;
use std::mem::size_of;
use std::ops::{Index, IndexMut};
use std::path::Path;

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

/// A Segmenter is a something responsible for allocating and reading segments
pub trait Segmenter {
    /// Header is a Sized type used to identify the offset for reading and writing segments. Some
    /// implementations will have a 0-sized header. The `BasicSegmenter` will have a u64 header for
    /// storing `num_segments`. Implementations can expose a way to read the header in their API,
    /// but it is not strictly necessary.
    type Header;
    /// Attempts to read an existing segment.
    fn segment(&mut self, index: u32) -> Result<Segment>;
    /// Creates a new segment and returns a tuple containing the index of the newly created segment
    /// and an instance of `Segment`. This increments `num_segments` by one. If the implementation
    /// stores on disk, it should write the new segment then increment the persisted `num_segments`
    /// after, effectively "committing" the change.
    fn allocate_segment(&mut self, depth: u8) -> Result<(u32, Segment)>;
    /// The same as `Segmenter.allocate_segment` except instead of empty buckets it writes the
    /// provided buckets.
    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u8)
        -> Result<(u32, Segment)>;
    /// Retrieves a bucket from segment at bucket index. The resulting bucket can be modified and synced
    /// back to to the Segmenter using `write_bucket`. Implementations that support concurrency are
    /// responsible for providing the appropriate locking mechanism to prevent race-conditions.
    fn bucket(&mut self, segment: &Segment, index: u32) -> Result<Bucket>;
    /// Overwrites an existing bucket.
    fn write_bucket(&mut self, bucket: &Bucket) -> Result<()>;
    /// Returns the *current* number of segements allocated. This may or may not be cached
    /// in-memory. Implementations that save segements to non-volatile storage *should* store this
    /// value along with the segments. If the implementation supports concurrency, this should
    /// only be used in a context where a read of this value is guaranteed to be correct for the
    /// duration of it's use. That is, if you support concurrent segement creation, this method
    /// should only be called inside a syncronized block.
    fn num_segments(&mut self) -> Result<u32>;
    /// Updates the local_depth of `Segment`. Implementations that allow for concurrent access
    /// should take care to prevent race conditions.
    fn update_segment(&mut self, segment: Segment) -> Result<()>;
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

pub struct BasicSegmenter<B: Read + Write + Seek> {
    buffer: B,
    num_segments: u32,
    header_dirty: bool,
    // Maps segment index to depth
    segment_depth_cache: HashMap<u32, u8>,
    iter_index: usize,
}

impl<B: Read + Write + Seek> BasicSegmenter<B> {
    pub fn init(mut buffer: B) -> Result<Self> {
        buffer
            .seek(io::SeekFrom::Start(0))
            .context("Seeking to beginning of segment file.")?;
        // Attempt to read the header and use it, otherwise initialize as new
        let mut first_time = false;
        let num_segments = match u32::unpack(&mut buffer) {
            Ok(n) => n,
            Err(e) => {
                first_time = true;
                0
            }
        };
        let mut out = Self {
            num_segments,
            buffer,
            header_dirty: false,
            segment_depth_cache: HashMap::new(),
            iter_index: 0,
        };
        if first_time {
            out.allocate_segment(0)
                .with_context(|| format!("Error initializing initial segment."))?;
        }
        Ok(out)
    }

    fn write_num_segments(&mut self) -> Result<()> {
        self.buffer
            .seek(io::SeekFrom::Start(0))
            .context("Seeking to beginning of segment file.")?;
        self.buffer
            .write_all(&self.num_segments.to_le_bytes())
            .context("Syncing num_segments")?;
        self.buffer.flush()?;
        Ok(())
    }

    fn segment_offset_to_index(segment: &Segment) -> u32 {
        return ((segment.offset as usize - size_of::<<BasicSegmenter<B> as Segmenter>::Header>())
            / SEGMENT_SIZE) as u32;
    }
}

impl<B> Segmenter for BasicSegmenter<B>
where
    B: Write + Read + Seek,
{
    // For this implementation, the header is simply the u32 num_segments
    type Header = u32;
    fn segment(&mut self, index: u32) -> Result<Segment> {
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        if let Some(cached_depth) = self.segment_depth_cache.get(&index) {
            return Ok(Segment {
                depth: *cached_depth,
                offset,
            });
        }
        self.buffer.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 1] = [0; 1];
        self.buffer.read_exact(&mut buf).with_context(|| {
            format!(
                "Error reading segment local depth for segment index {}/{} with offset {}",
                index, self.num_segments, offset
            )
        })?;
        let depth = u8::from_le_bytes(buf);
        self.segment_depth_cache.insert(index, depth);
        Ok(Segment { depth, offset })
    }

    fn allocate_segment(&mut self, depth: u8) -> Result<(u32, Segment)> {
        debug!("Allocating empty segment with depth {}", depth);
        // Seek to the proper offset
        let index = self.num_segments;
        //------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        debug!("New segment offset: {}", offset);
        self.buffer
            .seek(io::SeekFrom::Start(offset))
            .with_context(|| format!("Seeking to new segment's offset {}", offset))?;
        let mut buf: [u8; SEGMENT_SIZE] = [0; SEGMENT_SIZE];
        buf[..1].copy_from_slice(&depth.to_le_bytes());
        self.buffer
            .write_all(&buf)
            .context("Writing new segment bytes")?;
        self.buffer
            .flush()
            .context("Flushing buffer after segment allocate.")?;
        self.num_segments += 1;
        self.write_num_segments()
            .context("Syncronizing num_segments after allocating.")?;
        Ok((index, Segment { depth, offset }))
    }

    fn allocate_with_buckets(
        &mut self,
        buckets: Vec<Bucket>,
        depth: u8,
    ) -> Result<(u32, Segment)> {
        // The number of buckets passed in *must* be the entire segment's buckets
        assert!(buckets.len() == BUCKETS_PER_SEGMENT);

        let index = self.num_segments;
        // ------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        info!("New segment offset: {}", offset);
        // We create a BufWriter because we're going to be writing a lot and don't want to flush it
        // until we're done.
        let mut buffer = BufWriter::with_capacity(SEGMENT_SIZE as usize, &mut self.buffer);
        buffer
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to new segment location")?;
        let mut buf: [u8; 1] = [0; 1];
        buf[..].copy_from_slice(&depth.to_le_bytes());
        buffer
            .write_all(&buf)
            .context("Writing new segment's depth.")?;
        for (i, bucket) in buckets.iter().enumerate() {
            debug!("Writing bucket with index {}", i);
            bucket
                .pack(&mut buffer)
                .with_context(|| format!("Writing bucket with index {}", i))?;
        }
        buffer
            .flush()
            .context("Flushing outer buffer after segment allocate.")?;
        drop(buffer);
        let index = self.num_segments;
        self.num_segments += 1;
        self.write_num_segments()
            .context("Syncronizing num_segments after allocating.")?;
        let end_of_buffer = self
            .buffer
            .seek(io::SeekFrom::End(0))
            .context("Seeking to end of buffer")?;
        trace!("End of buffer: {}", end_of_buffer);
        Ok((index, Segment { offset, depth }))
    }

    fn bucket(&mut self, segment: &Segment, index: u32) -> Result<Bucket> {
        assert!(index < BUCKETS_PER_SEGMENT as u32);
        //----------------------------------------------------------👇 for segment_depth
        let offset = segment.offset + (index as usize * BUCKET_SIZE) as u64 + 1;
        self.buffer
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to bucket's offset")?;
        debug!("Reading bucket at offset {}", offset);
        return Bucket::unpack(&mut self.buffer);
    }

    fn write_bucket(&mut self, bucket: &Bucket) -> Result<()> {
        self.buffer
            .seek(io::SeekFrom::Start(bucket.offset))
            .context("Seeking to bucket's offset")?;
        bucket.pack(&mut self.buffer)?;
        self.buffer.flush()?;
        Ok(())
    }

    fn num_segments(&mut self) -> Result<u32> {
        Ok(self.num_segments)
    }

    fn update_segment(&mut self, segment: Segment) -> Result<()> {
        debug!("Updating segment depth to {}", segment.depth);
        self.buffer.seek(io::SeekFrom::Start(segment.offset))?;
        self.buffer.write_all(&segment.depth.to_le_bytes())?;
        self.buffer.flush()?;
        let index = Self::segment_offset_to_index(&segment);
        if self.segment_depth_cache.contains_key(&index) {
            self.segment_depth_cache
                .insert(index, segment.depth);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::{self, Cursor, Read, Seek, Write};

    fn inmemory_segmenter() -> BasicSegmenter<Cursor<Vec<u8>>> {
        let buffer = Cursor::new(Vec::new());
        BasicSegmenter::init(buffer).unwrap()
    }

    #[test]
    fn can_pack_segment() {
        let segment = Segment {
            depth: 5,
            offset: 3,
        };
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let result = segment.pack(&mut buf).unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn can_unpack_segment() {
        let fixture: Vec<u8> = vec![0x10, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut buf: Cursor<Vec<u8>> = Cursor::new(fixture);
        buf.seek(io::SeekFrom::Current(1)).unwrap();
        let segment = Segment::unpack(&mut buf).unwrap();
        assert_eq!(segment.depth, 51);
        assert_eq!(segment.offset, 1);
    }

    #[test]
    fn segmenter_init_syncs_num_segments() {
        let mut segmenter = inmemory_segmenter();
        let num_segments = segmenter.num_segments().unwrap();
        assert_eq!(num_segments, 1);
    }

    #[test]
    fn segmenter_init_initializes_first_segment() {
        let mut segmenter = inmemory_segmenter();
        const HEADER_SIZE: usize =
            size_of::<<BasicSegmenter<Cursor<Vec<u8>>> as Segmenter>::Header>();
        let first_segment = segmenter.segment(0).unwrap();
        // Get the last bucket
        let mut bucket = segmenter
            .bucket(&first_segment, BUCKETS_PER_SEGMENT as u32 - 1)
            .unwrap();
        bucket
            .put(123, 456, 0)
            .expect("Unable to insert record into bucket");
        segmenter
            .write_bucket(&bucket)
            .expect("Unable to write bucket to segment");
        let bucket = segmenter
            .bucket(&first_segment, BUCKETS_PER_SEGMENT as u32 - 1)
            .unwrap();
        let r = bucket.get(123).unwrap();
        assert_eq!(r.hash_key, 123);
        assert_eq!(r.value, 456);

        let expected_size = SEGMENT_SIZE + HEADER_SIZE;
        assert_eq!(segmenter.buffer.into_inner().len(), expected_size);
    }

    #[test]
    fn segmenter_initialize_segment_correct_size() {
        let mut segmenter = inmemory_segmenter();
        const HEADER_SIZE: usize =
            size_of::<<BasicSegmenter<Cursor<Vec<u8>>> as Segmenter>::Header>();
        let first_segment = segmenter.segment(0).unwrap();
        let second_segment = segmenter.allocate_segment(3).unwrap().1;
        assert_eq!(second_segment.depth, 3);
        let expected_offset = HEADER_SIZE + SEGMENT_SIZE;
        assert_eq!(second_segment.offset, expected_offset as u64);
        let another_second_segment = segmenter.segment(1).unwrap();
        assert_eq!(another_second_segment.offset, second_segment.offset);
        assert_eq!(another_second_segment.depth, second_segment.depth);
        let expected_buffer_length = HEADER_SIZE + 2 * (SEGMENT_SIZE);
        let actual_len = segmenter.buffer.into_inner().len();
        assert_eq!(actual_len, expected_buffer_length);
    }

    #[test]
    fn allocate_segment_doesnt_overwrite_previous_bucket() {
        let mut segmenter = inmemory_segmenter();
        let segment = segmenter.segment(0).expect("Unable to get segment.");
        let mut last_bucket_first_segment = segmenter
            .bucket(&segment, BUCKETS_PER_SEGMENT as u32 - 1)
            .expect("Unable to read last bucket.");
        last_bucket_first_segment
            .put(123, 456, 0)
            .expect("Unable to insert record into bucket.");
        segmenter
            .write_bucket(&last_bucket_first_segment)
            .expect("Saving bucket back to ");
        segmenter
            .allocate_segment(0)
            .expect("Unable to allocate segment");
        last_bucket_first_segment = segmenter
            .bucket(&segment, BUCKETS_PER_SEGMENT as u32 - 1)
            .expect("Unable to read last bucket.");
        assert_eq!(last_bucket_first_segment.get(123).unwrap().value, 456);
    }

    #[test]
    fn allocate_segment_with_buckets_doesnt_overwrite_previous_bucket() {
        let mut segmenter = inmemory_segmenter();
        let segment = segmenter.segment(0).expect("Unable to get segment.");
        let mut last_bucket_first_segment = segmenter
            .bucket(&segment, BUCKETS_PER_SEGMENT as u32 - 1)
            .expect("Unable to read last bucket.");
        last_bucket_first_segment
            .put(123, 456, 0)
            .expect("Unable to insert record into bucket.");
        segmenter
            .write_bucket(&last_bucket_first_segment)
            .expect("Saving bucket back to ");
        let mut buckets = Vec::<Bucket>::with_capacity(BUCKETS_PER_SEGMENT);
        for bi in 0..BUCKETS_PER_SEGMENT {
            let b = segmenter.bucket(&segment, bi as u32).unwrap();
            buckets.push(b);
        }
        segmenter
            .allocate_with_buckets(buckets, 0)
            .expect("Unable to allocate segment");
        last_bucket_first_segment = segmenter
            .bucket(&segment, BUCKETS_PER_SEGMENT as u32 - 1)
            .expect("Unable to read last bucket.");
        assert_eq!(last_bucket_first_segment.get(123).unwrap().value, 456);
    }

    use crate::segment::bucket::BUCKET_RECORDS;
    #[test]
    fn allocate_many_segments() {
        let mut segmenter = inmemory_segmenter();
        for z in 1..10 {
            let mut buckets = Vec::<Bucket>::with_capacity(BUCKETS_PER_SEGMENT);
            for i in 0..BUCKETS_PER_SEGMENT {
                let mut bucket: Bucket = Default::default();
                for y in 0..BUCKET_RECORDS {
                    let k: u64 = ((z + 1) * (i + 1) * (y + 1)) as u64;
                    let v: u64 = k * 2;
                    bucket.put(k, v, 0).unwrap();
                }
                buckets.push(bucket);
            }
            segmenter.allocate_with_buckets(buckets, 0).unwrap();
        }
        let first_segment = segmenter.segment(0).unwrap();
        for i in 0..BUCKETS_PER_SEGMENT {
            let bucket = segmenter.bucket(&first_segment, i as u32).unwrap();
            for y in 0..BUCKET_RECORDS {
                match bucket.get(y as u64) {
                    Some(r) => {
                        panic!(
                            "We aren't supposed to have anything here!? Found: {}: {}",
                            r.hash_key, r.value
                        );
                    }
                    None => (),
                }
            }
        }
        for z in 1..10 {
            let segment = segmenter.segment(z).unwrap();
            for i in 0..BUCKETS_PER_SEGMENT {
                let bucket = segmenter.bucket(&segment, i as u32).unwrap();
                for y in 0..BUCKET_RECORDS {
                    // Why the FUCK does this need to be different from the above
                    // The compiler complains about multiplying usize by u64, but there's *no*
                    // reason for z, i, or y to be anything *but* usize.
                    let k: u64 = (z + 1) as u64 * (i + 1) as u64 * (y + 1) as u64;
                    let expected_v = k * 2;
                    let r = bucket.get(k).unwrap();
                    assert_eq!(r.value, expected_v);
                }
            }
        }
    }
}
