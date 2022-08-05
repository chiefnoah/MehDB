use std::io::{self, Write, Read, Seek};
use crate::serializer::Serializable;
use anyhow::Result;

pub struct Header {
    pub global_depth: u64,
    pub num_segments: u64,
}

impl Serializable for Header {
    fn pack<W: Write + Seek>(&self, buffer: &mut W) -> Result<u64> {
        let global_depth_bytes = self.global_depth.to_le_bytes();
        let num_segment_bytes = self.num_segments.to_le_bytes();
        let offset = buffer.seek(io::SeekFrom::Current(0)).unwrap();
        buffer.write(&global_depth_bytes)?;
        buffer.write(&num_segment_bytes)?;
        Ok(offset)
    }

    fn unpack<R: Read + Seek>(file: &mut R) -> Result<Self> {
        let mut buf: [u8; 8] = [0; 8];
        file.read_exact(&mut buf[..])?;
        let global_depth = u64::from_le_bytes(buf);
        file.read_exact(&mut buf[..])?;
        let num_segments = u64::from_le_bytes(buf);
        Ok(Header {
            global_depth,
            num_segments,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use io::{self, Read, Write, Cursor, Seek};

    #[test]
    fn header_can_pack() {
        let header: Header = Header{global_depth: 1, num_segments: 2};
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let res = header.pack::<Cursor<Vec<u8>>>(&mut buf);
        match res {
            Err(e) => panic!("Error: {}", e),
            _ => (),
        }
        let expected_buf: [u8; 16] = [0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
                                      0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0];
        assert_eq!(&buf.into_inner()[..], &expected_buf[..]);
    }

    #[test]
    fn header_can_unpack() {
        let fixture: Vec<u8> = vec![0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
                                    0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0];
        let mut buf = Cursor::new(fixture);
        let header = match Header::unpack(&mut buf) {
            Err(e) => panic!("Unable to unpack header: {}", e),
            Ok(h) => h,
        };
        assert_eq!(header.global_depth, 1);
        assert_eq!(header.num_segments, 2);
    }

    #[test]
    fn header_too_small_buffer_unpack() {
        let fixture: Vec<u8> = vec![0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0];
        let mut buf = Cursor::new(fixture);
        match Header::unpack(&mut buf) {
            Err(e) => (),
            Ok(h) => panic!("Was able to unpack header from too small of buffer."),
        }
    }
}
