

pub struct SegmentWorker<B: Read + Write + Seek> {
    buffer: B,
    owned_bits: u8,
}
