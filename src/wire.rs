use crate::capture::{PixelFormat, StreamSpec};
use crate::config::{ECHO_BYTES, HEADER_BYTES, MAGIC};

// [magic][frame_id][capture_ts][width][height][format][raw_len]
// [chunk_index][total_chunks][chunk_len][flags]
pub struct PacketHeader {
    pub frame_id: u32,
    pub capture_ts: u64,
    pub stream: StreamSpec,
    pub chunk_index: u16,
    pub total_chunks: u16,
    pub chunk_len: u16,
    pub flags: u8,
}

impl PacketHeader {
    pub fn encode(&self, out: &mut [u8]) {
        out[0..2].copy_from_slice(&MAGIC.to_be_bytes());
        out[2..6].copy_from_slice(&self.frame_id.to_be_bytes());
        out[6..14].copy_from_slice(&self.capture_ts.to_be_bytes());
        out[14..16].copy_from_slice(&(self.stream.width as u16).to_be_bytes());
        out[16..18].copy_from_slice(&(self.stream.height as u16).to_be_bytes());
        out[18] = self.stream.format.wire_code();
        out[19..23].copy_from_slice(&(self.stream.byte_len as u32).to_be_bytes());
        out[23..25].copy_from_slice(&self.chunk_index.to_be_bytes());
        out[25..27].copy_from_slice(&self.total_chunks.to_be_bytes());
        out[27..29].copy_from_slice(&self.chunk_len.to_be_bytes());
        out[29] = self.flags;
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_BYTES || u16::from_be_bytes([bytes[0], bytes[1]]) != MAGIC {
            return None;
        }
        let format = PixelFormat::from_wire_code(bytes[18])?;
        let stream = StreamSpec::new(
            u16::from_be_bytes([bytes[14], bytes[15]]) as u32,
            u16::from_be_bytes([bytes[16], bytes[17]]) as u32,
            format,
        )
        .ok()?;
        if stream.byte_len != u32::from_be_bytes(bytes[19..23].try_into().ok()?) as usize {
            return None;
        }
        Some(Self {
            frame_id: u32::from_be_bytes(bytes[2..6].try_into().ok()?),
            capture_ts: u64::from_be_bytes(bytes[6..14].try_into().ok()?),
            stream,
            chunk_index: u16::from_be_bytes([bytes[23], bytes[24]]),
            total_chunks: u16::from_be_bytes([bytes[25], bytes[26]]),
            chunk_len: u16::from_be_bytes([bytes[27], bytes[28]]),
            flags: bytes[29],
        })
    }
}

pub struct FrameEcho {
    pub frame_id: u32,
    pub capture_ts: u64,
    pub spread_us: u32,
    pub decompress_us: u32,
}

impl FrameEcho {
    pub fn encode(&self, out: &mut [u8; ECHO_BYTES]) {
        out[0..4].copy_from_slice(&self.frame_id.to_be_bytes());
        out[4..12].copy_from_slice(&self.capture_ts.to_be_bytes());
        out[12..16].copy_from_slice(&self.spread_us.to_be_bytes());
        out[16..20].copy_from_slice(&self.decompress_us.to_be_bytes());
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ECHO_BYTES {
            return None;
        }
        Some(Self {
            frame_id: u32::from_be_bytes(bytes[0..4].try_into().ok()?),
            capture_ts: u64::from_be_bytes(bytes[4..12].try_into().ok()?),
            spread_us: u32::from_be_bytes(bytes[12..16].try_into().ok()?),
            decompress_us: u32::from_be_bytes(bytes[16..20].try_into().ok()?),
        })
    }
}
