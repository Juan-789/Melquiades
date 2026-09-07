//! Binary representations used by Melquiades transports.
//!
//! `PacketHeader` is the H.264 access-unit protocol. The older Deflate
//! transport remains available as `LegacyRawPacketHeader` only until codec
//! integration replaces it. Keeping their names distinct prevents accidental
//! raw-frame metadata from leaking into the H.264 protocol.

use crate::capture::{PixelFormat, StreamSpec};
use crate::config::{ECHO_BYTES, LEGACY_RAW_HEADER_BYTES, LEGACY_RAW_MAGIC};

/// A recognizable Melquiades datagram prefix. It is a protocol signature
/// (often called a "magic value"), not an authentication mechanism.
pub const SIGNATURE: [u8; 4] = *b"JUAN";
/// Version one carries fragments of complete H.264 access units.
pub const VERSION: u8 = 1;
pub const HEADER_BYTES: usize = 14;

/// The encoded frame is independently decodable (an H.264 IDR/keyframe).
pub const FLAG_KEYFRAME: u8 = 0b0000_0001;

/// H.264 access-unit fragment metadata repeated on every UDP datagram.
///
/// The payload has no length field: it is every byte after `HEADER_BYTES` in
/// the UDP datagram. A missing fragment makes the entire access unit invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketHeader {
    pub flags: u8,
    pub frame_id: u32,
    pub chunk_index: u16,
    pub total_chunks: u16,
}

impl PacketHeader {
    pub const BYTES: usize = HEADER_BYTES;

    pub fn encode(&self, out: &mut [u8]) {
        assert!(
            out.len() >= Self::BYTES,
            "packet header output is too small"
        );
        out[0..4].copy_from_slice(&SIGNATURE);
        out[4] = VERSION;
        out[5] = self.flags;
        out[6..10].copy_from_slice(&self.frame_id.to_be_bytes());
        out[10..12].copy_from_slice(&self.chunk_index.to_be_bytes());
        out[12..14].copy_from_slice(&self.total_chunks.to_be_bytes());
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::BYTES || bytes[0..4] != SIGNATURE || bytes[4] != VERSION {
            return None;
        }

        let total_chunks = u16::from_be_bytes([bytes[12], bytes[13]]);
        let chunk_index = u16::from_be_bytes([bytes[10], bytes[11]]);
        if total_chunks == 0 || chunk_index >= total_chunks {
            return None;
        }

        Some(Self {
            flags: bytes[5],
            frame_id: u32::from_be_bytes(bytes[6..10].try_into().ok()?),
            chunk_index,
            total_chunks,
        })
    }

    /// Parses one received datagram and returns its H.264 fragment bytes.
    /// The payload length is intentionally derived from the datagram length.
    pub fn split_datagram(datagram: &[u8]) -> Option<(Self, &[u8])> {
        let header = Self::decode(datagram)?;
        let payload = datagram.get(Self::BYTES..)?;
        (!payload.is_empty()).then_some((header, payload))
    }
}

/// Raw-frame/Deflate header used only by the currently active transport.
///
/// It will be removed when the transport begins carrying `EncodedFrame`s.
// [magic][frame_id][capture_ts][width][height][format][raw_len]
// [chunk_index][total_chunks][chunk_len][flags]
pub struct LegacyRawPacketHeader {
    pub frame_id: u32,
    pub capture_ts: u64,
    pub stream: StreamSpec,
    pub chunk_index: u16,
    pub total_chunks: u16,
    pub chunk_len: u16,
    pub flags: u8,
}

impl LegacyRawPacketHeader {
    pub const BYTES: usize = LEGACY_RAW_HEADER_BYTES;

    pub fn encode(&self, out: &mut [u8]) {
        out[0..2].copy_from_slice(&LEGACY_RAW_MAGIC.to_be_bytes());
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
        if bytes.len() < Self::BYTES || u16::from_be_bytes([bytes[0], bytes[1]]) != LEGACY_RAW_MAGIC
        {
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
    /// Time spent turning the complete media payload into displayable pixels.
    /// It is Deflate decompression today and becomes H.264 decode time later.
    pub decode_us: u32,
}

impl FrameEcho {
    pub fn encode(&self, out: &mut [u8; ECHO_BYTES]) {
        out[0..4].copy_from_slice(&self.frame_id.to_be_bytes());
        out[4..12].copy_from_slice(&self.capture_ts.to_be_bytes());
        out[12..16].copy_from_slice(&self.spread_us.to_be_bytes());
        out[16..20].copy_from_slice(&self.decode_us.to_be_bytes());
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ECHO_BYTES {
            return None;
        }
        Some(Self {
            frame_id: u32::from_be_bytes(bytes[0..4].try_into().ok()?),
            capture_ts: u64::from_be_bytes(bytes[4..12].try_into().ok()?),
            spread_us: u32::from_be_bytes(bytes[12..16].try_into().ok()?),
            decode_us: u32::from_be_bytes(bytes[16..20].try_into().ok()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{FLAG_KEYFRAME, HEADER_BYTES, PacketHeader, SIGNATURE, VERSION};

    #[test]
    fn h264_header_round_trips_and_exposes_payload_remainder() {
        let header = PacketHeader {
            flags: FLAG_KEYFRAME,
            frame_id: 42,
            chunk_index: 2,
            total_chunks: 5,
        };
        let mut datagram = [0_u8; HEADER_BYTES + 3];
        header.encode(&mut datagram);
        datagram[HEADER_BYTES..].copy_from_slice(&[9, 8, 7]);

        let (decoded, payload) = PacketHeader::split_datagram(&datagram).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(payload, [9, 8, 7]);
        assert_eq!(&datagram[0..4], &SIGNATURE);
        assert_eq!(datagram[4], VERSION);
    }

    #[test]
    fn h264_header_rejects_invalid_signature_version_and_chunk_range() {
        let header = PacketHeader {
            flags: 0,
            frame_id: 7,
            chunk_index: 0,
            total_chunks: 1,
        };
        let mut bytes = [0_u8; HEADER_BYTES];
        header.encode(&mut bytes);

        bytes[0] = b'X';
        assert!(PacketHeader::decode(&bytes).is_none());

        header.encode(&mut bytes);
        bytes[4] = VERSION.wrapping_add(1);
        assert!(PacketHeader::decode(&bytes).is_none());

        header.encode(&mut bytes);
        bytes[10..12].copy_from_slice(&1_u16.to_be_bytes());
        assert!(PacketHeader::decode(&bytes).is_none());
    }
}
