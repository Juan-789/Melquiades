use std::time::Instant;

use crate::capture::StreamSpec;
use crate::config::{MAX_CHUNK_PAYLOAD, MAX_COMPRESSED_FRAME_BYTES};
use crate::wire::{LegacyRawPacketHeader, PacketHeader};

/// One in-progress compressed frame. Storage grows only when a newly accepted
/// stream needs more space; normal frames reuse both this byte buffer and the
/// packet-received bitmap.
pub struct Reassembler {
    pub frame_id: u32,
    pub stream: Option<StreamSpec>,
    pub buf: Vec<u8>,
    received: Vec<u64>,
    count: u16,
    pub total_chunks: u16,
    pub bytes: usize,
    pub first_chunk_at: Option<Instant>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self {
            frame_id: u32::MAX,
            stream: None,
            buf: Vec::new(),
            received: Vec::new(),
            count: 0,
            total_chunks: 0,
            bytes: 0,
            first_chunk_at: None,
        }
    }

    /// Starts assembling `header`'s frame. `false` means its claimed packet
    /// count would exceed the receiver's explicit memory safety limit.
    pub fn reset(&mut self, header: &LegacyRawPacketHeader) -> bool {
        let total_chunks = header.total_chunks as usize;
        let compressed_capacity = match total_chunks.checked_mul(MAX_CHUNK_PAYLOAD) {
            Some(bytes) if total_chunks > 0 && bytes <= MAX_COMPRESSED_FRAME_BYTES => bytes,
            _ => return false,
        };

        self.frame_id = header.frame_id;
        self.stream = Some(header.stream);
        self.received.clear();
        self.received.resize(total_chunks.div_ceil(64), 0);
        self.count = 0;
        self.total_chunks = header.total_chunks;
        self.bytes = 0;
        self.first_chunk_at = None;
        if self.buf.len() < compressed_capacity {
            self.buf.resize(compressed_capacity, 0);
        }
        true
    }

    pub fn add(&mut self, header: &LegacyRawPacketHeader, payload: &[u8]) -> bool {
        let index = header.chunk_index as usize;
        if self.stream != Some(header.stream)
            || header.total_chunks != self.total_chunks
            || index >= self.total_chunks as usize
            || payload.len() > MAX_CHUNK_PAYLOAD
        {
            return false;
        }

        let word = index / 64;
        let bit = 1_u64 << (index % 64);
        if self.received[word] & bit != 0 {
            return false;
        }
        if self.count == 0 {
            self.first_chunk_at = Some(Instant::now());
        }
        let offset = index * MAX_CHUNK_PAYLOAD;
        let Some(end) = offset.checked_add(payload.len()) else {
            return false;
        };
        if end > self.buf.len() {
            return false;
        }
        self.buf[offset..end].copy_from_slice(payload);
        self.received[word] |= bit;
        self.count += 1;
        self.bytes = self.bytes.max(end);
        self.count == self.total_chunks
    }

    pub fn missing(&self) -> u16 {
        self.total_chunks.saturating_sub(self.count)
    }

    pub fn is_newer_frame(&self, candidate: u32) -> bool {
        candidate != self.frame_id && candidate.wrapping_sub(self.frame_id) < (1_u32 << 31)
    }
}

/// Reassembles one H.264 access unit carried by the JUAN v1 protocol.
///
/// Unlike [`Reassembler`], this has no raw image shape or Deflate metadata:
/// its bytes are opaque until a video decoder receives the completed access
/// unit. A missing fragment invalidates the entire unit.
pub struct H264Reassembler {
    pub frame_id: u32,
    pub flags: u8,
    pub buf: Vec<u8>,
    received: Vec<u64>,
    count: u16,
    pub total_chunks: u16,
    pub bytes: usize,
    pub first_chunk_at: Option<Instant>,
}

impl H264Reassembler {
    pub fn new() -> Self {
        Self {
            frame_id: u32::MAX,
            flags: 0,
            buf: Vec::new(),
            received: Vec::new(),
            count: 0,
            total_chunks: 0,
            bytes: 0,
            first_chunk_at: None,
        }
    }

    /// Starts a new access unit. The same explicit byte cap that protects the
    /// legacy receiver also bounds malformed H.264 packet headers.
    pub fn reset(&mut self, header: &PacketHeader) -> bool {
        let total_chunks = header.total_chunks as usize;
        let capacity = match total_chunks.checked_mul(MAX_CHUNK_PAYLOAD) {
            Some(bytes) if total_chunks > 0 && bytes <= MAX_COMPRESSED_FRAME_BYTES => bytes,
            _ => return false,
        };

        self.frame_id = header.frame_id;
        self.flags = header.flags;
        self.received.clear();
        self.received.resize(total_chunks.div_ceil(64), 0);
        self.count = 0;
        self.total_chunks = header.total_chunks;
        self.bytes = 0;
        self.first_chunk_at = None;
        if self.buf.len() < capacity {
            self.buf.resize(capacity, 0);
        }
        true
    }

    /// Adds one H.264 fragment and reports whether it completed the unit.
    pub fn add(&mut self, header: &PacketHeader, payload: &[u8]) -> bool {
        let index = header.chunk_index as usize;
        if header.frame_id != self.frame_id
            || header.flags != self.flags
            || header.total_chunks != self.total_chunks
            || index >= self.total_chunks as usize
            || payload.is_empty()
            || payload.len() > MAX_CHUNK_PAYLOAD
            // PacketHeader deliberately omits a payload length. Therefore
            // packet offsets are unambiguous only when every non-final
            // fragment is exactly MAX_CHUNK_PAYLOAD bytes.
            || (index + 1 < self.total_chunks as usize && payload.len() != MAX_CHUNK_PAYLOAD)
        {
            return false;
        }

        let word = index / 64;
        let bit = 1_u64 << (index % 64);
        if self.received[word] & bit != 0 {
            return false;
        }
        if self.count == 0 {
            self.first_chunk_at = Some(Instant::now());
        }
        let offset = index * MAX_CHUNK_PAYLOAD;
        let Some(end) = offset.checked_add(payload.len()) else {
            return false;
        };
        if end > self.buf.len() {
            return false;
        }

        self.buf[offset..end].copy_from_slice(payload);
        self.received[word] |= bit;
        self.count += 1;
        self.bytes = self.bytes.max(end);
        self.count == self.total_chunks
    }

    pub fn missing(&self) -> u16 {
        self.total_chunks.saturating_sub(self.count)
    }

    pub fn is_newer_frame(&self, candidate: u32) -> bool {
        candidate != self.frame_id && candidate.wrapping_sub(self.frame_id) < (1_u32 << 31)
    }
}

#[cfg(test)]
mod h264_tests {
    use super::H264Reassembler;
    use crate::wire::{FLAG_KEYFRAME, PacketHeader};

    #[test]
    fn reassembles_h264_access_unit_out_of_order() {
        let mut reassembler = H264Reassembler::new();
        let last = PacketHeader {
            flags: FLAG_KEYFRAME,
            frame_id: 9,
            chunk_index: 1,
            total_chunks: 2,
        };
        let first = PacketHeader {
            chunk_index: 0,
            ..last
        };
        assert!(reassembler.reset(&first));
        assert!(!reassembler.add(&last, b"world"));
        let first_payload = vec![b'a'; crate::config::MAX_CHUNK_PAYLOAD];
        assert!(reassembler.add(&first, &first_payload));
        assert_eq!(reassembler.bytes, crate::config::MAX_CHUNK_PAYLOAD + 5);
        assert_eq!(
            &reassembler.buf[..crate::config::MAX_CHUNK_PAYLOAD],
            first_payload.as_slice()
        );
        assert_eq!(
            &reassembler.buf[crate::config::MAX_CHUNK_PAYLOAD..reassembler.bytes],
            b"world"
        );
        assert_eq!(reassembler.missing(), 0);
    }
}
