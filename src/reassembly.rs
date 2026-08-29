use crate::config::{CHUNKS_PER_FRAME, FRAME_SIZE, MAX_CHUNK_PAYLOAD};
use crate::time::now_nanos;
use crate::wire::PacketHeader;

pub struct Reassembler {
    pub frame_id: u32,
    pub buf: Vec<u8>,
    received: [u64; 8],
    count: u16,
    pub total_chunks: u16,
    pub bytes: usize,
    pub first_chunk_ns: u64,
}

impl Reassembler {
    pub fn new() -> Self {
        Self {
            frame_id: u32::MAX,
            buf: vec![0; FRAME_SIZE],
            received: [0; 8],
            count: 0,
            total_chunks: 0,
            bytes: 0,
            first_chunk_ns: 0,
        }
    }

    pub fn reset(&mut self, frame_id: u32) {
        self.frame_id = frame_id;
        self.received = [0; 8];
        self.count = 0;
        self.total_chunks = 0;
        self.bytes = 0;
        self.first_chunk_ns = 0;
    }

    pub fn add(&mut self, header: &PacketHeader, payload: &[u8]) -> bool {
        let index = header.chunk_index as usize;
        if index >= CHUNKS_PER_FRAME {
            return false;
        }
        let word = index / 64;
        let bit = 1_u64 << (index % 64);
        if self.received[word] & bit != 0 {
            return false;
        }
        if self.count == 0 {
            self.first_chunk_ns = now_nanos();
        }
        let offset = index * MAX_CHUNK_PAYLOAD;
        self.buf[offset..offset + payload.len()].copy_from_slice(payload);
        self.received[word] |= bit;
        self.count += 1;
        self.total_chunks = header.total_chunks;
        self.bytes = self.bytes.max(offset + payload.len());
        self.count == self.total_chunks
    }

    pub fn missing(&self) -> u16 {
        self.total_chunks.saturating_sub(self.count)
    }
}
