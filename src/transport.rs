use std::io::Write;
use std::net::UdpSocket;
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use crate::capture::{FrameSlot, FrameSource, PixelFormat};
use crate::compression::{compress, decompress};
use crate::config::{
    CHUNKS_PER_FRAME, DATAGRAM_MAX, ECHO_BYTES, FLAG_COMPRESSED, FRAME_SIZE, HEADER_BYTES,
    MAX_CHUNK_PAYLOAD,
};
use crate::metrics::{CompressionStats, FrameTimings, ReassemblyStats, SenderStats, SenderTimings};
use crate::reassembly::Reassembler;
use crate::time::now_nanos;
use crate::wire::{FrameEcho, PacketHeader};

fn send_datagram(socket: &UdpSocket, datagram: &[u8]) -> std::io::Result<()> {
    loop {
        match socket.send(datagram) {
            Ok(sent) if sent == datagram.len() => return Ok(()),
            Ok(sent) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    format!("UDP send wrote {sent} of {} bytes", datagram.len()),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
}

pub fn streaming(
    source: &mut impl FrameSource,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(addr)?;
    socket.set_nonblocking(true)?;
    let mut frame = FrameSlot::new(FRAME_SIZE);
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut echo_bytes = [0; ECHO_BYTES];
    let mut compression_stats = CompressionStats::new();
    let mut sender_stats = SenderStats::new();
    let mut frame_id = 0_u32;

    loop {
        let s0_capture_begins = Instant::now();
        let frame_info = source.next_frame(&mut frame)?;
        let s1_frame_acquired = frame_info.captured_at;
        validate_current_stream_format(
            frame_info.width,
            frame_info.height,
            frame_info.format,
            frame_info.byte_len,
        )?;
        let frame_bytes = frame.bytes(frame_info.byte_len)?;
        let capture_ts = now_nanos();
        let s2_compression_begins = Instant::now();
        let compressed = compress(frame_bytes)?;
        let s3_compression_ends = Instant::now();
        let total_chunks = compressed.len().div_ceil(MAX_CHUNK_PAYLOAD);
        if total_chunks > CHUNKS_PER_FRAME {
            eprintln!("frame {} too large: {} chunks", frame_id, total_chunks);
            continue;
        }
        compression_stats.record(
            frame_info.byte_len,
            compressed.len(),
            total_chunks,
            HEADER_BYTES,
            s3_compression_ends.duration_since(s2_compression_begins),
        );
        let mut first_datagram_accepted = None;
        let mut final_datagram_accepted = None;
        for (index, chunk) in compressed.chunks(MAX_CHUNK_PAYLOAD).enumerate() {
            let header = PacketHeader {
                frame_id,
                capture_ts,
                chunk_index: index as u16,
                total_chunks: total_chunks as u16,
                chunk_len: chunk.len() as u16,
                flags: FLAG_COMPRESSED,
            };
            header.encode(&mut datagram[..HEADER_BYTES]);
            datagram[HEADER_BYTES..HEADER_BYTES + chunk.len()].copy_from_slice(chunk);
            send_datagram(&socket, &datagram[..HEADER_BYTES + chunk.len()])?;
            let accepted_at = Instant::now();
            first_datagram_accepted.get_or_insert(accepted_at);
            final_datagram_accepted = Some(accepted_at);
        }
        sender_stats.record(&SenderTimings {
            s0_capture_begins,
            s1_frame_acquired,
            s2_compression_begins,
            s3_compression_ends,
            s4_first_datagram_accepted: first_datagram_accepted
                .expect("a compressed frame must contain at least one datagram"),
            s5_final_datagram_accepted: final_datagram_accepted
                .expect("a compressed frame must contain at least one datagram"),
        });
        while let Ok(received) = socket.recv(&mut echo_bytes) {
            // Drain receipts so they do not accumulate. Cross-machine and delayed-read
            // timing is intentionally not reported as one-way latency.
            let _ = FrameEcho::decode(&echo_bytes[..received]);
        }
        frame_id = frame_id.wrapping_add(1);
    }
}

/// The capture boundary is format-aware, while the current wire format and
/// receiver are intentionally still fixed to the first YUYV experiment.
/// A later protocol revision will carry this metadata alongside each frame.
fn validate_current_stream_format(
    width: u32,
    height: u32,
    format: PixelFormat,
    byte_len: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if width != crate::config::WIDTH as u32
        || height != crate::config::HEIGHT as u32
        || format != PixelFormat::Yuyv422
        || byte_len != FRAME_SIZE
    {
        return Err(format!(
            "current transport accepts only {}x{} YUYV frames of {FRAME_SIZE} bytes; source produced {width}x{height} {format:?} with {byte_len} bytes",
            crate::config::WIDTH,
            crate::config::HEIGHT,
        )
        .into());
    }
    Ok(())
}

pub struct ReceivedFrame {
    pub pixels: Vec<u8>,
    pub timings: FrameTimings,
}

pub fn receiving(tx: Option<SyncSender<ReceivedFrame>>) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:5000")?;
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut reassembler = Reassembler::new();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let mut decompressed = Vec::with_capacity(FRAME_SIZE);
    let mut echo_bytes = [0; ECHO_BYTES];
    let mut reassembly_stats = ReassemblyStats::new();

    loop {
        let (received, source) = socket.recv_from(&mut datagram)?;
        let header = match PacketHeader::decode(&datagram[..received]) {
            Some(header) => header,
            None => {
                eprintln!("malformed packet: {} bytes", received);
                continue;
            }
        };
        if header.frame_id != reassembler.frame_id {
            if !reassembler.is_newer_frame(header.frame_id) {
                reassembly_stats.record_late_packet();
                continue;
            }
            if reassembler.total_chunks > 0 && reassembler.missing() > 0 {
                reassembly_stats.record_drop(reassembler.missing());
                eprintln!(
                    "frame {} dropped: {} of {} chunks missing",
                    reassembler.frame_id,
                    reassembler.missing(),
                    reassembler.total_chunks
                );
            }
            reassembler.reset(header.frame_id);
        }
        let payload_end = HEADER_BYTES + header.chunk_len as usize;
        if payload_end > received {
            eprintln!(
                "truncated packet: claims {} bytes, got {}",
                payload_end, received
            );
            continue;
        }
        if !reassembler.add(&header, &datagram[HEADER_BYTES..payload_end]) {
            continue;
        }

        let t0_compressed_frame_complete = Instant::now();
        let first_chunk_at = reassembler
            .first_chunk_at
            .expect("a complete frame must have a first chunk timestamp");
        reassembly_stats.record_complete(first_chunk_at, t0_compressed_frame_complete);
        let spread_us = t0_compressed_frame_complete
            .duration_since(first_chunk_at)
            .as_micros()
            .min(u32::MAX as u128) as u32;
        let t1_decode_begins = Instant::now();
        let result = decompress(&reassembler.buf[..reassembler.bytes], &mut decompressed);
        let t2_decode_ends = Instant::now();
        let decompress_us = t2_decode_ends
            .duration_since(t1_decode_begins)
            .as_micros()
            .min(u32::MAX as u128) as u32;
        match result {
            Ok(()) => {
                if decompressed.len() != FRAME_SIZE {
                    eprintln!(
                        "frame {} wrong size: {}",
                        header.frame_id,
                        decompressed.len()
                    );
                    continue;
                }
                FrameEcho {
                    frame_id: header.frame_id,
                    capture_ts: header.capture_ts,
                    spread_us,
                    decompress_us,
                }
                .encode(&mut echo_bytes);
                let _ = socket.send_to(&echo_bytes, source);
                match &tx {
                    Some(tx) => {
                        let frame = ReceivedFrame {
                            pixels: decompressed.clone(),
                            timings: FrameTimings {
                                t0_compressed_frame_complete,
                                t1_decode_begins,
                                t2_decode_ends,
                            },
                        };
                        let _ = tx.try_send(frame);
                    }
                    None => {
                        output.write_all(&decompressed)?;
                        output.flush()?;
                    }
                }
            }
            Err(error) => eprintln!(
                "decompress failed for frame {}: {}",
                reassembler.frame_id, error
            ),
        }
    }
}
