use std::io::Write;
use std::net::UdpSocket;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::time::Instant;

use crate::capture::{FrameSource, StreamSpec};
use crate::codec::{Codec, EncodedFrame};
use crate::compression::{compress, decompress};
use crate::config::{
    DATAGRAM_MAX, ECHO_BYTES, INTER_PACKET_GAP_US, LEGACY_FLAG_COMPRESSED, LEGACY_RAW_HEADER_BYTES,
    MAX_CHUNK_PAYLOAD,
};
use crate::metrics::{CompressionStats, FrameTimings, ReassemblyStats, SenderStats, SenderTimings};
use crate::pipeline::{CapturePort, Pipeline, SenderPort};
use crate::reassembly::{H264Reassembler, Reassembler};
use crate::time::now_nanos;
use crate::wire::{FLAG_KEYFRAME, FrameEcho, LegacyRawPacketHeader, PacketHeader};

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

/// Keep the next packet out of the kernel until the configured gap has passed.
/// A spin wait is intentional here: `thread::sleep` is far too imprecise for a
/// 25 microsecond experiment and would measure scheduler wake-up latency too.
fn pace_after_send(sent_at: Instant) {
    let next_send_at = sent_at + std::time::Duration::from_micros(INTER_PACKET_GAP_US);
    while Instant::now() < next_send_at {
        std::hint::spin_loop();
    }
}

/// Connected UDP sender for complete H.264 access units.
///
/// This keeps H.264 packetization separate from the legacy raw/Deflate
/// sender. Its datagrams use `PacketHeader` (the `JUAN` signature), not the
/// older 30-byte raw-frame header.
pub struct H264UdpSender {
    socket: UdpSocket,
    datagram: Vec<u8>,
}

impl H264UdpSender {
    pub fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect(addr)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            datagram: vec![0; DATAGRAM_MAX],
        })
    }

    /// Fragments and sends exactly one complete H.264 access unit.
    ///
    /// Packet pacing is intentionally absent in this first codec transport:
    /// the encoder reduced normal frames to only a few datagrams. We will
    /// measure packet loss before adding pacing back as a transport policy.
    pub fn send_access_unit(&mut self, frame: &EncodedFrame) -> std::io::Result<u16> {
        if frame.codec != Codec::H264 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "H.264 UDP sender received a non-H.264 frame",
            ));
        }
        let total_chunks = frame.bytes.len().div_ceil(MAX_CHUNK_PAYLOAD);
        if total_chunks == 0 || total_chunks > u16::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "H.264 access unit of {} bytes needs {total_chunks} chunks",
                    frame.bytes.len()
                ),
            ));
        }

        let flags = if frame.is_keyframe { FLAG_KEYFRAME } else { 0 };
        for (index, payload) in frame.bytes.chunks(MAX_CHUNK_PAYLOAD).enumerate() {
            let header = PacketHeader {
                flags,
                frame_id: frame.frame_id,
                chunk_index: index as u16,
                total_chunks: total_chunks as u16,
            };
            header.encode(&mut self.datagram[..PacketHeader::BYTES]);
            self.datagram[PacketHeader::BYTES..PacketHeader::BYTES + payload.len()]
                .copy_from_slice(payload);
            send_datagram(
                &self.socket,
                &self.datagram[..PacketHeader::BYTES + payload.len()],
            )?;
        }
        Ok(total_chunks as u16)
    }
}

pub fn streaming<S>(source: S, addr: &str) -> Result<(), Box<dyn std::error::Error>>
where
    S: FrameSource + Send + 'static,
{
    let stream = source.stream_spec();
    let (capture, sender) = Pipeline::new(stream).into_ports();
    let capture_thread = thread::Builder::new()
        .name("melquiades-capture".into())
        .spawn(move || {
            if let Err(error) = capture_loop(source, &capture) {
                capture.stop();
                eprintln!("capture stage stopped: {error}");
            }
        })?;

    // Sender runs on the caller's thread. It is concurrent with the dedicated
    // capture thread above, but avoids a third coordinating thread.
    let result = stream_from_sender(sender, addr, stream);
    drop(capture_thread);
    result
}

/// Runs the sender half of a pipeline whose capture half is driven by an
/// asynchronous source such as a PipeWire process callback.
///
/// Unlike [`streaming`], this function does not create a capture thread. The
/// caller owns that independently-running producer and must have constructed
/// both ports from the same [`Pipeline`].
pub fn stream_from_sender(
    sender: SenderPort,
    addr: &str,
    stream: StreamSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    sender_loop(sender, addr, stream)
}

fn capture_loop(
    mut source: impl FrameSource,
    capture: &CapturePort,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        capture.capture_once(&mut source)?;
    }
}

fn sender_loop(
    sender: SenderPort,
    addr: &str,
    stream: StreamSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(addr)?;
    socket.set_nonblocking(true)?;
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut echo_bytes = [0; ECHO_BYTES];
    let mut compression_stats = CompressionStats::new();
    let mut sender_stats = SenderStats::new();
    let mut frame_id = 0_u32;

    eprintln!("inter-packet pacing: {INTER_PACKET_GAP_US}us (spin wait)");

    loop {
        let Some(id) = sender.take_newest() else {
            if !sender.capture_is_running() {
                return Err("capture stage stopped".into());
            }
            thread::yield_now();
            continue;
        };

        let send_result = sender.with_frame(&id, |frame_info, frame_bytes| {
            validate_stream_format(frame_info.stream_spec()?, stream)?;
            let capture_ts = now_nanos();
            let s2_compression_begins = Instant::now();
            let compressed = compress(frame_bytes)?;
            let s3_compression_ends = Instant::now();
            let total_chunks = compressed.len().div_ceil(MAX_CHUNK_PAYLOAD);
            if total_chunks > u16::MAX as usize {
                eprintln!("frame {} too large: {} chunks", frame_id, total_chunks);
                return Ok::<(), Box<dyn std::error::Error>>(());
            }
            compression_stats.record(
                frame_info.byte_len,
                compressed.len(),
                total_chunks,
                LEGACY_RAW_HEADER_BYTES,
                s3_compression_ends.duration_since(s2_compression_begins),
            );
            let mut first_datagram_accepted = None;
            let mut final_datagram_accepted = None;
            for (index, chunk) in compressed.chunks(MAX_CHUNK_PAYLOAD).enumerate() {
                let header = LegacyRawPacketHeader {
                    frame_id,
                    capture_ts,
                    stream,
                    chunk_index: index as u16,
                    total_chunks: total_chunks as u16,
                    chunk_len: chunk.len() as u16,
                    flags: LEGACY_FLAG_COMPRESSED,
                };
                header.encode(&mut datagram[..LEGACY_RAW_HEADER_BYTES]);
                datagram[LEGACY_RAW_HEADER_BYTES..LEGACY_RAW_HEADER_BYTES + chunk.len()]
                    .copy_from_slice(chunk);
                send_datagram(&socket, &datagram[..LEGACY_RAW_HEADER_BYTES + chunk.len()])?;
                let accepted_at = Instant::now();
                first_datagram_accepted.get_or_insert(accepted_at);
                final_datagram_accepted = Some(accepted_at);
                if index + 1 < total_chunks {
                    pace_after_send(accepted_at);
                }
            }
            sender_stats.record(&SenderTimings {
                s0_capture_begins: frame_info.capture_begins_at,
                s1_frame_acquired: frame_info.captured_at,
                s2_compression_begins,
                s3_compression_ends,
                s4_first_datagram_accepted: first_datagram_accepted
                    .expect("a compressed frame must contain at least one datagram"),
                s5_final_datagram_accepted: final_datagram_accepted
                    .expect("a compressed frame must contain at least one datagram"),
            });
            Ok::<(), Box<dyn std::error::Error>>(())
        });
        sender.return_to_free(id);
        send_result?;

        if frame_id % 300 == 299 {
            let handoff = sender.take_snapshot();
            eprintln!(
                "capture-to-sender handoff over 300 sent frames: capture_no_free_drops={} sender_stale_drops={}",
                handoff.capture_dropped_no_free_slot, handoff.sender_dropped_stale_ready,
            );
        }

        while let Ok(received) = socket.recv(&mut echo_bytes) {
            // Drain receipts so they do not accumulate. Cross-machine and delayed-read
            // timing is intentionally not reported as one-way latency.
            let _ = FrameEcho::decode(&echo_bytes[..received]);
        }
        frame_id = frame_id.wrapping_add(1);
    }
}

/// The source declares one format for a run. Catch a backend accidentally
/// changing its capture shape before it can corrupt the fixed pool layout.
fn validate_stream_format(
    frame: StreamSpec,
    configured: StreamSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    if frame != configured {
        return Err(format!(
            "source changed stream shape from {configured:?} to {frame:?}; recreate the pipeline for a new resolution"
        )
        .into());
    }
    Ok(())
}

pub struct ReceivedFrame {
    pub pixels: Vec<u8>,
    pub stream: StreamSpec,
    pub timings: FrameTimings,
}

pub fn receiving(tx: Option<SyncSender<ReceivedFrame>>) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:5000")?;
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut reassembler = Reassembler::new();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let mut decompressed = Vec::new();
    let mut echo_bytes = [0; ECHO_BYTES];
    let mut reassembly_stats = ReassemblyStats::new();

    loop {
        let (received, source) = socket.recv_from(&mut datagram)?;
        let header = match LegacyRawPacketHeader::decode(&datagram[..received]) {
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
            if !reassembler.reset(&header) {
                eprintln!(
                    "refused frame {}: {} chunks exceed the reassembly safety limit",
                    header.frame_id, header.total_chunks
                );
                continue;
            }
        }
        let payload_end = LEGACY_RAW_HEADER_BYTES + header.chunk_len as usize;
        if payload_end > received {
            eprintln!(
                "truncated packet: claims {} bytes, got {}",
                payload_end, received
            );
            continue;
        }
        if !reassembler.add(&header, &datagram[LEGACY_RAW_HEADER_BYTES..payload_end]) {
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
        let decode_us = t2_decode_ends
            .duration_since(t1_decode_begins)
            .as_micros()
            .min(u32::MAX as u128) as u32;
        match result {
            Ok(()) => {
                if decompressed.len() != header.stream.byte_len {
                    eprintln!(
                        "frame {} wrong size: {}, expected {}",
                        header.frame_id,
                        decompressed.len(),
                        header.stream.byte_len,
                    );
                    continue;
                }
                FrameEcho {
                    frame_id: header.frame_id,
                    capture_ts: header.capture_ts,
                    spread_us,
                    decode_us,
                }
                .encode(&mut echo_bytes);
                let _ = socket.send_to(&echo_bytes, source);
                match &tx {
                    Some(tx) => {
                        let frame = ReceivedFrame {
                            pixels: decompressed.clone(),
                            stream: header.stream,
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

/// Receives and validates H.264 access units without decoding them yet.
///
/// This command is the safe UDP integration checkpoint. It proves that the
/// Mac H.264 sender and Linux transport agree on fragmentation, frame IDs,
/// flags, and reassembly before a decoder is asked to process network input.
pub fn receiving_h264() -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:5000")?;
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut reassembler = H264Reassembler::new();
    let mut completed = 0_u64;
    let mut dropped = 0_u64;

    eprintln!("waiting for JUAN H.264 access units on UDP port 5000");
    loop {
        let (received, _) = socket.recv_from(&mut datagram)?;
        let Some((header, payload)) = PacketHeader::split_datagram(&datagram[..received]) else {
            eprintln!("malformed H.264 packet: {received} bytes");
            continue;
        };

        if header.frame_id != reassembler.frame_id {
            if !reassembler.is_newer_frame(header.frame_id) {
                continue;
            }
            if reassembler.total_chunks > 0 && reassembler.missing() > 0 {
                dropped += 1;
                eprintln!(
                    "H.264 frame {} dropped: {} of {} packets missing",
                    reassembler.frame_id,
                    reassembler.missing(),
                    reassembler.total_chunks
                );
            }
            if !reassembler.reset(&header) {
                eprintln!(
                    "refused H.264 frame {}: {} packets exceed the safety limit",
                    header.frame_id, header.total_chunks
                );
                continue;
            }
        }

        if !reassembler.add(&header, payload) {
            continue;
        }

        completed += 1;
        let spread_us = reassembler
            .first_chunk_at
            .expect("a completed H.264 frame has a first packet time")
            .elapsed()
            .as_micros();
        eprintln!(
            "H.264 frame {} complete: {} bytes in {} packets, keyframe={}, spread={}us (completed={completed}, dropped={dropped})",
            reassembler.frame_id,
            reassembler.bytes,
            reassembler.total_chunks,
            reassembler.flags & FLAG_KEYFRAME != 0,
            spread_us,
        );
    }
}
