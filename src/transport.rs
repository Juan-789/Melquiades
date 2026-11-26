use std::io::Write;
use std::net::UdpSocket;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::time::Instant;

use crate::capture::{FrameSource, StreamSpec};
#[cfg(target_os = "linux")]
use crate::codec::linux::LinuxH264Decoder as H264Decoder;
#[cfg(target_os = "windows")]
use crate::codec::windows::WindowsH264Decoder as H264Decoder;
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
use crate::wire::{
    FLAG_KEYFRAME, FLAG_PARAMETER_SETS, FrameEcho, LegacyRawPacketHeader, PacketHeader,
};

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

/// Timing from the first UDP datagram accepted by the local socket through
/// the final datagram accepted for one H.264 access unit.
///
/// This measures the sender-side burst that the network and receiver must
/// absorb. It is deliberately not an end-to-end delivery measurement: UDP
/// provides no delivery acknowledgement.
#[derive(Clone, Copy, Debug)]
pub struct H264SendReport {
    pub packets: u16,
    pub first_datagram_accepted: Instant,
    pub final_datagram_accepted: Instant,
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
    /// Packet pacing is intentionally absent while we measure the natural
    /// burst shape. `H264SendReport` makes that burst observable before any
    /// pacing policy is introduced.
    pub fn send_access_unit(&mut self, frame: &EncodedFrame) -> std::io::Result<H264SendReport> {
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

        let mut flags = 0;
        if frame.is_keyframe {
            flags |= FLAG_KEYFRAME;
        }
        if frame.has_parameter_sets {
            flags |= FLAG_PARAMETER_SETS;
        }
        let mut first_datagram_accepted = None;
        let mut final_datagram_accepted = None;
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
            )
            .map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "H.264 UDP send failed: frame={} keyframe={} bytes={} chunk={}/{}: {error}",
                        frame.frame_id,
                        frame.is_keyframe,
                        frame.bytes.len(),
                        index,
                        total_chunks,
                    ),
                )
            })?;
            let accepted_at = Instant::now();
            first_datagram_accepted.get_or_insert(accepted_at);
            final_datagram_accepted = Some(accepted_at);
            if index + 1 < total_chunks {
                pace_after_send(accepted_at);
            }
        }
        Ok(H264SendReport {
            packets: total_chunks as u16,
            // `total_chunks > 0` was checked above, so both timestamps were
            // assigned in the loop unless sending returned the error above.
            first_datagram_accepted: first_datagram_accepted
                .expect("non-empty H.264 access unit must send a first datagram"),
            final_datagram_accepted: final_datagram_accepted
                .expect("non-empty H.264 access unit must send a final datagram"),
        })
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
    pub frame_id: u32,
    pub display_channel_drops: u64,
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
    let mut display_channel_drops = 0_u64;

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
                            frame_id: reassembler.frame_id,
                            display_channel_drops,
                        };
                        if matches!(
                            tx.try_send(frame),
                            Err(std::sync::mpsc::TrySendError::Full(_))
                        ) {
                            display_channel_drops += 1;
                        }
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
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;

    socket.set_recv_buffer_size(4 * 1024 * 1024)?;

    eprintln!(
        "Diagnostic UDP receive buffer: {} bytes",
        socket.recv_buffer_size()?
    );

    socket.bind(&"0.0.0.0:5000".parse::<std::net::SocketAddr>()?.into())?;

    let socket: UdpSocket = socket.into();

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

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn bind_h264_display(addr: &str) -> Result<UdpSocket, Box<dyn std::error::Error>> {
    const RECEIVE_BUFFER_BYTES: usize = 4 * 1024 * 1024;
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_recv_buffer_size(RECEIVE_BUFFER_BYTES)?;
    let effective_buffer_bytes = socket.recv_buffer_size()?;
    eprintln!(
        "H.264 UDP receive buffer: requested={RECEIVE_BUFFER_BYTES} bytes, effective={effective_buffer_bytes} bytes"
    );
    socket.bind(&addr.parse::<std::net::SocketAddr>()?.into())?;
    Ok(socket.into())
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn receiving_h264_to_display(
    socket: UdpSocket,
    tx: SyncSender<ReceivedFrame>,
    mut on_frame: impl FnMut(),
) -> Result<(), Box<dyn std::error::Error>> {
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut reassembler = H264Reassembler::new();
    let mut decoder = None;
    let mut peer = None;
    let mut retired_peer = None;
    let mut last_accepted = Instant::now();
    let mut startup_at = None;
    let mut skipped_while_waiting = 0_u64;

    let telemetry_started = Instant::now();
    let mut telemetry_since = Instant::now();
    let mut packets_received = 0_u64;
    let mut bytes_received = 0_u64;
    let mut completed_access_units = 0_u64;
    let mut lost_access_units = 0_u64;
    let mut late_or_old_packets = 0_u64;
    let mut decode_calls = 0_u64;
    let mut decode_pictures = 0_u64;
    let mut decode_none = 0_u64;
    let mut decode_errors = 0_u64;
    let mut decode_total_ms = 0.0_f64;
    let mut decode_max_ms = 0.0_f64;
    let mut channel_sent = 0_u64;
    let mut channel_full_drops = 0_u64;
    let mut last_packet_at: Option<Instant> = None;
    let mut max_packet_gap_ms = 0.0_f64;

    eprintln!("waiting for h264 on {}", socket.local_addr()?);
    loop {
        let (received, source) = socket.recv_from(&mut datagram)?;
        let packet_at = Instant::now();
        packets_received += 1;
        bytes_received += received as u64;
        if let Some(previous) = last_packet_at {
            max_packet_gap_ms =
                max_packet_gap_ms.max(packet_at.duration_since(previous).as_secs_f64() * 1000.0);
        }
        last_packet_at = Some(packet_at);
        let Some((header, payload)) = PacketHeader::split_datagram(&datagram[..received]) else {
            eprintln!("malformed H.264 packet: {received} bytes");
            continue;
        };

        let recovery_flags = FLAG_KEYFRAME | FLAG_PARAMETER_SETS;
        let recovery = header.flags & recovery_flags == recovery_flags;
        let idle = packet_at.duration_since(last_accepted) >= std::time::Duration::from_secs(1);
        let restarted = idle && recovery && header.frame_id < reassembler.frame_id;
        if peer != Some(source) || restarted {
            if peer.is_some() && (!recovery || (retired_peer == Some(source) && !idle)) {
                continue;
            }
            if peer != Some(source) {
                retired_peer = peer;
            }
            peer = Some(source);
            startup_at = Some(packet_at);
            telemetry_since = packet_at;
            eprintln!(
                "first stream packet from {source}, frame {}",
                header.frame_id
            );
            reassembler = H264Reassembler::new();
            decoder = None;
            skipped_while_waiting = 0;
        }
        if reassembler.total_chunks == 0 || header.frame_id != reassembler.frame_id {
            if reassembler.total_chunks != 0 && !reassembler.is_newer_frame(header.frame_id) {
                late_or_old_packets += 1;
                continue;
            }
            if reassembler.total_chunks != 0 {
                let absent = header.frame_id.wrapping_sub(reassembler.frame_id) - 1;
                if absent > 0 {
                    lost_access_units += u64::from(absent);
                    decoder = None;
                }
            }
            if reassembler.total_chunks > 0 && reassembler.missing() > 0 {
                lost_access_units += 1;
                eprintln!(
                    "H.264 frame {} lost: {} of {} packets missing; waiting for a recovery keyframe",
                    reassembler.frame_id,
                    reassembler.missing(),
                    reassembler.total_chunks,
                );
                decoder = None;
            }
            if !reassembler.reset(&header) {
                eprintln!(
                    "refused H.264 frame {}: {} packets exceed the safety limit",
                    header.frame_id, header.total_chunks
                );
                decoder = None;
                continue;
            }
        }
        last_accepted = packet_at;
        if !reassembler.add(&header, payload) {
            continue;
        }
        completed_access_units += 1;

        let is_keyframe = reassembler.flags & FLAG_KEYFRAME != 0;
        let has_parameter_sets = reassembler.flags & FLAG_PARAMETER_SETS != 0;
        if decoder.is_none() {
            if !is_keyframe || !has_parameter_sets {
                skipped_while_waiting += 1;
                continue;
            }
            decoder = Some(H264Decoder::new()?);
            eprintln!(
                "H.264 recovery at keyframe {} after skipping {skipped_while_waiting} access units",
                reassembler.frame_id
            );
            skipped_while_waiting = 0;
        }

        let t0_complete = Instant::now();
        let t1_decode_begins = Instant::now();
        let decoded = decoder
            .as_mut()
            .unwrap()
            .decode_access_unit(&reassembler.buf[..reassembler.bytes]);
        let t2_decode_ends = Instant::now();
        decode_calls += 1;
        let decode_ms = t2_decode_ends
            .duration_since(t1_decode_begins)
            .as_secs_f64()
            * 1000.0;
        decode_total_ms += decode_ms;
        decode_max_ms = decode_max_ms.max(decode_ms);
        match decoded {
            Ok(Some(frame)) => {
                decode_pictures += 1;
                if let Some(start) = startup_at.take() {
                    eprintln!(
                        "first picture {} decoded {:.2} ms after first stream packet",
                        reassembler.frame_id,
                        t2_decode_ends.duration_since(start).as_secs_f64() * 1000.0
                    );
                }
                match tx.try_send(ReceivedFrame {
                    pixels: frame.pixels,
                    stream: frame.stream,
                    timings: FrameTimings {
                        t0_compressed_frame_complete: t0_complete,
                        t1_decode_begins,
                        t2_decode_ends,
                    },
                    frame_id: reassembler.frame_id,
                    display_channel_drops: channel_full_drops,
                }) {
                    Ok(()) => {
                        channel_sent += 1;
                        on_frame();
                    }
                    Err(std::sync::mpsc::TrySendError::Full(_)) => channel_full_drops += 1,
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        return Err("display frame channel disconnected".into());
                    }
                }
            }
            Ok(None) => {
                decode_none += 1;
                decoder = None;
                eprintln!(
                    "H.264 decoder produced no picture for frame {}; waiting for a keyframe",
                    reassembler.frame_id
                );
            }
            Err(error) => {
                decode_errors += 1;
                decoder = None;
                eprintln!(
                    "H.264 decode failed for frame {}: {error}; waiting for a keyframe",
                    reassembler.frame_id
                );
            }
        }

        if telemetry_since.elapsed() >= std::time::Duration::from_secs(2) {
            let window_s = telemetry_since.elapsed().as_secs_f64();
            let uptime_s = telemetry_started.elapsed().as_secs_f64();
            eprintln!(
                "[{uptime_s:8.3}s] H264 RX: packets={:.1}/s bytes={:.2}MB/s complete={:.1}fps lost={} old_pkts={} waiting={} skipped={} max_packet_gap={:.2}ms | decode calls={} pictures={} none={} errors={} avg={:.2}ms max={:.2}ms | channel sent={} full_drop={}",
                packets_received as f64 / window_s,
                bytes_received as f64 / window_s / 1_000_000.0,
                completed_access_units as f64 / window_s,
                lost_access_units,
                late_or_old_packets,
                decoder.is_none(),
                skipped_while_waiting,
                max_packet_gap_ms,
                decode_calls,
                decode_pictures,
                decode_none,
                decode_errors,
                decode_total_ms / decode_calls.max(1) as f64,
                decode_max_ms,
                channel_sent,
                channel_full_drops,
            );
            telemetry_since = Instant::now();
            packets_received = 0;
            bytes_received = 0;
            completed_access_units = 0;
            lost_access_units = 0;
            late_or_old_packets = 0;
            decode_calls = 0;
            decode_pictures = 0;
            decode_none = 0;
            decode_errors = 0;
            decode_total_ms = 0.0;
            decode_max_ms = 0.0;
            channel_sent = 0;
            channel_full_drops = 0;
            max_packet_gap_ms = 0.0;
        }
    }
}
