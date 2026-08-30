use std::io::Write;
use std::net::UdpSocket;
use std::sync::mpsc::SyncSender;

use crate::capture::Capture;
use crate::compression::{compress, decompress};
use crate::config::{
    CHUNKS_PER_FRAME, DATAGRAM_MAX, ECHO_BYTES, FLAG_COMPRESSED, FRAME_SIZE, HEADER_BYTES,
    MAX_CHUNK_PAYLOAD,
};
use crate::metrics::LatencyStats;
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

pub fn streaming(capture: &mut impl Capture, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(addr)?;
    socket.set_nonblocking(true)?;
    let mut frame = vec![0; FRAME_SIZE];
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut echo_bytes = [0; ECHO_BYTES];
    let mut stats = LatencyStats::new();
    let mut frame_id = 0_u32;

    loop {
        capture.next_frame(&mut frame)?;
        let capture_ts = now_nanos();
        let compressed = compress(&frame)?;
        let total_chunks = compressed.len().div_ceil(MAX_CHUNK_PAYLOAD);
        if total_chunks > CHUNKS_PER_FRAME {
            eprintln!("frame {} too large: {} chunks", frame_id, total_chunks);
            continue;
        }
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
        }
        while let Ok(received) = socket.recv(&mut echo_bytes) {
            if let Some(echo) = FrameEcho::decode(&echo_bytes[..received]) {
                let _telemetry = (echo.frame_id, echo.spread_us, echo.decompress_us);
                stats.record((now_nanos() - echo.capture_ts) as f64 / 2_000_000.0);
            }
        }
        frame_id = frame_id.wrapping_add(1);
    }
}

pub fn receiving(tx: Option<SyncSender<Vec<u8>>>) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:5000")?;
    let mut datagram = vec![0; DATAGRAM_MAX];
    let mut reassembler = Reassembler::new();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let mut decompressed = Vec::with_capacity(FRAME_SIZE);
    let mut echo_bytes = [0; ECHO_BYTES];

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
            if reassembler.total_chunks > 0 && reassembler.missing() > 0 {
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

        let spread_us = ((now_nanos() - reassembler.first_chunk_ns) / 1_000) as u32;
        let started = now_nanos();
        let result = decompress(&reassembler.buf[..reassembler.bytes], &mut decompressed);
        let decompress_us = ((now_nanos() - started) / 1_000) as u32;
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
                        let _ = tx.try_send(decompressed.clone());
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
