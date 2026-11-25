use std::{
    error::Error,
    sync::mpsc::{SyncSender, sync_channel},
    thread,
};

use crate::pipeline::{Pipeline, SenderPort};
use crate::{
    capture::{PixelFormat, StreamSpec},
    codec::{Codec, EncodedFrame},
    metrics::H264SendStats,
    transport::H264UdpSender,
};
use openh264::{
    OpenH264API,
    decoder::Decoder,
    encoder::{BitRate, Encoder, EncoderConfig, FrameRate, IntraFramePeriod, UsageType},
    formats::{BgraSliceU8, YUVBuffer, YUVSource},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// An owned displayable image decoded from one complete H.264 access unit.
pub struct DecodedH264Frame {
    pub pixels: Vec<u8>,
    pub stream: StreamSpec,
}

/// Persistent OpenH264 decoder plus reusable conversion storage.
/// OpenH264 exposes borrowed YUV420 planes, valid only until the next decode.
/// We convert them before returning, so no decoder-owned pointer crosses into
/// the window thread.
pub struct LinuxH264Decoder {
    decoder: Decoder,
    bgra: Vec<u8>,
}

impl LinuxH264Decoder {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            decoder: Decoder::new()?,
            bgra: Vec::new(),
        })
    }

    /// Decodes one complete Annex B access unit.
    ///
    /// A `None` result is normal while the decoder is waiting for a complete
    /// parameter-set/keyframe combination. The UDP receiver enforces that it
    /// starts after a keyframe containing SPS/PPS, rather than treating this
    /// as a displayed black frame.
    pub fn decode_access_unit(
        &mut self,
        annex_b_access_unit: &[u8],
    ) -> Result<Option<DecodedH264Frame>, Box<dyn Error>> {
        let Some(yuv) = self.decoder.decode(annex_b_access_unit)? else {
            return Ok(None);
        };
        let (width, height) = yuv.dimensions();
        let stream = StreamSpec::new(width as u32, height as u32, PixelFormat::Bgra8888)?;
        self.bgra.resize(stream.byte_len, 0);

        // OpenH264's optimized helper writes RGBA. The existing renderer's
        // Bgra8888 path expects BGRA, so exchange R and B in-place after the
        // required YUV420 -> RGB conversion.
        yuv.write_rgba8(&mut self.bgra);
        for pixel in self.bgra.chunks_exact_mut(4) {
            pixel.swap(0, 2);
        }

        // The display channel owns its frame independently of the decoder.
        // This copy is explicit and temporary; an output FramePool is the
        // future optimization that will remove it without leaking decoder
        // borrows across threads.
        Ok(Some(DecodedH264Frame {
            pixels: self.bgra.clone(),
            stream,
        }))
    }
}

pub struct LinuxH264Encoder {
    encoder: Encoder,
    next_frame_id: u32,
}

impl LinuxH264Encoder {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            encoder: Encoder::with_api_config(
                OpenH264API::from_source(),
                EncoderConfig::new()
                    .bitrate(BitRate::from_bps(8_000_000))
                    .max_frame_rate(FrameRate::from_hz(30.0))
                    .usage_type(UsageType::ScreenContentRealTime)
                    .intra_frame_period(IntraFramePeriod::from_num_frames(30)),
            )?,
            next_frame_id: 0,
        })
    }

    pub fn encode(&mut self, frame: &impl YUVSource) -> Result<Vec<u8>, openh264::Error> {
        Ok(self.encoder.encode(frame)?.to_vec())
    }

    /// Call from the encoding worker, never from the PipeWire callback:
    /// sending blocks when the bounded queue is full.
    pub fn encode_and_send(
        &mut self,
        frame: &impl YUVSource,
        source_pts_us: u64,
        outbound: &SyncSender<EncodedFrame>,
    ) -> Result<(), Box<dyn Error>> {
        let bytes = self.encode(frame)?;
        if bytes.is_empty() {
            return Ok(()); // OpenH264 may skip a frame.
        }
        let mut is_keyframe = false;
        let mut has_sps = false;
        let mut has_pps = false;
        for nal in openh264::nal_units(&bytes) {
            let header = nal
                .strip_prefix(&[0, 0, 0, 1])
                .or_else(|| nal.strip_prefix(&[0, 0, 1]))
                .and_then(|payload| payload.first());
            match header.map(|byte| byte & 0x1f) {
                Some(5) => is_keyframe = true,
                Some(7) => has_sps = true,
                Some(8) => has_pps = true,
                _ => {}
            }
        }
        outbound.send(EncodedFrame {
            frame_id: self.next_frame_id,
            codec: Codec::H264,
            source_pts_us,
            is_keyframe,
            has_parameter_sets: has_sps && has_pps,
            bytes,
        })?;
        self.next_frame_id = self.next_frame_id.wrapping_add(1);
        Ok(())
    }
}

/// Drop every outbound sender before joining this worker.
fn start_h264_sender(
    receiver_addr: &str,
) -> Result<
    (
        SyncSender<EncodedFrame>,
        thread::JoinHandle<std::io::Result<()>>,
    ),
    Box<dyn Error>,
> {
    let mut sender = H264UdpSender::connect(receiver_addr)?;
    let (outbound, encoded_frames) = sync_channel::<EncodedFrame>(2);
    let worker = thread::Builder::new()
        .name("melquiades-h264-udp".into())
        .spawn(move || {
            let mut stats = H264SendStats::new();
            let started = Instant::now();
            let mut report_since = Instant::now();
            let mut frames_sent = 0_u64;
            let mut bytes_sent = 0_u64;
            let mut packets_sent = 0_u64;
            let mut send_total_ms = 0.0_f64;
            let mut send_max_ms = 0.0_f64;
            // Preserve encoded frame order: later frames may reference earlier ones.
            while let Ok(frame) = encoded_frames.recv() {
                let send_started = Instant::now();
                let sent = sender.send_access_unit(&frame)?;
                let send_ms = send_started.elapsed().as_secs_f64() * 1000.0;
                frames_sent += 1;
                bytes_sent += frame.bytes.len() as u64;
                packets_sent += u64::from(sent.packets);
                send_total_ms += send_ms;
                send_max_ms = send_max_ms.max(send_ms);
                stats.record(
                    frame.frame_id,
                    frame.is_keyframe,
                    frame.bytes.len(),
                    sent.packets,
                    sent.first_datagram_accepted,
                    sent.final_datagram_accepted,
                );
                if report_since.elapsed() >= Duration::from_secs(2) {
                    let window_s = report_since.elapsed().as_secs_f64();
                    eprintln!(
                        "[{:.3}s] H264 UDP TX: frames={:.1}fps bytes={:.2}MB/s packets={:.1}/s send_avg={:.2}ms send_max={:.2}ms",
                        started.elapsed().as_secs_f64(),
                        frames_sent as f64 / window_s,
                        bytes_sent as f64 / window_s / 1_000_000.0,
                        packets_sent as f64 / window_s,
                        send_total_ms / frames_sent.max(1) as f64,
                        send_max_ms,
                    );
                    report_since = Instant::now();
                    frames_sent = 0;
                    bytes_sent = 0;
                    packets_sent = 0;
                    send_total_ms = 0.0;
                    send_max_ms = 0.0;
                }
            }
            Ok(())
        })?;
    Ok((outbound, worker))
}

/// Consumes only owned pool frames; PipeWire's callback never waits on encoding.
fn encode_captured_frames(
    frames: SenderPort,
    stream: StreamSpec,
    outbound: SyncSender<EncodedFrame>,
    mut encoder: LinuxH264Encoder,
) -> Result<(), Box<dyn Error>> {
    let mut yuv = YUVBuffer::new(stream.width as usize, stream.height as usize);
    let started = Instant::now();
    let mut next_frame = started;
    let mut have_frame = false;
    let mut report_since = Instant::now();
    let mut encoded_ticks = 0_u64;
    let mut fresh_frames = 0_u64;
    let mut repeated_frames = 0_u64;
    let mut conversion_total_ms = 0.0_f64;
    let mut conversion_max_ms = 0.0_f64;
    let mut encode_queue_total_ms = 0.0_f64;
    let mut encode_queue_max_ms = 0.0_f64;
    while frames.capture_is_running() {
        let now = Instant::now();
        if now < next_frame {
            thread::sleep((next_frame - now).min(Duration::from_millis(5)));
            continue;
        }
        if let Some(id) = frames.take_newest() {
            let conversion_started = Instant::now();
            frames.with_frame(&id, |_info, bytes| {
                yuv.read_bgra8(BgraSliceU8::new(
                    bytes,
                    (stream.width as usize, stream.height as usize),
                ));
            });
            frames.return_to_free(id);
            let conversion_ms = conversion_started.elapsed().as_secs_f64() * 1000.0;
            conversion_total_ms += conversion_ms;
            conversion_max_ms = conversion_max_ms.max(conversion_ms);
            fresh_frames += 1;
            have_frame = true;
        } else if have_frame {
            repeated_frames += 1;
        }
        if !have_frame {
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        // Portal screen capture may be damage-driven: a static desktop need
        // not produce a fresh PipeWire buffer every 33 ms. Encode the newest
        // retained image on every presentation tick so frame-count-based GOP
        // recovery and receiver playback still advance at a real 30 Hz.
        let timestamp = now.saturating_duration_since(started).as_micros() as u64;
        let encode_started = Instant::now();
        encoder.encode_and_send(&yuv, timestamp, &outbound)?;
        let encode_queue_ms = encode_started.elapsed().as_secs_f64() * 1000.0;
        encode_queue_total_ms += encode_queue_ms;
        encode_queue_max_ms = encode_queue_max_ms.max(encode_queue_ms);
        encoded_ticks += 1;
        next_frame = now + Duration::from_secs_f64(1.0 / 30.0);
        if report_since.elapsed() >= Duration::from_secs(2) {
            let window_s = report_since.elapsed().as_secs_f64();
            eprintln!(
                "[{:.3}s] H264 ENCODE: output={:.1}fps fresh={} repeated={} | BGRA->YUV avg={:.2}ms max={:.2}ms | encode+queue avg={:.2}ms max={:.2}ms",
                started.elapsed().as_secs_f64(),
                encoded_ticks as f64 / window_s,
                fresh_frames,
                repeated_frames,
                conversion_total_ms / fresh_frames.max(1) as f64,
                conversion_max_ms,
                encode_queue_total_ms / encoded_ticks.max(1) as f64,
                encode_queue_max_ms,
            );
            report_since = Instant::now();
            encoded_ticks = 0;
            fresh_frames = 0;
            repeated_frames = 0;
            conversion_total_ms = 0.0;
            conversion_max_ms = 0.0;
            encode_queue_total_ms = 0.0;
            encode_queue_max_ms = 0.0;
        }
    }
    Ok(())
}

pub async fn cast_h264(receiver_addr: &str) -> Result<(), Box<dyn Error>> {
    eprintln!("h264 cast: querying shareable displays");
    let authorized = crate::screen::ShareScreen::full_monitor()
        .authorize()
        .await?;
    let stream = authorized.stream_spec()?;
    if stream.width % 2 != 0 || stream.height % 2 != 0 {
        return Err("H.264 YUV420 capture requires even monitor dimensions".into());
    }
    let encoder = LinuxH264Encoder::new()?;
    let (capture, frames) = Pipeline::new(stream).into_ports();
    let remote = authorized.connect_pipewire(capture.clone(), stream)?;
    let (outbound, sending_thread) = start_h264_sender(receiver_addr)?;
    let stopping = Arc::new(AtomicBool::new(false));
    let encoder_stop = Arc::clone(&stopping);
    let encoding_thread = thread::Builder::new()
        .name("melquiades-h264-encoder".into())
        .spawn(move || {
            let result = encode_captured_frames(frames, stream, outbound, encoder)
                .map_err(|error| error.to_string());
            encoder_stop.store(true, Ordering::Release);
            result
        });
    let encoding_thread = match encoding_thread {
        Ok(worker) => worker,
        Err(error) => {
            capture.stop();
            let _ = sending_thread.join();
            return Err(error.into());
        }
    };
    let signal_stop = Arc::clone(&stopping);
    let signal_task = tokio::spawn(async move {
        let result = tokio::signal::ctrl_c().await;
        signal_stop.store(true, Ordering::Release);
        result
    });
    eprintln!(
        "H.264 capture: {}x{}, up to 30fps, 8 Mbps -> {receiver_addr}; press Ctrl-C to stop",
        stream.width, stream.height
    );
    let capture_result = remote.run_until(move || stopping.load(Ordering::Acquire));
    capture.stop();
    drop(remote);
    signal_task.abort();
    let (encoded, sent) =
        tokio::task::spawn_blocking(move || (encoding_thread.join(), sending_thread.join()))
            .await?;
    capture_result?;
    sent.map_err(|_| std::io::Error::other("H.264 sender thread panicked"))??;
    encoded
        .map_err(|_| std::io::Error::other("H.264 encoder thread panicked"))?
        .map_err(std::io::Error::other)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::UdpSocket, time::Duration};

    #[test]
    fn captured_bgra_frame_reaches_udp_receiver_and_decodes() {
        use crate::{
            capture::FrameInfo,
            reassembly::H264Reassembler,
            wire::{FLAG_KEYFRAME, FLAG_PARAMETER_SETS, PacketHeader},
        };
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let stream = StreamSpec::new(64, 64, PixelFormat::Bgra8888).unwrap();
        let (capture, frames) = Pipeline::new(stream).into_ports();
        let (outbound, sender) =
            start_h264_sender(&receiver.local_addr().unwrap().to_string()).unwrap();
        let encoder = LinuxH264Encoder::new().unwrap();
        let worker = thread::spawn(move || {
            encode_captured_frames(frames, stream, outbound, encoder).map_err(|e| e.to_string())
        });
        let now = Instant::now();
        // Red BGRx pixels, with padding to exercise PipeWire row packing.
        let stride = 64 * 4 + 16;
        let mut pixels = vec![0; stride * 64];
        for row in pixels.chunks_exact_mut(stride) {
            for pixel in row[..256].chunks_exact_mut(4) {
                pixel.copy_from_slice(&[0, 0, 255, 0]);
            }
        }
        capture
            .publish_strided(
                FrameInfo {
                    capture_begins_at: now,
                    captured_at: now,
                    width: 64,
                    height: 64,
                    format: PixelFormat::Bgra8888,
                    byte_len: stream.byte_len,
                },
                &pixels,
                stride,
            )
            .unwrap();
        let mut assembly = H264Reassembler::new();
        let mut datagram = [0; 2048];
        loop {
            let size = receiver.recv(&mut datagram).unwrap();
            let (header, payload) = PacketHeader::split_datagram(&datagram[..size]).unwrap();
            if assembly.frame_id != header.frame_id {
                assert!(assembly.reset(&header));
            }
            if assembly.add(&header, payload) {
                break;
            }
        }
        capture.stop();
        worker.join().unwrap().unwrap();
        sender.join().unwrap().unwrap();
        assert_eq!(
            assembly.flags & (FLAG_KEYFRAME | FLAG_PARAMETER_SETS),
            FLAG_KEYFRAME | FLAG_PARAMETER_SETS
        );
        let mut decoder = LinuxH264Decoder::new().unwrap();
        let decoded = decoder
            .decode_access_unit(&assembly.buf[..assembly.bytes])
            .unwrap()
            .unwrap();
        assert_eq!(decoded.stream, stream);
        let pixel = &decoded.pixels[..4];
        assert!(
            pixel[2] > 200 && pixel[0] < 40 && pixel[1] < 40,
            "wrong BGRA color: {pixel:?}"
        );
    }

    #[test]
    fn periodic_keyframes_include_decoder_parameter_sets() {
        let mut encoder = LinuxH264Encoder::new().unwrap();
        let pixels = vec![128; 64 * 64 * 4];
        let yuv = YUVBuffer::from_bgra8_source(BgraSliceU8::new(&pixels, (64, 64)));
        let (outbound, received) = sync_channel(2);
        let mut keyframes = 0;
        for frame_id in 0..65 {
            encoder
                .encode_and_send(&yuv, frame_id * 33_333, &outbound)
                .unwrap();
            if let Ok(frame) = received.try_recv() {
                if frame.is_keyframe {
                    assert!(frame.has_parameter_sets);
                    keyframes += 1;
                }
            }
        }
        assert!(
            keyframes >= 3,
            "expected periodic recovery keyframes, got {keyframes}"
        );
    }

    #[test]
    fn sender_delivers_frames_in_order_and_exits_when_channel_closes() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (outbound, worker) =
            start_h264_sender(&receiver.local_addr().unwrap().to_string()).unwrap();
        for frame_id in 0..3 {
            outbound
                .send(EncodedFrame {
                    frame_id,
                    codec: Codec::H264,
                    source_pts_us: u64::from(frame_id) * 33_333,
                    is_keyframe: frame_id == 0,
                    has_parameter_sets: false,
                    bytes: vec![0, 0, 0, 1, 0x41, frame_id as u8],
                })
                .unwrap();
        }
        drop(outbound);
        worker.join().unwrap().unwrap();
        let mut datagram = [0; 2048];
        for frame_id in 0..3 {
            let size = receiver.recv(&mut datagram).unwrap();
            assert!(datagram[..size].ends_with(&[0, 0, 0, 1, 0x41, frame_id]));
        }
    }
}
