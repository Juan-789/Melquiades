//! macOS H.264 encoding through VideoToolbox.
//!
//! This is intentionally an encode-only boundary. It proves that a
//! ScreenCaptureKit-provided `IOSurface` can produce one H.264 access unit.
//! UDP packetization and decoder setup remain separate steps.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};

use apple_cf::iosurface::IOSurface;
use apple_cf::raw::CMVideoFormatDescriptionGetH264ParameterSetAtIndex;
use screencapturekit::prelude::{
    CMSampleBuffer, CMSampleBufferExt, PixelFormat as ScreenPixelFormat, SCContentFilter,
    SCShareableContent, SCStream, SCStreamConfiguration, SCStreamOutputTrait, SCStreamOutputType,
};
use videotoolbox::{
    compression::EncodedFrame as VideoToolboxEncodedFrame,
    prelude::{Codec as VideoToolboxCodec, CompressionSession, ProfileLevel},
};

use crate::{
    capture::{PixelFormat, StreamSpec},
    codec::{Codec, EncodedFrame},
    metrics::H264SendStats,
    transport::H264UdpSender,
};

const BGRA_FOURCC: u32 = u32::from_be_bytes(*b"BGRA");

/// A persistent, real-time H.264 encoder for one fixed BGRA stream shape.
///
/// ScreenCaptureKit owns a reusable IOSurface pool for its frames. Taking the
/// IOSurface directly from its `CVPixelBuffer` avoids an 8 MB CPU copy at
/// 1920x1080, while this `CompressionSession` stays alive across frames.
pub struct MacH264Encoder {
    stream: StreamSpec,
    frames_per_second: i32,
    session: CompressionSession,
    next_pts: i64,
    /// VideoToolbox stores SPS/PPS in `CMFormatDescription`, rather than in
    /// the AVCC access-unit bytes it returns. They do not change for one
    /// fixed session, so retaining them once avoids querying CoreMedia for
    /// every frame and lets every IDR be independently decodable on the wire.
    parameter_sets: Option<Vec<Vec<u8>>>,
}

impl MacH264Encoder {
    /// Builds a low-latency H.264 session for a fixed BGRA stream.
    pub fn new(
        stream: StreamSpec,
        frames_per_second: i32,
        average_bit_rate: i32,
        keyframe_interval: i32,
    ) -> Result<Self, Box<dyn Error>> {
        if stream.format != PixelFormat::Bgra8888 {
            return Err("the macOS H.264 encoder currently accepts BGRA8888 only".into());
        }
        if frames_per_second <= 0 || average_bit_rate <= 0 || keyframe_interval <= 0 {
            return Err("frame rate, bit rate, and keyframe interval must be positive".into());
        }

        let width = i32::try_from(stream.width)?;
        let height = i32::try_from(stream.height)?;
        let session = CompressionSession::builder(width, height, VideoToolboxCodec::H264)
            .with_real_time(true)
            .with_allow_frame_reordering(false)
            // OpenH264 is deliberately conservative about decoder features.
            // Requesting constrained baseline avoids VideoToolbox selecting a
            // more advanced profile that the Linux baseline decoder may not
            // accept, while AutoLevel still selects the needed H.264 level
            // for this resolution and frame rate.
            .with_profile_level(ProfileLevel::H264ConstrainedBaselineAutoLevel)
            .with_average_bit_rate(average_bit_rate)
            .with_expected_frame_rate(frames_per_second as f64)
            .with_max_keyframe_interval(keyframe_interval)
            .build()?;

        Ok(Self {
            stream,
            frames_per_second,
            session,
            next_pts: 0,
            parameter_sets: None,
        })
    }

    /// Encodes a ScreenCaptureKit-provided BGRA IOSurface as one H.264 access
    /// unit. The supplied surface is borrowed only for this synchronous call.
    pub fn encode_surface(
        &mut self,
        frame_id: u32,
        source_pts_us: u64,
        surface: &IOSurface,
    ) -> Result<EncodedFrame, Box<dyn Error>> {
        if surface.width() != self.stream.width as usize
            || surface.height() != self.stream.height as usize
            || surface.pixel_format() != BGRA_FOURCC
        {
            return Err(format!(
                "unexpected IOSurface: {}x{} format={:#x}; expected {}x{} BGRA",
                surface.width(),
                surface.height(),
                surface.pixel_format(),
                self.stream.width,
                self.stream.height,
            )
            .into());
        }

        let output = self
            .session
            .encode(surface, (self.next_pts, self.frames_per_second))?;
        self.next_pts = self
            .next_pts
            .checked_add(1)
            .ok_or("presentation timestamp overflow")?;

        if output.data.is_empty() {
            return Err("VideoToolbox returned an empty H.264 access unit".into());
        }
        let (is_keyframe, source_has_parameter_sets) = inspect_length_prefixed_h264(&output.data);
        if self.parameter_sets.is_none() {
            self.parameter_sets = Some(parameter_sets_from_video_toolbox(&output)?);
        }
        let parameter_sets = if is_keyframe && !source_has_parameter_sets {
            self.parameter_sets.as_deref().unwrap_or_default()
        } else {
            &[]
        };
        let bytes = avcc_access_unit_to_annex_b(&output.data, parameter_sets)?;
        let has_parameter_sets = source_has_parameter_sets || !parameter_sets.is_empty();

        Ok(EncodedFrame {
            frame_id,
            codec: Codec::H264,
            source_pts_us,
            is_keyframe,
            has_parameter_sets,
            bytes,
        })
    }
}

/// Copies the SPS/PPS NAL units out of VideoToolbox's format description.
///
/// Apple owns the pointers returned by CoreMedia. We copy their contents while
/// the encoded sample buffer and its format description are retained, then
/// cache the owned vectors in `MacH264Encoder` for this fixed stream session.
fn parameter_sets_from_video_toolbox(
    output: &VideoToolboxEncodedFrame,
) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let sample = output
        .cm_sample_buffer()
        .ok_or("VideoToolbox output had no CoreMedia sample buffer")?;
    let description = sample
        .format_description()
        .ok_or("VideoToolbox output had no H.264 format description")?;
    if !description.is_h264() {
        return Err("VideoToolbox output format description was not H.264".into());
    }

    let mut sets = Vec::new();
    let mut count = 0_usize;
    let mut nal_header_length = 0_i32;
    for index in 0.. {
        let mut pointer = std::ptr::null();
        let mut byte_len = 0_usize;
        let status = unsafe {
            CMVideoFormatDescriptionGetH264ParameterSetAtIndex(
                description.as_ptr().cast(),
                index,
                &mut pointer,
                &mut byte_len,
                &mut count,
                &mut nal_header_length,
            )
        };
        if status != 0 {
            if index == 0 {
                return Err(format!(
                    "CoreMedia could not read H.264 parameter set 0 (status {status})"
                )
                .into());
            }
            break;
        }
        if pointer.is_null() || byte_len == 0 {
            return Err("CoreMedia returned an empty H.264 parameter set".into());
        }
        // SAFETY: CoreMedia owns `pointer` for as long as `description` is
        // retained above; we immediately copy exactly its reported byte span.
        sets.push(unsafe { std::slice::from_raw_parts(pointer, byte_len) }.to_vec());
        if index + 1 >= count {
            break;
        }
    }
    if sets.len() < 2 || nal_header_length != 4 {
        return Err(format!(
            "expected H.264 SPS/PPS with 4-byte AVCC lengths; got {} sets and {nal_header_length}-byte lengths",
            sets.len()
        )
        .into());
    }
    Ok(sets)
}

/// Converts VideoToolbox's AVCC length-prefixed NAL units into Annex B start
/// code units, the elementary-stream form OpenH264 accepts on Linux.
///
/// `parameter_sets` are copied before the first IDR when VideoToolbox kept
/// them solely in its format description. Their NAL headers are included in
/// each slice; this function adds only Annex B's `00 00 00 01` delimiter.
fn avcc_access_unit_to_annex_b(
    avcc: &[u8],
    parameter_sets: &[Vec<u8>],
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut annex_b = Vec::with_capacity(avcc.len() + parameter_sets.len() * 32);
    for set in parameter_sets {
        if set.is_empty() {
            return Err("cached H.264 parameter set was empty".into());
        }
        annex_b.extend_from_slice(&[0, 0, 0, 1]);
        annex_b.extend_from_slice(set);
    }

    let mut cursor = 0_usize;
    while cursor < avcc.len() {
        let length_end = cursor
            .checked_add(4)
            .ok_or("H.264 AVCC length offset overflow")?;
        if length_end > avcc.len() {
            return Err("truncated H.264 AVCC NAL length".into());
        }
        let nal_len = u32::from_be_bytes(avcc[cursor..length_end].try_into()?) as usize;
        cursor = length_end;
        let nal_end = cursor
            .checked_add(nal_len)
            .ok_or("H.264 AVCC NAL length overflow")?;
        if nal_len == 0 || nal_end > avcc.len() {
            return Err("invalid H.264 AVCC NAL length".into());
        }
        annex_b.extend_from_slice(&[0, 0, 0, 1]);
        annex_b.extend_from_slice(&avcc[cursor..nal_end]);
        cursor = nal_end;
    }
    if annex_b.is_empty() {
        return Err("H.264 Annex B access unit was empty".into());
    }
    Ok(annex_b)
}

/// Parses VideoToolbox's AVCC length-prefixed NAL units for metadata before
/// `avcc_access_unit_to_annex_b` converts the access unit for transmission.
fn inspect_length_prefixed_h264(bytes: &[u8]) -> (bool, bool) {
    let mut cursor: usize = 0;
    let mut is_keyframe = false;
    let mut has_parameter_sets = false;

    while cursor
        .checked_add(4)
        .is_some_and(|start| start <= bytes.len())
    {
        let nal_len = u32::from_be_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        cursor += 4;
        let Some(nal_end) = cursor.checked_add(nal_len) else {
            break;
        };
        if nal_len == 0 || nal_end > bytes.len() {
            break;
        }
        match bytes[cursor] & 0x1f {
            5 => is_keyframe = true,            // IDR slice
            7 | 8 => has_parameter_sets = true, // SPS or PPS
            _ => {}
        }
        cursor = nal_end;
    }
    (is_keyframe, has_parameter_sets)
}

struct H264SmokeHandler {
    encoder: Mutex<MacH264Encoder>,
    started_at: Instant,
    frames_encoded: AtomicU32,
}

/// ScreenCaptureKit's callback-side half of the network H.264 pipeline.
///
/// The callback keeps the source IOSurface and the encoder together, because
/// VideoToolbox consumes that IOSurface synchronously. It then moves the
/// resulting encoded `Vec<u8>` into a bounded queue. The UDP thread owns the
/// socket, so a slow socket never makes the capture callback wait. A full
/// queue deliberately drops the newly encoded frame: real-time video should
/// prefer the most recent image over growing latency.
struct H264CastHandler {
    encoder: Mutex<MacH264Encoder>,
    outbound: SyncSender<EncodedFrame>,
    sender_alive: Arc<AtomicBool>,
    started_at: Instant,
    next_frame_id: AtomicU32,
    encoded: AtomicU32,
    dropped_before_send: AtomicU32,
}

impl SCStreamOutputTrait for H264CastHandler {
    fn did_output_sample_buffer(&self, sample: CMSampleBuffer, of_type: SCStreamOutputType) {
        if of_type != SCStreamOutputType::Screen || !self.sender_alive.load(Ordering::Acquire) {
            return;
        }
        let Some(pixel_buffer) = sample.image_buffer() else {
            return;
        };
        let Some(surface) = pixel_buffer.io_surface() else {
            eprintln!("ScreenCaptureKit frame was not IOSurface-backed");
            return;
        };

        let frame_id = self.next_frame_id.fetch_add(1, Ordering::Relaxed);
        let source_pts_us = self.started_at.elapsed().as_micros() as u64;
        let frame = match self.encoder.lock() {
            Ok(mut encoder) => encoder.encode_surface(frame_id, source_pts_us, &surface),
            Err(_) => Err("H.264 encoder mutex poisoned".into()),
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                eprintln!("H.264 encode failed: {error}");
                return;
            }
        };

        match self.outbound.try_send(frame) {
            Ok(()) => {
                let encoded = self.encoded.fetch_add(1, Ordering::Relaxed) + 1;
                if encoded.is_multiple_of(300) {
                    eprintln!(
                        "H.264 capture: encoded={encoded}, dropped_before_send={}",
                        self.dropped_before_send.load(Ordering::Relaxed)
                    );
                }
            }
            Err(TrySendError::Full(_)) => {
                self.dropped_before_send.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.sender_alive.store(false, Ordering::Release);
                eprintln!("H.264 sender thread stopped; capture will stop encoding frames");
            }
        }
    }
}

impl SCStreamOutputTrait for H264SmokeHandler {
    fn did_output_sample_buffer(&self, sample: CMSampleBuffer, of_type: SCStreamOutputType) {
        if of_type != SCStreamOutputType::Screen {
            return;
        }
        let Some(pixel_buffer) = sample.image_buffer() else {
            return;
        };
        let Some(surface) = pixel_buffer.io_surface() else {
            eprintln!("ScreenCaptureKit frame was not IOSurface-backed");
            return;
        };

        let frame_id = self.frames_encoded.fetch_add(1, Ordering::Relaxed);
        let source_pts_us = self.started_at.elapsed().as_micros() as u64;
        let result = self
            .encoder
            .lock()
            .map_err(|_| "H.264 smoke encoder mutex poisoned".to_owned())
            .and_then(|mut encoder| {
                encoder
                    .encode_surface(frame_id, source_pts_us, &surface)
                    .map_err(|error| error.to_string())
            });

        match result {
            Ok(frame) => println!(
                "H.264 frame {}: {} bytes, keyframe={}, parameter_sets={}",
                frame.frame_id,
                frame.bytes.len(),
                frame.is_keyframe,
                frame.has_parameter_sets
            ),
            Err(error) => eprintln!("H.264 encode failed: {error}"),
        }
    }
}

/// Captures the local display at 640x360 and encodes its first few frames.
///
/// Run with `cargo run -- h264-smoke`. A successful result proves the first
/// useful integration seam: ScreenCaptureKit IOSurface -> persistent H.264
/// session. It neither sends UDP packets nor displays decoded video.
pub fn run_h264_smoke() -> Result<(), Box<dyn Error>> {
    let stream = StreamSpec::new(640, 360, PixelFormat::Bgra8888)?;
    eprintln!("h264 smoke: querying shareable displays");
    let content = SCShareableContent::get()?;
    let displays = content.displays();
    let display = displays.first().ok_or("no display was available")?;
    let filter = SCContentFilter::create()
        .with_display(display)
        .with_excluding_windows(&[])
        .build();
    let config = SCStreamConfiguration::new()
        .with_width(stream.width)
        .with_height(stream.height)
        .with_pixel_format(ScreenPixelFormat::BGRA)
        .with_fps(30);
    eprintln!("h264 smoke: building persistent VideoToolbox encoder");
    let encoder = MacH264Encoder::new(stream, 30, 2_000_000, 30)?;
    let mut capture = SCStream::new(&filter, &config);
    capture.add_output_handler(
        H264SmokeHandler {
            encoder: Mutex::new(encoder),
            started_at: Instant::now(),
            frames_encoded: AtomicU32::new(0),
        },
        SCStreamOutputType::Screen,
    );
    eprintln!("h264 smoke: starting 640x360 ScreenCaptureKit stream");
    capture.start_capture()?;
    eprintln!("h264 smoke: collecting frames for two seconds");
    std::thread::sleep(Duration::from_secs(2));
    eprintln!("h264 smoke: stopping stream");
    capture.stop_capture()?;
    Ok(())
}

/// Captures a macOS display, encodes it as H.264, and sends access units over
/// UDP using the JUAN v1 packet header.
///
/// The matching receiver is `Melquiades h264-recv` on the Linux host. It
/// currently proves packet reassembly only; displaying the stream is the next
/// milestone, after the Linux decoder and SPS/PPS handoff are added.
pub fn cast_h264(receiver_addr: &str) -> Result<(), Box<dyn Error>> {
    const WIDTH: u32 = 1920;
    const HEIGHT: u32 = 1080;
    const FPS: i32 = 30;
    const BIT_RATE: i32 = 8_000_000;
    const KEYFRAME_INTERVAL: i32 = 30;

    let stream_spec = StreamSpec::new(WIDTH, HEIGHT, PixelFormat::Bgra8888)?;
    eprintln!("h264 cast: querying shareable displays");
    let content = SCShareableContent::get()?;
    let displays = content.displays();
    let display = displays.first().ok_or("no display was available")?;
    let filter = SCContentFilter::create()
        .with_display(display)
        .with_excluding_windows(&[])
        .build();
    let config = SCStreamConfiguration::new()
        .with_width(WIDTH)
        .with_height(HEIGHT)
        .with_pixel_format(ScreenPixelFormat::BGRA)
        .with_fps(FPS as u32);

    eprintln!(
        "h264 cast: creating {WIDTH}x{HEIGHT} {FPS}fps VideoToolbox encoder at {} Mbps",
        BIT_RATE / 1_000_000
    );
    let encoder = MacH264Encoder::new(stream_spec, FPS, BIT_RATE, KEYFRAME_INTERVAL)?;

    // Two encoded access units gives the socket thread a little scheduling
    // room while bounding the latency that can accumulate behind it.
    let (outbound, encoded_frames) = sync_channel::<EncodedFrame>(2);
    let sender_alive = Arc::new(AtomicBool::new(true));
    let sender_running = Arc::clone(&sender_alive);
    let sender_addr = receiver_addr.to_owned();
    thread::Builder::new()
        .name("melquiades-h264-udp".into())
        .spawn(move || {
            let result = (|| -> Result<(), Box<dyn Error>> {
                let mut sender = H264UdpSender::connect(&sender_addr)?;
                let mut frames_sent = 0_u64;
                let mut total_bytes = 0_u64;
                let mut stale_before_send = 0_u64;
                let mut send_stats = H264SendStats::new();
                while let Ok(mut frame) = encoded_frames.recv() {
                    // The queue is FIFO, but real-time media should not be.
                    // Before committing socket work, retain the freshest
                    // already-encoded access unit and discard older queued
                    // units. This mirrors SenderPort::take_newest for raw
                    // frame-pool slots.
                    while let Ok(newer) = encoded_frames.try_recv() {
                        frame = newer;
                        stale_before_send += 1;
                    }
                    let sent = sender.send_access_unit(&frame)?;
                    send_stats.record(
                        frame.frame_id,
                        frame.is_keyframe,
                        frame.bytes.len(),
                        sent.packets,
                        sent.first_datagram_accepted,
                        sent.final_datagram_accepted,
                    );
                    frames_sent += 1;
                    total_bytes += frame.bytes.len() as u64;
                    if frames_sent.is_multiple_of(300) {
                        eprintln!(
                            "H.264 UDP: sent={frames_sent}, stale_before_send={stale_before_send}, mean_access_unit={} bytes, last={} bytes/{} packets, keyframe={}",
                            total_bytes / frames_sent,
                            frame.bytes.len(),
                            sent.packets,
                            frame.is_keyframe,
                        );
                    }
                }
                Ok(())
            })();
            sender_running.store(false, Ordering::Release);
            if let Err(error) = result {
                eprintln!("H.264 UDP sender stopped: {error}");
            }
        })?;

    let mut capture = SCStream::new(&filter, &config);
    capture.add_output_handler(
        H264CastHandler {
            encoder: Mutex::new(encoder),
            outbound,
            sender_alive,
            started_at: Instant::now(),
            next_frame_id: AtomicU32::new(0),
            encoded: AtomicU32::new(0),
            dropped_before_send: AtomicU32::new(0),
        },
        SCStreamOutputType::Screen,
    );
    eprintln!("h264 cast: sending to {receiver_addr}; press Ctrl-C to stop");
    capture.start_capture()?;
    thread::park();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{avcc_access_unit_to_annex_b, inspect_length_prefixed_h264};

    #[test]
    fn identifies_idr_and_parameter_sets_in_avcc() {
        let bytes = [
            0, 0, 0, 1, 0x67, // SPS
            0, 0, 0, 1, 0x68, // PPS
            0, 0, 0, 1, 0x65, // IDR
        ];
        assert_eq!(inspect_length_prefixed_h264(&bytes), (true, true));
    }

    #[test]
    fn converts_avcc_and_prepends_cached_parameter_sets() {
        let avcc = [
            0, 0, 0, 2, 0x65, 0x88, // IDR NAL
            0, 0, 0, 2, 0x41, 0x99, // non-IDR slice NAL
        ];
        let parameter_sets = vec![vec![0x67, 0x42], vec![0x68, 0xCE]];
        let annex_b = avcc_access_unit_to_annex_b(&avcc, &parameter_sets).unwrap();
        assert_eq!(
            annex_b,
            [
                0, 0, 0, 1, 0x67, 0x42, // SPS
                0, 0, 0, 1, 0x68, 0xCE, // PPS
                0, 0, 0, 1, 0x65, 0x88, // IDR
                0, 0, 0, 1, 0x41, 0x99, // slice
            ]
        );
    }
}
