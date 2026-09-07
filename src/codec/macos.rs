//! macOS H.264 encoding through VideoToolbox.
//!
//! This is intentionally an encode-only boundary. It proves that a
//! ScreenCaptureKit-provided `IOSurface` can produce one H.264 access unit.
//! UDP packetization and decoder setup remain separate steps.

use std::{
    error::Error,
    sync::{
        Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use apple_cf::iosurface::IOSurface;
use screencapturekit::prelude::{
    CMSampleBuffer, CMSampleBufferExt, PixelFormat as ScreenPixelFormat, SCContentFilter,
    SCShareableContent, SCStream, SCStreamConfiguration, SCStreamOutputTrait, SCStreamOutputType,
};
use videotoolbox::prelude::{Codec as VideoToolboxCodec, CompressionSession};

use crate::{
    capture::{PixelFormat, StreamSpec},
    codec::{Codec, EncodedFrame},
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
            .with_average_bit_rate(average_bit_rate)
            .with_expected_frame_rate(frames_per_second as f64)
            .with_max_keyframe_interval(keyframe_interval)
            .build()?;

        Ok(Self {
            stream,
            frames_per_second,
            session,
            next_pts: 0,
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
        let (is_keyframe, has_parameter_sets) = inspect_length_prefixed_h264(&output.data);

        Ok(EncodedFrame {
            frame_id,
            codec: Codec::H264,
            source_pts_us,
            is_keyframe,
            has_parameter_sets,
            bytes: output.data,
        })
    }
}

/// Parses AVCC length-prefixed NAL units only for metadata. The bytes remain
/// untouched; packetization will carry the entire access unit.
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

#[cfg(test)]
mod tests {
    use super::inspect_length_prefixed_h264;

    #[test]
    fn identifies_idr_and_parameter_sets_in_avcc() {
        let bytes = [
            0, 0, 0, 1, 0x67, // SPS
            0, 0, 0, 1, 0x68, // PPS
            0, 0, 0, 1, 0x65, // IDR
        ];
        assert_eq!(inspect_length_prefixed_h264(&bytes), (true, true));
    }
}
