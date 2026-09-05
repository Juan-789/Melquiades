//! macOS ScreenCaptureKit backend.
//!
//! This module intentionally contains only the public boundary for now. The
//! next implementation step is to replace this explicit error with a
//! ScreenCaptureKit stream whose callback calls the shared
//! `CapturePort::publish_strided` path.

use std::{error::Error, time::Instant};
use screencapturekit::prelude::{
    CMSampleBuffer,
    CMSampleBufferExt,
    PixelFormat as ScreenPixelFormat,
    SCContentFilter,
    SCShareableContent,
    SCStream,
    SCStreamConfiguration,
    SCStreamOutputTrait,
    SCStreamOutputType,
};
use crate::{capture::{
    FrameInfo,
    PixelFormat as FramePixelFormat, 
    StreamSpec
}, pipeline::{CapturePort, Pipeline}, transport::stream_from_sender};

const SCREEN_WIDTH: u32 = 1920;
const SCREEN_HEIGHT: u32 = 1080;
const BGRA_FOURCC: u32 = u32::from_be_bytes(*b"BGRA");

struct Handler{
    capture: CapturePort,
    expected_stream: StreamSpec,
}

// impl Handler{
//     fn new()
// }

impl SCStreamOutputTrait for Handler{
    fn did_output_sample_buffer(&self, sample_buffer: CMSampleBuffer, of_type: SCStreamOutputType){
        if of_type != SCStreamOutputType::Screen {
        // System audio output
        // Audio,
        // Microphone audio output (macOS 15.0+)
        //
        // When using microphone capture, this output type allows separate handling
        // of microphone audio from system audio.
        // Microphone,
            return;
        }
        let capture_begins = Instant::now();
        let Some(pixel_buffer) = sample_buffer.image_buffer() else {
            return;
        };
        if pixel_buffer.width() != self.expected_stream.width as usize
            || pixel_buffer.height() != self.expected_stream.height as usize
            || pixel_buffer.pixel_format() != BGRA_FOURCC
        {
            eprintln!(
                "unexpected screen frame: {}x{} format={:#x}",
                pixel_buffer.width(),
                pixel_buffer.height(),
                pixel_buffer.pixel_format(),
            );
            return;
        }
        let Ok(guard) = pixel_buffer.lock_read_only() else {
            eprintln!("could not lock macOS pixel buffer");
            return;
        };
        let frame_metadata = FrameInfo{
            capture_begins_at: capture_begins,
            width: self.expected_stream.width,
            height: self.expected_stream.height,
            format: self.expected_stream.format,
            byte_len: self.expected_stream.byte_len,
            captured_at: Instant::now(),
        };
        if let Err(error) = self.capture.publish_strided(frame_metadata, guard.as_slice(), pixel_buffer.bytes_per_row(),
        ) {
            eprint!("macos frame rehected before pool publication: {error}");
        }


    }
}



/// The macOS counterpart to Linux's portal-backed `ShareScreen` entry point.
///
/// Keeping this public shape identical means `main.rs` does not need to know
/// which OS supplied pixels. It does not imply that ScreenCaptureKit and
/// PipeWire have the same internals.
pub struct ShareScreen;

impl ShareScreen {
    pub const fn full_monitor() -> Self {
        Self
    }

    pub fn run(self, receiver_addr: &str) -> Result<(), Box<dyn Error>> {
        let content = SCShareableContent::get()?;
        let display = &content.displays()[0];
        // for dis in 0..display.len() {
        //     println!("Display #{} of width: {} and height: {}", dis, display[dis].width(),  display[dis].height());
        // }
        let filter = SCContentFilter::create()
        .with_display(display)
        .with_excluding_windows(&[])
        .build();

        let config = SCStreamConfiguration::new()
            .with_width(SCREEN_WIDTH)
            .with_height(SCREEN_HEIGHT)
            .with_pixel_format(ScreenPixelFormat::BGRA);

        let mut stream: SCStream = SCStream::new(&filter, &config);
        let stream_spec: StreamSpec = StreamSpec::new(SCREEN_WIDTH, SCREEN_HEIGHT, FramePixelFormat::Bgra8888)?;
        
        let (capture, sender) = Pipeline::new(stream_spec).into_ports();
        let sender_addr = receiver_addr.to_owned();
        std::thread::Builder::new()
            .name("melquiades-screen-sender".into())
            .spawn(move || {
                if let Err(error) = stream_from_sender(sender, &sender_addr, stream_spec) {
                    eprintln!("screen sender stopped: {error}")
                }
            })?;
        
        stream.add_output_handler(
            Handler{
                expected_stream: stream_spec, 
                capture: capture },
                SCStreamOutputType::Screen);
        stream.start_capture()?;
        eprintln!("screen capture running; press Ctrl-C to stop");
        std::thread::park();
        // stream.stop_capture()?;
        Ok(())
    }
}
