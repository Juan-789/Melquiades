//! macOS ScreenCaptureKit backend.
//!
//! This module intentionally contains only the public boundary for now. The
//! next implementation step is to replace this explicit error with a
//! ScreenCaptureKit stream whose callback calls the shared
//! `CapturePort::publish_strided` path.

use std::error::Error;
use screencapturekit::prelude::*;

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

    pub fn run(self, _receiver_addr: &str) -> Result<(), Box<dyn Error>> {
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
            .with_width(1920)
            .with_height(1080)
            .with_pixel_format(PixelFormat::BGRA);

        let mut stream = SCStream::new(&filter, &config);
        stream.add_output_handler(Handler, SCStreamOutputType::Screen);
        stream.start_capture()?;

        std::thread::sleep(std::time::Duration::from_secs(5));
        stream.stop_capture()?;
        Ok(())
    }
}
