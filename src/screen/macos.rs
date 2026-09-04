//! macOS ScreenCaptureKit backend.
//!
//! This module intentionally contains only the public boundary for now. The
//! next implementation step is to replace this explicit error with a
//! ScreenCaptureKit stream whose callback calls the shared
//! `CapturePort::publish_strided` path.

use std::error::Error;

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
        Err(
            "macOS screen capture is not implemented yet; implement ScreenCaptureKit callback → CapturePort::publish_strided"
                .into(),
        )
    }
}
