//! Platform boundary for monitor capture.
//!
//! The platform modules obtain screen pixels from their native capture APIs.
//! Everything after a frame is published to `CapturePort`—the FramePool, SPSC
//! handoff, compression, UDP transport, and receiver—remains shared Rust.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
pub use linux::ShareScreen;
#[cfg(target_os = "macos")]
pub use macos::ShareScreen;
