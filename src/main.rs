mod capture;
mod codec;
mod color;
mod compression;
mod config;
mod display;
mod metrics;
mod pipeline;
mod reassembly;
mod spsc;
mod time;
mod transport;
mod wire;

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod screen;

use capture::FileCapture;
#[cfg(target_os = "linux")]
use capture::V4l2Capture;
use transport::{receiving, receiving_h264, streaming};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("send") => {
            let capture = FileCapture::open("raw_frames.bin")?;
            streaming(capture, "0.0.0.0")?;
        }
        Some("recv") => receiving(None)?,
        Some("h264-recv") => receiving_h264()?,
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        Some("cast") => {
            let addr = args.get(2).map(String::as_str).unwrap_or("127.0.0.1:5000");
            screen::ShareScreen::full_monitor().run(addr)?
        }
        #[cfg(target_os = "macos")]
        Some("h264-smoke") => codec::macos::run_h264_smoke()?,
        #[cfg(target_os = "macos")]
        Some("cast-h264") => {
            let addr = args.get(2).map(String::as_str).unwrap_or("127.0.0.1:5000");
            codec::macos::cast_h264(addr)?
        }
        #[cfg(target_os = "linux")]
        Some("cam") => {
            let addr = args.get(2).map(String::as_str).unwrap_or("127.0.0.1");
            let capture = V4l2Capture::open("/dev/video0")?;
            streaming(capture, addr)?;
        }
        #[cfg(target_os = "linux")]
        Some("call-cam") => {
            let addr = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "127.0.0.1:5000".to_owned());
            display::display_with_sender(move || {
                let capture = V4l2Capture::open("/dev/video0")?;
                streaming(capture, &addr)
            })?;
        }
        #[cfg(target_os = "linux")]
        Some("h264-display") => display::display_h264()?,
        Some("display") => display::display()?,
        _ => {
            eprintln!(
                "pick [send|recv|h264-recv|h264-display|display|cam|call-cam|cast [receiver:port]|h264-smoke|cast-h264 [receiver:port]]"
            );
            std::process::exit(1);
        }
    }
    Ok(())
}
