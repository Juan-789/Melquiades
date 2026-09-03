mod capture;
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

#[cfg(target_os = "linux")]
mod screen;

use capture::FileCapture;
#[cfg(target_os = "linux")]
use capture::V4l2Capture;
use transport::{receiving, streaming};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("send") => {
            let capture = FileCapture::open("raw_frames.bin")?;
            streaming(capture, "0.0.0.0")?;
        }
        Some("recv") => receiving(None)?,
        #[cfg(target_os = "linux")]
        Some("cast") => {
            let addr = args.get(2).map(String::as_str).unwrap_or("127.0.0.1:5000");
            screen::ShareScreen::full_monitor().run(addr)?
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
        Some("display") => display::display()?,
        _ => {
            eprintln!("pick [send|recv|display|cam|call-cam|cast [receiver:port]]");
            std::process::exit(1);
        }
    }
    Ok(())
}
