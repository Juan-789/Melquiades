mod capture;
mod color;
mod compression;
mod config;
mod display;
mod metrics;
mod reassembly;
mod time;
mod transport;
mod wire;

use capture::FileCapture;
#[cfg(target_os = "linux")]
use capture::V4l2Capture;
use transport::{receiving, streaming};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("send") => {
            let mut capture = FileCapture::open("raw_frames.bin")?;
            streaming(&mut capture, "0.0.0.0")?;
        }
        Some("recv") => receiving(None)?,
        #[cfg(target_os = "linux")]
        Some("cam") => {
            let addr = args.get(2).map(String::as_str).unwrap_or("127.0.0.1");
            let mut capture = V4l2Capture::open("/dev/video0")?;
            streaming(&mut capture, addr)?;
        }
        #[cfg(target_os = "linux")]
        Some("call-cam") => {
            let addr = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "127.0.0.1:5000".to_owned());
            display::display_with_sender(move || {
                let mut capture = V4l2Capture::open("/dev/video0")?;
                streaming(&mut capture, &addr)
            })?;
        }
        Some("display") => display::display()?,
        _ => {
            eprintln!("pick [send|recv|display|cam|call-cam]");
            std::process::exit(1);
        }
    }
    Ok(())
}
