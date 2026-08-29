use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::thread::sleep;
use std::time::Duration;

use crate::config::FRAME_SIZE;

pub trait Capture {
    fn next_frame(&mut self, out: &mut [u8]) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct FileCapture {
    reader: BufReader<File>,
    total_frames: u64,
    index: u64,
}

impl FileCapture {
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let total_frames = file.metadata()?.len() / FRAME_SIZE as u64;
        if total_frames == 0 {
            return Err("file is smaller than one frame".into());
        }
        Ok(Self {
            reader: BufReader::new(file),
            total_frames,
            index: 0,
        })
    }
}

impl Capture for FileCapture {
    fn next_frame(&mut self, out: &mut [u8]) -> Result<(), Box<dyn std::error::Error>> {
        if self.index >= self.total_frames {
            self.reader.rewind()?;
            self.index = 0;
        }
        self.reader.read_exact(out)?;
        self.index += 1;
        sleep(Duration::from_millis(33));
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use linuxvideo::Device;
    use linuxvideo::format::{PixFormat, PixelFormat};
    use linuxvideo::stream::ReadStream;

    use super::Capture;
    use crate::config::{FRAME_SIZE, HEIGHT, WIDTH};

    pub struct V4l2Capture {
        stream: ReadStream,
    }

    impl V4l2Capture {
        pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let device = Device::open(path)?;
            let capture = device.video_capture(PixFormat::new(
                WIDTH as u32,
                HEIGHT as u32,
                PixelFormat::YUYV,
            ))?;
            let format = capture.format();
            if format.width() != WIDTH as u32 || format.height() != HEIGHT as u32 {
                return Err(format!("driver gave {}x{}", format.width(), format.height()).into());
            }
            Ok(Self {
                stream: capture.into_stream()?,
            })
        }
    }

    impl Capture for V4l2Capture {
        fn next_frame(&mut self, out: &mut [u8]) -> Result<(), Box<dyn std::error::Error>> {
            self.stream.dequeue(|buf| {
                let length = buf.len().min(FRAME_SIZE);
                out[..length].copy_from_slice(&buf[..length]);
                Ok(())
            })?;
            Ok(())
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::V4l2Capture;
