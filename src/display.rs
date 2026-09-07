use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Instant;
use winit::dpi::LogicalSize;
use winit::event::{Event, WindowEvent};
use winit::event_loop::{ControlFlow, EventLoop};
use winit::window::WindowBuilder;

use crate::capture::{PixelFormat, StreamSpec};
use crate::color::{bgra_to_rgb, yuyv_to_rgb};
use crate::config::{HEIGHT, WIDTH};
use crate::metrics::PipelineStats;
#[cfg(target_os = "linux")]
use crate::transport::receiving_h264_to_display;
use crate::transport::{ReceivedFrame, receiving};

pub fn display() -> Result<(), Box<dyn std::error::Error>> {
    display_with_sender(|| Ok(()))
}

pub fn display_with_sender(
    sender: impl FnOnce() -> Result<(), Box<dyn std::error::Error>> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    std::thread::spawn(move || {
        if let Err(error) = sender() {
            eprintln!("sender died: {error}");
        }
    });
    display_with_receiver(|sender| receiving(Some(sender)))
}

/// Runs the shared native window against any receiver that emits decoded BGRA
/// or YUYV frames. Raw/Deflate and H.264 therefore share presentation and its
/// timing instrumentation, even though their decoding boundaries differ.
pub fn display_with_receiver(
    receive: impl FnOnce(
        std::sync::mpsc::SyncSender<ReceivedFrame>,
    ) -> Result<(), Box<dyn std::error::Error>>
    + Send
    + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = std::sync::mpsc::sync_channel::<ReceivedFrame>(1);
    std::thread::spawn(move || {
        if let Err(error) = receive(sender) {
            eprintln!("receiver died: {}", error);
        }
    });
    run_display(receiver)
}

/// Receives the Mac VideoToolbox H.264 stream on Linux and displays it.
#[cfg(target_os = "linux")]
pub fn display_h264() -> Result<(), Box<dyn std::error::Error>> {
    display_with_receiver(receiving_h264_to_display)
}

fn run_display(
    receiver: std::sync::mpsc::Receiver<ReceivedFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
    let event_loop = EventLoop::new()?;
    let window = Arc::new(
        WindowBuilder::new()
            .with_title("Melquiades")
            .with_inner_size(LogicalSize::new(WIDTH as u32, HEIGHT as u32))
            .build(&event_loop)?,
    );
    let context = softbuffer::Context::new(Arc::clone(&window))?;
    let mut surface = softbuffer::Surface::new(&context, Arc::clone(&window))?;
    surface.resize(
        NonZeroU32::new(WIDTH as u32).unwrap(),
        NonZeroU32::new(HEIGHT as u32).unwrap(),
    )?;
    let mut displayed_stream = StreamSpec::new(WIDTH as u32, HEIGHT as u32, PixelFormat::Yuyv422)?;
    let mut pipeline_stats = PipelineStats::new();
    event_loop.run(move |event, event_loop| {
        event_loop.set_control_flow(ControlFlow::Poll);
        match event {
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => event_loop.exit(),
            Event::AboutToWait => {
                if let Ok(frame) = receiver.try_recv() {
                    if frame.stream != displayed_stream {
                        let _ = surface.as_ref().request_inner_size(LogicalSize::new(
                            frame.stream.width,
                            frame.stream.height,
                        ));
                        surface
                            .resize(
                                NonZeroU32::new(frame.stream.width).unwrap(),
                                NonZeroU32::new(frame.stream.height).unwrap(),
                            )
                            .unwrap();
                        displayed_stream = frame.stream;
                    }
                    let mut buffer = surface.buffer_mut().unwrap();
                    match frame.stream.format {
                        PixelFormat::Yuyv422 => yuyv_to_rgb(&frame.pixels, &mut buffer),
                        PixelFormat::Bgra8888 => bgra_to_rgb(&frame.pixels, &mut buffer),
                    }
                    let t3_gpu_submission = Instant::now();
                    buffer.present().unwrap();
                    let t4_present_returned = Instant::now();
                    pipeline_stats.record(&frame.timings, t3_gpu_submission, t4_present_returned);
                }
            }
            _ => {}
        }
    })?;
    Ok(())
}
