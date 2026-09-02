use std::num::NonZeroU32;
use std::time::Instant;
use winit::dpi::LogicalSize;
use winit::event::{Event, WindowEvent};
use winit::event_loop::{ControlFlow, EventLoop};
use winit::window::WindowBuilder;

use crate::color::yuyv_to_rgb;
use crate::config::{HEIGHT, WIDTH};
use crate::metrics::PipelineStats;
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
    let (sender, receiver) = std::sync::mpsc::sync_channel::<ReceivedFrame>(1);
    std::thread::spawn(move || {
        if let Err(error) = receiving(Some(sender)) {
            eprintln!("receiver died: {}", error);
        }
    });
    let event_loop = EventLoop::new()?;
    let window = WindowBuilder::new()
        .with_title("Melquiades")
        .with_inner_size(LogicalSize::new(WIDTH as u32, HEIGHT as u32))
        .build(&event_loop)?;
    let context = softbuffer::Context::new(&window)?;
    let mut surface = softbuffer::Surface::new(&context, &window)?;
    surface.resize(
        NonZeroU32::new(WIDTH as u32).unwrap(),
        NonZeroU32::new(HEIGHT as u32).unwrap(),
    )?;
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
                    let mut buffer = surface.buffer_mut().unwrap();
                    yuyv_to_rgb(&frame.pixels, &mut buffer);
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
