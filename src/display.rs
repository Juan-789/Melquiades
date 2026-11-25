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

#[cfg(target_os = "windows")]
use crate::prediction::{
    spawn_worker,
    PredictionRequest,
};

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
#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn display_h264() -> Result<(), Box<dyn std::error::Error>> {
    // #[cfg(target_os="linux")] {
    //     let recv = crate::transport::receiving_h264_to_display;
    // }
    // #[cfg(target_os="windows")] {
    //     let recv = crate::transport::receiving_h264_to_display;
    // }

    display_with_receiver(crate::transport::receiving_h264_to_display)
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

    #[cfg(target_os = "windows")]
    let (prediction_tx, prediction_rx) = spawn_worker();

    #[cfg(target_os = "windows")]
    let mut previous_frame: Option<ReceivedFrame> = None;

    #[cfg(target_os = "windows")]
    let mut sent_test_request = false;
    #[cfg(target_os = "windows")]
    let mut latest_prediction: Option<crate::prediction::PredictionResult> = None;
    #[cfg(target_os = "windows")]
    let mut prediction_started: Option<Instant> = None;
    event_loop.run(move |event, event_loop| {
        event_loop.set_control_flow(ControlFlow::Poll);
        match event {
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => event_loop.exit(),
            Event::AboutToWait => {
            #[cfg(target_os = "windows")]
            while let Ok(prediction) = prediction_rx.try_recv() {
                if let Some(started) = prediction_started.take() {
                    eprintln!(
                        "First prediction arrived after {:.2} ms",
                        started.elapsed().as_secs_f64() * 1000.0
                    );
                }

                eprintln!(
                    "Python returned frame {} ({}x{}, {} bytes)",
                    prediction.frame_id,
                    prediction.width,
                    prediction.height,
                    prediction.pixels.len(),
                );

                latest_prediction = Some(prediction);
            }

                if let Ok(frame) = receiver.try_recv() {
                    eprintln!("Displaying frame ID: {}", frame.frame_id);
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

                    #[cfg(target_os = "windows")]
                    if !sent_test_request {
                        if let Some(previous) = previous_frame.as_ref() {
                            let consecutive =
                                previous.frame_id.wrapping_add(1) == frame.frame_id;

                            if consecutive
                                && previous.stream == frame.stream
                                && frame.stream.format == PixelFormat::Bgra8888
                            {
                                let request = PredictionRequest {
                                    target_id: frame.frame_id.wrapping_add(1),
                                    width: frame.stream.width,
                                    height: frame.stream.height,
                                    count:21,
                                    frame_a: previous.pixels.clone(),
                                    frame_b: frame.pixels.clone(),
                                };

                                if prediction_tx.try_send(request).is_ok() {
                                    prediction_started = Some(Instant::now());
                                    sent_test_request = true;
                                    eprintln!(
                                        "Sent seed frames {} and {} to Python",
                                        previous.frame_id,
                                        frame.frame_id
                                    );
                                }
                            }
                        }
                    }

                    #[cfg(target_os = "windows")]
                    if let Some(prediction) = latest_prediction.as_ref() {
                        let screen_width = frame.stream.width as usize;
                        let screen_height = frame.stream.height as usize;

                        let pred_width = prediction.width as usize;
                        let pred_height = prediction.height as usize;

                        if pred_width <= screen_width
                            && pred_height <= screen_height
                            && prediction.pixels.len() == pred_width * pred_height * 3
                        {
                            // Place prediction in the top-right corner.
                            let offset_x = 0;

                            for y in 0..pred_height {
                                for x in 0..pred_width {
                                    let src = (y * pred_width + x) * 3;

                                    let b = prediction.pixels[src] as u32;
                                    let g = prediction.pixels[src + 1] as u32;
                                    let r = prediction.pixels[src + 2] as u32;

                                    let rgb = (r << 16) | (g << 8) | b;

                                    let dst = y * screen_width + offset_x + x;
                                    buffer[dst] = rgb;
                                }
                            }
                        }
                    }

                    let t3_gpu_submission = Instant::now();
                    buffer.present().unwrap();
                    let t4_present_returned = Instant::now();
                    pipeline_stats.record(&frame.timings, t3_gpu_submission, t4_present_returned);
                    #[cfg(target_os = "windows")] {
                        previous_frame = Some(frame);
                    }
                }
            }
            _ => {}
        }
    })?;
    Ok(())
}
