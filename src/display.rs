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
use crate::transport::{ReceivedFrame, receiving};

#[cfg(target_os = "windows")]
#[path = "cv_capture.rs"]
mod cv_capture;

#[cfg(target_os = "windows")]
use crate::prediction::{PredictionRequest, PredictionResult, spawn_worker};
#[cfg(target_os = "windows")]
use cv_capture::CaptureWriter;

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
    display_with_receiver(|sender, _| receiving(Some(sender)))
}

pub fn display_with_receiver(
    receive: impl FnOnce(
        std::sync::mpsc::SyncSender<ReceivedFrame>,
        winit::event_loop::EventLoopProxy<()>,
    ) -> Result<(), Box<dyn std::error::Error>>
    + Send
    + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let event_loop = EventLoop::new()?;
    let wake = event_loop.create_proxy();
    let (sender, receiver) = std::sync::mpsc::sync_channel::<ReceivedFrame>(1);

    std::thread::spawn(move || {
        if let Err(error) = receive(sender, wake) {
            eprintln!("receiver died: {error}");
        }
    });

    run_display(event_loop, receiver)
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn display_h264() -> Result<(), Box<dyn std::error::Error>> {
    let socket = crate::transport::bind_h264_display("0.0.0.0:5000")?;
    display_with_receiver(move |sender, wake| {
        crate::transport::receiving_h264_to_display(socket, sender, || {
            let _ = wake.send_event(());
        })
    })
}

fn run_display(
    event_loop: EventLoop<()>,
    receiver: std::sync::mpsc::Receiver<ReceivedFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let mut experiment = Experiment::new();

    event_loop.run(move |event, event_loop| {
        event_loop.set_control_flow(ControlFlow::Wait);

        match event {
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => event_loop.exit(),

            #[cfg(target_os = "windows")]
            Event::WindowEvent {
                event: WindowEvent::KeyboardInput { event: key, .. },
                ..
            } if key.state.is_pressed()
                && matches!(
                    key.logical_key,
                    winit::keyboard::Key::Named(winit::keyboard::NamedKey::Space)
                ) =>
            {
                experiment.toggle();
            }

            Event::WindowEvent {
                event: WindowEvent::Resized(size),
                ..
            } if size.width > 0 && size.height > 0 => {
                surface
                    .resize(
                        NonZeroU32::new(size.width).unwrap(),
                        NonZeroU32::new(size.height).unwrap(),
                    )
                    .unwrap();
            }

            Event::AboutToWait => {
                #[cfg(target_os = "windows")]
                experiment.drain_results();

                while let Ok(frame) = receiver.try_recv() {
                    if frame.stream != displayed_stream {
                        let _ = window.request_inner_size(LogicalSize::new(
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
                    let submitted = Instant::now();
                    buffer.present().unwrap();
                    pipeline_stats.record(&frame.timings, submitted, Instant::now());

                    #[cfg(target_os = "windows")]
                    experiment.record(&frame);
                }
            }

            _ => {}
        }
    })?;
    Ok(())
}

#[cfg(target_os = "windows")]
struct Experiment {
    capture: CaptureWriter,
    request_tx: std::sync::mpsc::SyncSender<PredictionRequest>,
    result_rx: std::sync::mpsc::Receiver<std::io::Result<PredictionResult>>,
    run: Option<u32>,
    next_run: u32,
    previous: Option<SeedFrame>,
    pending: Option<(u32, u32)>,
}

#[cfg(target_os = "windows")]
struct SeedFrame {
    frame_id: u32,
    stream: StreamSpec,
    pixels: Vec<u8>,
}

#[cfg(target_os = "windows")]
impl Experiment {
    fn new() -> Self {
        let (request_tx, result_rx) = spawn_worker();
        Self {
            capture: CaptureWriter::new(),
            request_tx,
            result_rx,
            run: None,
            next_run: 1,
            previous: None,
            pending: None,
        }
    }

    fn toggle(&mut self) {
        if let Some(run) = self.run.take() {
            self.previous = None;
            eprintln!("inference capture run {run} stopped");
            return;
        }
        if self.pending.is_some() {
            eprintln!("waiting for the previous inference capture to finish");
            return;
        }
        let run = self.next_run;
        self.next_run = self.next_run.wrapping_add(1);
        self.run = Some(run);
        self.previous = None;
        eprintln!(
            "inference capture run {run} started at {}",
            self.capture.root.display()
        );
    }

    fn record(&mut self, frame: &ReceivedFrame) {
        let Some(run) = self.run else { return };
        let Some(image) = preview_from_frame(frame) else {
            return;
        };
        self.capture.record(run, "actual", frame.frame_id, &image);

        if self.pending.is_some() {
            return;
        }
        let seed = SeedFrame {
            frame_id: frame.frame_id,
            stream: frame.stream,
            pixels: frame.pixels.clone(),
        };
        let Some(previous) = self.previous.replace(seed) else {
            return;
        };
        if previous.frame_id.wrapping_add(1) != frame.frame_id
            || previous.stream != frame.stream
            || frame.stream.format != PixelFormat::Bgra8888
        {
            return;
        }
        let request = PredictionRequest {
            target_id: frame.frame_id.wrapping_add(1),
            width: frame.stream.width,
            height: frame.stream.height,
            count: 30,
            frame_a: previous.pixels,
            frame_b: frame.pixels.clone(),
        };
        if self.request_tx.try_send(request).is_ok() {
            self.pending = Some((run, 30));
            eprintln!("inference capture run {run} queued");
        }
    }

    fn drain_results(&mut self) {
        while let Ok(result) = self.result_rx.try_recv() {
            let Some((run, remaining)) = self.pending else {
                continue;
            };
            match result {
                Ok(frame) => {
                    if let Some(image) = preview_from_prediction(&frame) {
                        self.capture.record(run, "inferred", frame.frame_id, &image);
                    }
                    self.pending = (remaining > 1).then_some((run, remaining - 1));
                    if self.pending.is_none() {
                        eprintln!("inference capture run {run} finished");
                    }
                }
                Err(error) => {
                    self.pending = None;
                    eprintln!("inference capture run {run} failed: {error}");
                }
            }
        }
    }
}

#[cfg(target_os = "windows")]
const RESULT_WIDTH: usize = 448;
#[cfg(target_os = "windows")]
const RESULT_HEIGHT: usize = 256;

#[cfg(target_os = "windows")]
fn preview_from_frame(frame: &ReceivedFrame) -> Option<Vec<u32>> {
    if frame.stream.format != PixelFormat::Bgra8888 || frame.pixels.len() != frame.stream.byte_len {
        return None;
    }
    let width = frame.stream.width as usize;
    let height = frame.stream.height as usize;
    resize_pixels(width, height, |x, y| {
        let offset = (y * width + x) * 4;
        (u32::from(frame.pixels[offset + 2]) << 16)
            | (u32::from(frame.pixels[offset + 1]) << 8)
            | u32::from(frame.pixels[offset])
    })
}

#[cfg(target_os = "windows")]
fn preview_from_prediction(frame: &PredictionResult) -> Option<Vec<u32>> {
    let width = frame.width as usize;
    let height = frame.height as usize;
    if width == 0
        || height == 0
        || frame.pixels.len() != width.checked_mul(height)?.checked_mul(3)?
    {
        return None;
    }
    resize_pixels(width, height, |x, y| {
        let offset = (y * width + x) * 3;
        (u32::from(frame.pixels[offset + 2]) << 16)
            | (u32::from(frame.pixels[offset + 1]) << 8)
            | u32::from(frame.pixels[offset])
    })
}

#[cfg(target_os = "windows")]
fn resize_pixels(
    width: usize,
    height: usize,
    mut pixel: impl FnMut(usize, usize) -> u32,
) -> Option<Vec<u32>> {
    if width == 0 || height == 0 {
        return None;
    }
    let mut result = vec![0; RESULT_WIDTH * RESULT_HEIGHT];
    for y in 0..RESULT_HEIGHT {
        let source_y = y * height / RESULT_HEIGHT;
        for x in 0..RESULT_WIDTH {
            result[y * RESULT_WIDTH + x] = pixel(x * width / RESULT_WIDTH, source_y);
        }
    }
    Some(result)
}
