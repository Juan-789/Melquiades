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
use crate::recording::Recorder;
#[cfg(target_os = "windows")]
use std::collections::VecDeque;

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
    let started = Instant::now();

    #[cfg(target_os = "windows")]
    let mut history: VecDeque<ReceivedFrame> = VecDeque::with_capacity(2);
    #[cfg(target_os = "windows")]
    let mut recorder = Recorder::new();
    #[cfg(target_os = "windows")]
    let finalize_wake = event_loop.create_proxy();

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
                    winit::keyboard::Key::Named(winit::keyboard::NamedKey::Enter)
                ) =>
            {
                eprintln!("normal display mode");
            }

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
                let now = timestamp_ms(started, Instant::now());
                if recorder.is_active() {
                    recorder.stop(now, finalize_wake.clone());
                } else if history.len() == 2 {
                    let seeds = [history.pop_front().unwrap(), history.pop_front().unwrap()];
                    let seed_timestamps = [
                        timestamp_ms(started, seeds[0].timings.t2_decode_ends),
                        timestamp_ms(started, seeds[1].timings.t2_decode_ends),
                    ];
                    if let Err(error) = recorder.start(seeds, seed_timestamps, now) {
                        eprintln!("recording did not start: {error}");
                    }
                } else {
                    eprintln!("recording needs two consecutive decoded frames");
                }
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
                recorder.poll();

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
                    {
                        let timestamp = timestamp_ms(started, frame.timings.t2_decode_ends);
                        if recorder.is_active() {
                            recorder.record(frame, timestamp);
                        } else {
                            if history.back().is_some_and(|previous: &ReceivedFrame| {
                                previous.frame_id.wrapping_add(1) != frame.frame_id
                                    || previous.stream != frame.stream
                            }) {
                                history.clear();
                            }
                            if history.len() == 2 {
                                history.pop_front();
                            }
                            history.push_back(frame);
                        }
                    }
                }
            }

            _ => {}
        }
    })?;
    Ok(())
}

fn timestamp_ms(origin: Instant, timestamp: Instant) -> u128 {
    timestamp.saturating_duration_since(origin).as_millis()
}
