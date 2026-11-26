use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};

use crate::capture::{PixelFormat, StreamSpec};
use crate::transport::ReceivedFrame;

const QUEUE_CAPACITY: usize = 32;

pub struct Recorder {
    next_run: u32,
    source_fps: u32,
    state: State,
}

enum State {
    Idle,
    Active(ActiveRecording),
    Finalizing(JoinHandle<Result<Summary, String>>),
}

struct ActiveRecording {
    directory: PathBuf,
    sender: Option<SyncSender<Job>>,
    writer: Option<JoinHandle<Vec<(usize, bool)>>>,
    records: Vec<Record>,
    queue_drops: u64,
    source_fps: u32,
    start_ms: u128,
    start_display_drops: u64,
    last_display_drops: u64,
    stream: StreamSpec,
}

struct Job {
    index: usize,
    path: PathBuf,
    pixels: Vec<u8>,
    stream: StreamSpec,
}

struct Record {
    frame_id: u32,
    timestamp_ms: u128,
    role: &'static str,
    filename: String,
    recorded: bool,
}

struct Summary {
    directory: PathBuf,
    duration_ms: u128,
    written: u64,
    dropped: u64,
    queue_drops: u64,
    display_drops: u64,
}

impl Recorder {
    pub fn new() -> Self {
        let source_fps = std::env::var("MELQUIADES_SOURCE_FPS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|fps: &u32| *fps > 0)
            .unwrap_or(15);
        Self {
            next_run: 1,
            source_fps,
            state: State::Idle,
        }
    }

    pub fn start(
        &mut self,
        seeds: [ReceivedFrame; 2],
        seed_timestamps_ms: [u128; 2],
        start_ms: u128,
    ) -> Result<(), String> {
        if !matches!(self.state, State::Idle) {
            return Err("recording is still finalizing".into());
        }
        validate_seeds([&seeds[0], &seeds[1]])?;
        let [first_seed, second_seed] = seeds;
        let (run, directory) = unique_directory(Path::new("captures"), self.next_run)?;
        self.next_run = run.wrapping_add(1);
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let writer = thread::Builder::new()
            .name("melquiades-recording".into())
            .spawn(move || writer_loop(receiver))
            .map_err(|error| error.to_string())?;
        let mut recording = ActiveRecording {
            directory,
            sender: Some(sender),
            writer: Some(writer),
            records: Vec::new(),
            queue_drops: 0,
            source_fps: self.source_fps,
            start_ms,
            start_display_drops: second_seed.display_channel_drops,
            last_display_drops: second_seed.display_channel_drops,
            stream: second_seed.stream,
        };
        recording.enqueue(first_seed, "seed", seed_timestamps_ms[0]);
        recording.enqueue(second_seed, "seed", seed_timestamps_ms[1]);
        eprintln!("recording started: {}", recording.directory.display());
        self.state = State::Active(recording);
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, State::Active(_))
    }

    pub fn record(&mut self, frame: ReceivedFrame, timestamp_ms: u128) {
        if let State::Active(recording) = &mut self.state {
            recording.enqueue(frame, "actual", timestamp_ms);
        }
    }

    pub fn stop(&mut self, stop_ms: u128, wake: winit::event_loop::EventLoopProxy<()>) {
        let State::Active(mut recording) = std::mem::replace(&mut self.state, State::Idle) else {
            return;
        };
        recording.sender.take();
        self.state = State::Finalizing(thread::spawn(move || {
            let result = finalize(recording, stop_ms);
            let _ = wake.send_event(());
            result
        }));
        eprintln!("recording stopped, finishing pending writes");
    }

    pub fn poll(&mut self) {
        let State::Finalizing(handle) = &self.state else {
            return;
        };
        if !handle.is_finished() {
            return;
        }
        let handle = match std::mem::replace(&mut self.state, State::Idle) {
            State::Finalizing(handle) => handle,
            _ => unreachable!(),
        };
        match handle.join() {
            Ok(Ok(summary)) => eprintln!(
                "recording finalized: {} duration={}ms written={} dropped={} queue_drops={} display_drops={}",
                summary.directory.display(),
                summary.duration_ms,
                summary.written,
                summary.dropped,
                summary.queue_drops,
                summary.display_drops,
            ),
            Ok(Err(error)) => eprintln!("recording finalization failed: {error}"),
            Err(_) => eprintln!("recording finalization panicked"),
        }
    }
}

impl ActiveRecording {
    fn enqueue(&mut self, frame: ReceivedFrame, role: &'static str, timestamp_ms: u128) {
        let folder = if role == "seed" { "seeds" } else { "actual" };
        let filename = format!("{folder}/frame_{:010}.ppm", frame.frame_id);
        let index = self.records.len();
        self.records.push(Record {
            frame_id: frame.frame_id,
            timestamp_ms,
            role,
            filename: filename.clone(),
            recorded: false,
        });
        self.last_display_drops = frame.display_channel_drops;
        if frame.stream.format != PixelFormat::Bgra8888
            || frame.pixels.len() != frame.stream.byte_len
        {
            return;
        }
        let Some(sender) = &self.sender else {
            return;
        };
        let path = self.directory.join(filename);
        let job = Job {
            index,
            path,
            pixels: frame.pixels,
            stream: frame.stream,
        };
        match sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.queue_drops += 1,
            Err(TrySendError::Disconnected(_)) => self.queue_drops += 1,
        }
    }
}

fn validate_seeds(seeds: [&ReceivedFrame; 2]) -> Result<(), String> {
    if seeds[0].frame_id.wrapping_add(1) != seeds[1].frame_id {
        return Err("need two consecutive decoded frames before recording".into());
    }
    if seeds[0].stream != seeds[1].stream || seeds[1].stream.format != PixelFormat::Bgra8888 {
        return Err("need two consecutive BGRA decoded frames before recording".into());
    }
    if seeds
        .iter()
        .any(|frame| frame.pixels.len() != frame.stream.byte_len)
    {
        return Err("decoded seed frame length is invalid".into());
    }
    Ok(())
}

fn unique_directory(root: &Path, mut run: u32) -> Result<(u32, PathBuf), String> {
    loop {
        fs::create_dir_all(root).map_err(|error| error.to_string())?;
        let directory = root.join(format!("run_{run:03}"));
        match fs::create_dir(&directory) {
            Ok(()) => {
                fs::create_dir(directory.join("seeds")).map_err(|error| error.to_string())?;
                fs::create_dir(directory.join("actual")).map_err(|error| error.to_string())?;
                return Ok((run, directory));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                run = run.wrapping_add(1);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn writer_loop(receiver: mpsc::Receiver<Job>) -> Vec<(usize, bool)> {
    receiver
        .into_iter()
        .map(|job| {
            let result = write_ppm(&job.path, &job.pixels, job.stream).is_ok();
            (job.index, result)
        })
        .collect()
}

fn write_ppm(path: &Path, pixels: &[u8], stream: StreamSpec) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut output = BufWriter::new(file);
    write!(output, "P6\n{} {}\n255\n", stream.width, stream.height)?;
    for pixel in pixels.chunks_exact(4) {
        output.write_all(&[pixel[2], pixel[1], pixel[0]])?;
    }
    output.flush()
}

fn finalize(mut recording: ActiveRecording, stop_ms: u128) -> Result<Summary, String> {
    let outcomes = recording
        .writer
        .take()
        .expect("recording writer is present")
        .join()
        .map_err(|_| "recording writer panicked".to_owned())?;
    for (index, written) in outcomes {
        if let Some(record) = recording.records.get_mut(index) {
            record.recorded = written;
        }
    }
    let written = recording
        .records
        .iter()
        .filter(|record| record.recorded)
        .count() as u64;
    let dropped = recording.records.len() as u64 - written;
    write_timestamps(&recording.directory, &recording.records)?;
    write_metadata(&recording, stop_ms, written, dropped)?;
    Ok(Summary {
        directory: recording.directory,
        duration_ms: stop_ms.saturating_sub(recording.start_ms),
        written,
        dropped,
        queue_drops: recording.queue_drops,
        display_drops: recording
            .last_display_drops
            .saturating_sub(recording.start_display_drops),
    })
}

fn write_timestamps(directory: &Path, records: &[Record]) -> Result<(), String> {
    let mut file =
        BufWriter::new(File::create(directory.join("timestamps.csv")).map_err(|e| e.to_string())?);
    writeln!(file, "frame_id,timestamp_ms,role,filename,recorded").map_err(|e| e.to_string())?;
    for record in records {
        writeln!(
            file,
            "{},{},{},{},{}",
            record.frame_id, record.timestamp_ms, record.role, record.filename, record.recorded
        )
        .map_err(|e| e.to_string())?;
    }
    file.flush().map_err(|e| e.to_string())
}

fn write_metadata(
    recording: &ActiveRecording,
    stop_ms: u128,
    written: u64,
    dropped: u64,
) -> Result<(), String> {
    let mut file = BufWriter::new(
        File::create(recording.directory.join("metadata.json")).map_err(|e| e.to_string())?,
    );
    writeln!(file, "{{").map_err(|e| e.to_string())?;
    writeln!(
        file,
        "  \"timestamp_origin\": \"display startup monotonic clock\","
    )
    .map_err(|e| e.to_string())?;
    writeln!(
        file,
        "  \"timestamp_measurement\": \"decoder completion before display\","
    )
    .map_err(|e| e.to_string())?;
    writeln!(file, "  \"recording_start_ms\": {},", recording.start_ms)
        .map_err(|e| e.to_string())?;
    writeln!(file, "  \"recording_stop_ms\": {},", stop_ms).map_err(|e| e.to_string())?;
    writeln!(file, "  \"nominal_source_fps\": {},", recording.source_fps)
        .map_err(|e| e.to_string())?;
    writeln!(file, "  \"frame_width\": {},", recording.stream.width).map_err(|e| e.to_string())?;
    writeln!(file, "  \"frame_height\": {},", recording.stream.height)
        .map_err(|e| e.to_string())?;
    writeln!(file, "  \"successful_frames\": {},", written).map_err(|e| e.to_string())?;
    writeln!(file, "  \"dropped_frames\": {},", dropped).map_err(|e| e.to_string())?;
    writeln!(
        file,
        "  \"recording_queue_drops\": {},",
        recording.queue_drops
    )
    .map_err(|e| e.to_string())?;
    writeln!(
        file,
        "  \"display_channel_drops\": {}",
        recording
            .last_display_drops
            .saturating_sub(recording.start_display_drops)
    )
    .map_err(|e| e.to_string())?;
    writeln!(file, "}}").map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn finalization_writes_full_resolution_ppm_and_metadata() {
        let root = std::env::temp_dir().join(format!(
            "melquiades-recording-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (_, directory) = unique_directory(&root, 1).unwrap();
        let (next_run, next_directory) = unique_directory(&root, 1).unwrap();
        assert_eq!(next_run, 2);
        assert_ne!(directory, next_directory);
        let stream = StreamSpec::new(2, 2, PixelFormat::Bgra8888).unwrap();
        let path = directory.join("seeds/frame_0000000041.ppm");
        let pixels = vec![0, 0, 255, 0].repeat(4);
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .send(Job {
                index: 0,
                path: path.clone(),
                pixels,
                stream,
            })
            .unwrap();
        drop(sender);
        let active = ActiveRecording {
            directory: directory.clone(),
            sender: None,
            writer: Some(thread::spawn(move || writer_loop(receiver))),
            records: vec![Record {
                frame_id: 41,
                timestamp_ms: 12,
                role: "seed",
                filename: "seeds/frame_0000000041.ppm".into(),
                recorded: false,
            }],
            queue_drops: 0,
            source_fps: 15,
            start_ms: 10,
            start_display_drops: 0,
            last_display_drops: 0,
            stream,
        };
        let summary = finalize(active, 20).unwrap();
        assert_eq!(summary.written, 1);
        assert_eq!(summary.dropped, 0);
        assert!(std::fs::read(&path).unwrap().starts_with(b"P6\n2 2\n255\n"));
        assert!(
            std::fs::read_to_string(directory.join("timestamps.csv"))
                .unwrap()
                .contains("41,12,seed,seeds/frame_0000000041.ppm,true")
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
