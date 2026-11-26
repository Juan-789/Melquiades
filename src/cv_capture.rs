use std::path::PathBuf;
use std::sync::mpsc::{self, SyncSender};

const WIDTH: usize = 448;
const HEIGHT: usize = 256;

struct Job {
    run: u32,
    kind: &'static str,
    frame_id: u32,
    pixels: Vec<u32>,
}

pub(super) struct CaptureWriter {
    tx: SyncSender<Job>,
    pub(super) root: PathBuf,
}

impl CaptureWriter {
    pub(super) fn new() -> Self {
        let root = std::env::var_os("CV_CAPTURE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("inference_results"));
        let (tx, rx) = mpsc::sync_channel::<Job>(64);
        let worker_root = root.clone();
        std::thread::spawn(move || {
            for job in rx {
                let folder = worker_root.join(format!("run_{:03}", job.run));
                if let Err(e) = std::fs::create_dir_all(&folder) {
                    eprintln!("CV capture cannot create {}: {e}", folder.display());
                    continue;
                }
                let path = folder.join(format!("{}_{:010}.ppm", job.kind, job.frame_id));
                let mut data = format!("P6\n{WIDTH} {HEIGHT}\n255\n").into_bytes();
                data.reserve(job.pixels.len() * 3);
                for rgb in job.pixels {
                    data.push((rgb >> 16) as u8);
                    data.push((rgb >> 8) as u8);
                    data.push(rgb as u8);
                }
                if let Err(e) = std::fs::write(&path, data) {
                    eprintln!("CV capture write failed {}: {e}", path.display());
                }
            }
        });
        Self { tx, root }
    }

    pub(super) fn record(&self, run: u32, kind: &'static str, frame_id: u32, pixels: &[u32]) {
        if run == 0 || pixels.len() != WIDTH * HEIGHT {
            return;
        }
        if self
            .tx
            .try_send(Job {
                run,
                kind,
                frame_id,
                pixels: pixels.to_vec(),
            })
            .is_err()
        {
            eprintln!("CV capture backlog full: dropped {kind} frame {frame_id}");
        }
    }
}
