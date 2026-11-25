// Off-thread IPC bridge between Melquiades and the Python DMVFN server.
// The event loop submits a seed pair with `try_send` and polls results with
// `try_recv`; it never blocks on TCP or CUDA inference.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread;
use std::time::Duration;

pub struct PredictionRequest {
    /// ID assigned to the first future frame, not to either seed frame.
    pub target_id: u32,
    pub width: u32,
    pub height: u32,
    /// Consecutive recursive predictions to generate (1..=150).
    pub count: u32,
    /// Tightly packed BGRA pixels.
    pub frame_a: Vec<u8>,
    pub frame_b: Vec<u8>,
}

pub struct PredictionResult {
    pub frame_id: u32,
    pub width: u32,
    pub height: u32,
    /// Tightly packed BGR pixels, 3 bytes per pixel.
    pub pixels: Vec<u8>,
}

pub fn spawn_worker() -> (
    SyncSender<PredictionRequest>,
    Receiver<PredictionResult>,
) {
    // one pending seed request: never block the display event loop.
    let (request_tx, request_rx) = mpsc::sync_channel(1);
    // remporary unbounded result queue to verify all 21 IDs arrive.
    // Replace with deadline-aware bounded buffering before sustained use.
    let (result_tx, result_rx) = mpsc::channel();

    thread::Builder::new()
        .name("dmvfn-worker".into())
        .spawn(move || {
            let mut stream = match TcpStream::connect("127.0.0.1:5055") {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("DMVFN connection failed: {error}; start Python first");
                    return;
                }
            };
            if let Err(error) = stream.set_nodelay(true) {
                eprintln!("Could not set TCP_NODELAY: {error}");
            }
            if let Err(error) = stream.set_read_timeout(Some(Duration::from_secs(10))) {
                eprintln!("Could not set read timeout: {error}");
            }

            for request in request_rx {
                if let Err(error) = exchange(&mut stream, request, &result_tx) {
                    eprintln!("DMVFN IPC exchange failed: {error}");
                    break;
                }
            }
        })
        .expect("failed to spawn DMVFN worker");

    (request_tx, result_rx)
}

fn exchange(
    stream: &mut TcpStream,
    request: PredictionRequest,
    result_tx: &Sender<PredictionResult>,
) -> io::Result<()> {
    if request.count == 0 || request.count > 150 {
        return Err(io::Error::other("count must be 1..=150"));
    }
    if request.width == 0 || request.height == 0 || request.width > 4096 || request.height > 4096 {
        return Err(io::Error::other("invalid input dimensions"));
    }
    let input_bytes = (request.width as usize)
        .checked_mul(request.height as usize)
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(|| io::Error::other("invalid input size"))?;
    if request.frame_a.len() != input_bytes || request.frame_b.len() != input_bytes {
        return Err(io::Error::other("expected tightly packed BGRA seed frames"));
    }

    // 20-byte request header. Must match Python struct.unpack('<4sIIII', ...).
    stream.write_all(b"SEED")?;
    stream.write_all(&request.target_id.to_le_bytes())?;
    stream.write_all(&request.width.to_le_bytes())?;
    stream.write_all(&request.height.to_le_bytes())?;
    stream.write_all(&request.count.to_le_bytes())?;
    stream.write_all(&request.frame_a)?;
    stream.write_all(&request.frame_b)?;

    for step in 0..request.count {
        let mut header = [0u8; 16];
        stream.read_exact(&mut header)?;
        if &header[..4] != b"PRED" {
            return Err(io::Error::other("unexpected prediction response"));
        }
        let frame_id = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let width = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let height = u32::from_le_bytes(header[12..16].try_into().unwrap());
        if frame_id != request.target_id.wrapping_add(step) {
            return Err(io::Error::other("prediction ID mismatch"));
        }
        if width == 0 || height == 0 || width > 4096 || height > 4096 {
            return Err(io::Error::other("invalid prediction dimensions"));
        }
        let result_bytes = (width as usize)
            .checked_mul(height as usize)
            .and_then(|n| n.checked_mul(3))
            .ok_or_else(|| io::Error::other("invalid prediction size"))?;
        let mut pixels = vec![0u8; result_bytes];
        stream.read_exact(&mut pixels)?;

        result_tx
            .send(PredictionResult {
                frame_id,
                width,
                height,
                pixels,
            })
            .map_err(|_| io::Error::other("display closed the results channel"))?;
    }
    Ok(())
}
