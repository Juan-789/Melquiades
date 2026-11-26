use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread;
use std::time::Duration;

pub struct PredictionRequest {
    pub target_id: u32,
    pub width: u32,
    pub height: u32,
    pub count: u32,
    pub frame_a: Vec<u8>,
    pub frame_b: Vec<u8>,
}

pub struct PredictionResult {
    pub frame_id: u32,
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

pub fn spawn_worker() -> (
    SyncSender<PredictionRequest>,
    Receiver<io::Result<PredictionResult>>,
) {
    let (request_tx, request_rx) = mpsc::sync_channel(1);
    let (result_tx, result_rx) = mpsc::channel();

    thread::Builder::new()
        .name("dmvfn-worker".into())
        .spawn(move || {
            let mut connection = None;
            for request in request_rx {
                let result = (|| {
                    let stream = match &mut connection {
                        Some(stream) => stream,
                        None => {
                            let stream = TcpStream::connect("127.0.0.1:5055")?;
                            stream.set_nodelay(true)?;
                            stream.set_read_timeout(Some(Duration::from_secs(10)))?;
                            stream.set_write_timeout(Some(Duration::from_secs(10)))?;
                            connection.insert(stream)
                        }
                    };
                    exchange(stream, request, &result_tx)
                })();
                if let Err(error) = result {
                    connection = None;
                    if result_tx.send(Err(error)).is_err() {
                        break;
                    }
                }
            }
        })
        .expect("failed to spawn DMVFN worker");

    (request_tx, result_rx)
}

fn exchange(
    stream: &mut TcpStream,
    request: PredictionRequest,
    result_tx: &Sender<io::Result<PredictionResult>>,
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
            .send(Ok(PredictionResult {
                frame_id,
                width,
                height,
                pixels,
            }))
            .map_err(|_| io::Error::other("display closed the results channel"))?;
    }
    Ok(())
}
