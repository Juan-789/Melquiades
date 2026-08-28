use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Seek, Write};
use std::net::UdpSocket;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use linuxvideo::Device;
use linuxvideo::format::PixelFormat;
use linuxvideo::format::PixFormat;
use linuxvideo::stream::ReadStream;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::num::NonZeroU32;
use std::sync::mpsc::SyncSender;
use winit::dpi::LogicalSize;
use winit::event::{Event, WindowEvent};
use winit::event_loop::{ControlFlow, EventLoop};
use winit::window::WindowBuilder;


const WIDTH: usize = 640;
const HEIGHT: usize = 480;
const FRAME_SIZE: usize = WIDTH * HEIGHT * 2; // YUYV = 2 bytes/px
const MAX_CHUNK_PAYLOAD: usize = 1200;
const CHUNKS_PER_FRAME: usize = FRAME_SIZE.div_ceil(MAX_CHUNK_PAYLOAD); // 512

const HEADER_BYTES: usize = 21;
const MAGIC: u16 = 0xADC0;
const DATAGRAM_MAX: usize = HEADER_BYTES + MAX_CHUNK_PAYLOAD;
const FLAG_COMPRESSED: u8 = 0b0000_0001;
// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------
// [2B magic][4B frame_id][8B capture_ts][2B chunk_index][2B total_chunks]
// [2B chunk_len][1B flags][chunk_len bytes payload]

struct PacketHeader {
    frame_id: u32,
    capture_ts: u64,
    chunk_index: u16,
    total_chunks: u16,
    chunk_len: u16,
    flags: u8,
}

impl PacketHeader {
    pub fn encode(&self, out: &mut [u8]) {
        out[0..2].copy_from_slice(&MAGIC.to_be_bytes());
        out[2..6].copy_from_slice(&self.frame_id.to_be_bytes());
        out[6..14].copy_from_slice(&self.capture_ts.to_be_bytes());
        out[14..16].copy_from_slice(&self.chunk_index.to_be_bytes());
        out[16..18].copy_from_slice(&self.total_chunks.to_be_bytes());
        out[18..20].copy_from_slice(&self.chunk_len.to_be_bytes());
        out[20] = self.flags;
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_BYTES {
            return None;
        }
        if u16::from_be_bytes([bytes[0], bytes[1]]) != MAGIC {
            return None;
        }
        Some(Self {
            frame_id: u32::from_be_bytes(bytes[2..6].try_into().ok()?),
            capture_ts: u64::from_be_bytes(bytes[6..14].try_into().ok()?),
            chunk_index: u16::from_be_bytes([bytes[14], bytes[15]]),
            total_chunks: u16::from_be_bytes([bytes[16], bytes[17]]),
            chunk_len: u16::from_be_bytes([bytes[18], bytes[19]]),
            flags: bytes[20],
        })
    }
}

// ---------------------------------------------------------------------------
// Reassembly
// ---------------------------------------------------------------------------

struct Reassembler {
    frame_id: u32,
    buf: Vec<u8>,
    received: [u64; 8], // 512 bits
    count: u16,
    total_chunks: u16,
    bytes: usize,
}

impl Reassembler {
    pub fn new() -> Self {
        Self {
            frame_id: u32::MAX,
            buf: vec![0u8; FRAME_SIZE],
            received: [0; 8],
            count: 0,
            total_chunks: 0,
            bytes: 0,
        }
    }

    pub fn reset(&mut self, frame_id: u32) {
        self.frame_id = frame_id;
        self.received = [0; 8];
        self.count = 0;
        self.total_chunks = 0;
        self.bytes = 0;
    }

    /// Returns true once every chunk of this frame has arrived.
    pub fn add(&mut self, hdr: &PacketHeader, payload: &[u8]) -> bool {
        let idx = hdr.chunk_index as usize;
        if idx >= CHUNKS_PER_FRAME { return false; }

        let word = idx / 64;
        let bit = 1u64 << (idx % 64);
        if self.received[word] & bit != 0 { return false; }

        let offset = idx * MAX_CHUNK_PAYLOAD;
        self.buf[offset..offset + payload.len()].copy_from_slice(payload);
        self.received[word] |= bit;
        self.count += 1;
        self.total_chunks = hdr.total_chunks;

        let end = offset + payload.len();
        if end > self.bytes { self.bytes = end; }

        self.count == self.total_chunks
    }

    pub fn missing(&self) -> u16 {
        self.total_chunks.saturating_sub(self.count)
    }
}

// ---------------------------------------------------------------------------
// Capture
// ---------------------------------------------------------------------------

pub trait Capture {
    fn next_frame(&mut self, out: &mut [u8]) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct FileCapture {
    reader: BufReader<File>,
    total_frames: u64,
    idx: u64,
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
            idx: 0,
        })
    }
}

impl Capture for FileCapture {
    fn next_frame(&mut self, out: &mut [u8]) -> Result<(), Box<dyn std::error::Error>> {
        if self.idx >= self.total_frames {
            self.reader.rewind()?;
            self.idx = 0;
        }
        self.reader.read_exact(out)?;
        self.idx += 1;
        sleep(Duration::from_millis(33));
        Ok(())
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}



struct LatencyStats {
    samples: Vec<f64>,
}

impl LatencyStats {
    fn new() -> Self {
        Self { samples: Vec::with_capacity(64) }
    }

    fn record(&mut self, ms: f64) {
        self.samples.push(ms);
        if self.samples.len() >= 30 {
            self.report();
        }
    }

    fn report(&mut self) {
        self.samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = self.samples.len();
        let p = |q: f64| self.samples[((n as f64 - 1.0) * q).round() as usize];
        eprintln!(
            "n={} p50={:.1}ms p90={:.1}ms p99={:.1}ms min={:.1}ms max={:.1}ms",
            n,
            p(0.50),
            p(0.90),
            p(0.99),
            self.samples[0],
            self.samples[n - 1],
        );
        self.samples.clear();
    }
}

// ---------------------------------------------------------------------------
// Send / receive
// ---------------------------------------------------------------------------

pub fn streaming(capture: &mut impl Capture) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("127.0.0.1:5000")?;

    let mut frame = vec![0u8; FRAME_SIZE];
    let mut datagram = vec![0u8; DATAGRAM_MAX];
    let mut frame_id: u32 = 0;

    loop {
        capture.next_frame(&mut frame)?;
        let capture_ts = now_nanos();
        let compressed = compress(&frame)?;
        let total_chunks = compressed.len().div_ceil(MAX_CHUNK_PAYLOAD);

        if total_chunks > CHUNKS_PER_FRAME {
            eprintln!("frame {} too large: {} chunks", frame_id, total_chunks);
            continue;
        }


        for (i, chunk) in compressed.chunks(MAX_CHUNK_PAYLOAD).enumerate() {

            let hdr = PacketHeader {
                frame_id,
                capture_ts,
                chunk_index: i as u16,
                total_chunks: total_chunks as u16,
                chunk_len: chunk.len() as u16,
                flags: FLAG_COMPRESSED,
            };
            hdr.encode(&mut datagram[..HEADER_BYTES]);
            datagram[HEADER_BYTES..HEADER_BYTES + chunk.len()].copy_from_slice(chunk);
            socket.send(&datagram[..HEADER_BYTES + chunk.len()])?;
        }
        
        frame_id = frame_id.wrapping_add(1);
    }
}

pub fn receiving(txt: Option<SyncSender<Vec<u8>>>) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:5000")?;
    let mut buf = vec![0u8; DATAGRAM_MAX];
    let mut reasm = Reassembler::new();
    let mut stats = LatencyStats::new();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut decompressed = Vec::with_capacity(FRAME_SIZE);


    loop {
        let n = socket.recv(&mut buf)?;

        let hdr = match PacketHeader::decode(&buf[..n]) {
            Some(h) => h,
            None => {
                eprintln!("malformed packet: {} bytes", n);
                continue;
            }
        };

        if hdr.frame_id != reasm.frame_id {
            if reasm.total_chunks > 0 && reasm.missing() > 0 {
                eprintln!(
                    "frame {} dropped: {} of {} chunks missing",
                    reasm.frame_id,
                    reasm.missing(),
                    reasm.total_chunks
                );
            }
            reasm.reset(hdr.frame_id);
        }

        let payload_end = HEADER_BYTES + hdr.chunk_len as usize;
        if payload_end > n {
            eprintln!("truncated packet: claims {} bytes, got {}", payload_end, n);
            continue;
        }

        if reasm.add(&hdr, &buf[HEADER_BYTES..payload_end]) {
            stats.record((now_nanos() - hdr.capture_ts) as f64/ 1_000_000.0);

            match decompress(&reasm.buf[..reasm.bytes], &mut decompressed) {
                Ok(()) => {
                    out.write_all(&decompressed)?;
                    out.flush()?;
                }
                Err(e) => eprintln!("decompress failed for frame {}: {}", reasm.frame_id, e)
            }
        }
    }
}

pub struct V4l2Capture {
    stream: ReadStream,
}

impl V4l2Capture {
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let device = Device::open(path)?;
        let capture = device.video_capture(
            PixFormat::new(WIDTH as u32,
            HEIGHT as u32,
            PixelFormat::YUYV)
        )?;
        
        // verify the driver actually gave us what we asked for
        let fmt = capture.format();
        if fmt.width() != WIDTH as u32 || fmt.height() != HEIGHT as u32 {
            return Err(format!("driver gave {}x{}", fmt.width(), fmt.height()).into());
        }
        
        let stream = capture.into_stream()?;
        Ok(Self { stream })
    }
}

impl Capture for V4l2Capture {
    fn next_frame(&mut self, out: &mut [u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.stream.dequeue(|buf| {
            let n = buf.len().min(FRAME_SIZE);
            out[..n].copy_from_slice(&buf[..n]);
            Ok(())
        })?;
        Ok(())
    }
}

fn compress(blob: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut enc = DeflateEncoder::new(Vec::new(), Compression::default());
    enc.write_all(blob)?;
    enc.finish()
}

fn decompress(bytes: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
    out.clear();
    let mut dec = DeflateDecoder::new(bytes);
    dec.read_to_end(out)?;
    Ok(())
}




fn yuyv_to_rgb(yuyv: &[u8], out: &mut [u32]) {
    for (i, chunk) in yuyv.chunks_exact(4).enumerate() {
        let (y0, u, y1, v) = (chunk[0] as i32, chunk[1] as i32,
                              chunk[2] as i32, chunk[3] as i32);
        out[i * 2]     = yuv_to_u32(y0, u, v);
        out[i * 2 + 1] = yuv_to_u32(y1, u, v);
    }
}

fn yuv_to_u32(y: i32, u: i32, v: i32) -> u32 {
    let c = y - 16;
    let d = u - 128;
    let e = v - 128;
    let r = ((298 * c + 409 * e + 128) >> 8).clamp(0, 255) as u32;
    let g = ((298 * c - 100 * d - 208 * e + 128) >> 8).clamp(0, 255) as u32;
    let b = ((298 * c + 516 * d + 128) >> 8).clamp(0, 255) as u32;
    (r << 16) | (g << 8) | b
}



pub fn display() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);

    std::thread::spawn(move || {
        // your existing receiving() loop, but instead of write_all:
        //   let _ = tx.try_send(decompressed.clone());
        // try_send, not send — never block the receiver
    });

    let event_loop = EventLoop::new()?;
    let window = WindowBuilder::new()
        .with_inner_size(LogicalSize::new(WIDTH as u32, HEIGHT as u32))
        .build(&event_loop)?;

    let context = softbuffer::Context::new(&window)?;
    let mut surface = softbuffer::Surface::new(&context, &window)?;
    surface.resize(
        NonZeroU32::new(WIDTH as u32).unwrap(),
        NonZeroU32::new(HEIGHT as u32).unwrap(),
    )?;

    event_loop.run(move |event, elwt| {
        elwt.set_control_flow(ControlFlow::Poll);
        match event {
            Event::WindowEvent { event: WindowEvent::CloseRequested, .. } => elwt.exit(),
            Event::AboutToWait => {
                if let Ok(frame) = rx.try_recv() {
                    let mut buffer = surface.buffer_mut().unwrap();
                    yuyv_to_rgb(&frame, &mut buffer);
                    buffer.present().unwrap();
                }
                window.request_redraw();
            }
            _ => {}
        }
    })?;
    Ok(())
}






























fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("send") => {
            let mut capture = FileCapture::open("raw_frames.bin")?;
            streaming(&mut capture)?;
        }
        Some("recv") => receiving()?,
        Some("cam") => {
            let mut capture =   V4l2Capture::open("/dev/video0")?;
            streaming(&mut capture)?;
        }
        _ => {
            eprintln!("Gotta pick either [send|recv]");
            std::process::exit(1);
        }
    }
    Ok(())
}