use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Seek, Write};
use std::net::UdpSocket;
use std::thread::sleep;
use std::time::Duration;

const WIDTH: usize = 640;
const HEIGHT: usize = 640;
const FRAME_SIZE: usize = WIDTH*HEIGHT;

//wire format the format at which the packets will look like 
#[repr(C)]
struct Packet {
    frame_number: u32,   //ig this could loopback and have no problem
    payload: [u8; FRAME_SIZE],  //maybe less considering our initial test is small
}
impl Packet {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len()<FRAME_SIZE+4{
            return None; //corrupt case
        }
        let frame_number=u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let mut payload = [0u8; FRAME_SIZE];
        payload.copy_from_slice(&bytes[4..4+FRAME_SIZE]);

        return Some(Self { frame_number, payload })
    }
    
    pub fn encode(&self, out: &mut [u8]) {
        //this load is 4 bytes, the first is location, and the rest is color
        out[0..4].copy_from_slice(&self.frame_number.to_be_bytes());
        out[4..4+1536].copy_from_slice(&self.payload);
    }
}
pub trait Capture {
    fn next_frame(&mut self, out: &mut [u8]) -> Result<(), Box< dyn  std::error::Error>>;
}


pub fn streaming() -> Result<(), Box<dyn std::error::Error>>{
    let file= File::open("raw_frames.bin")?; // would be easy to read all this but we can simulate like as if it was reading realtime by adding time
    let mut reader = BufReader::new(file);
    let file_len = reader.get_ref().metadata()?.len();
    // let total_frames = file_len / FRAME_SIZE as u64;
    
    let mut frame_data = [0u8; FRAME_SIZE];
    let mut frame_number = 0;

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("127.0.0.1:5000")?;

    loop {
        reader.read_exact(&mut frame_data)?;
        if reader.stream_position()? == reader.get_ref().metadata()?.len() {
            reader.rewind()?;
        }

        let packet = Packet {
            frame_number,
            payload: frame_data,
        };

        let mut encoded_data = [0u8; FRAME_SIZE + 4];
        packet.encode(&mut encoded_data);

        socket.send(&encoded_data)?;

        frame_number = frame_number.wrapping_add(1);

        sleep(Duration::from_millis(33)); //fps is 30
    }

}


pub fn receiving() -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("127.0.0.1:5000")?;
    let mut buf = [0u8; FRAME_SIZE+4];
    let mut next_expected: u32 = 0;
    loop {
        let n = socket.recv(&mut buf)?;
        match Packet::decode(&buf[..n]) {
            Some(packet) => {
                if packet.frame_number == next_expected {
                    std::io::stdout().write_all(&packet.payload)?;
                    next_expected = packet.frame_number.wrapping_add(1);
                } else {
                    eprintln!("gap expected: {} bytes (expected {})", n, FRAME_SIZE + 4);
                }

            }
            _ => {
                eprintln!("malformed packet: {} bytes (expected {})", n, FRAME_SIZE + 4);
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let args: Vec<String> = env::args().collect();
    
    match args.get(1).map(|s| s.as_str()) {
        Some("send") => streaming()?,
        Some("recv") => receiving()?,
        _ => {
            eprintln!("Gotta pick either [send|recv]");
            std::process::exit(1);
        }
    }
    return Ok(());
}
