//wire format the format at which the packets will look like 


#[repr(C)]
struct PacketFormat {
    frame_number: u32,   //ig this could loopback and have no problem
    payload: [u8; 1536],  //maybe less considering our initial test is small
}
impl PacketFormat {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len()<1536+4{
            return None; //corrupt case
        }
        let frame_number=u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let mut payload = [0u8; 1536];
        payload.copy_from_slice(&bytes[4..4+1536]);
        
        return Some(Self { frame_number, payload })
    }
    
    pub fn encode(&self, out: &mut [u8]) {
        //this load is 4 bytes, the first is location, and the rest is color
        out[0..4].copy_from_slice(&self.frame_number.to_be_bytes());
        out[4..4+1536].copy_from_slice(&self.payload);
    }
}

fn main() {
    println!("Hello, world!");
}
