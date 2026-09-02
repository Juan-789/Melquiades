pub const WIDTH: usize = 640;
pub const HEIGHT: usize = 480;
pub const FRAME_SIZE: usize = WIDTH * HEIGHT * 2; // YUYV = 2 bytes/px
/// Raw capture slots allocated at startup. At 640x480 YUYV this is 2.34 MiB.
pub const FRAME_POOL_SLOTS: usize = 4;
pub const MAX_CHUNK_PAYLOAD: usize = 1200;
pub const CHUNKS_PER_FRAME: usize = FRAME_SIZE.div_ceil(MAX_CHUNK_PAYLOAD);
pub const HEADER_BYTES: usize = 21;
pub const ECHO_BYTES: usize = 20;
pub const MAGIC: u16 = 0xADC0;
pub const DATAGRAM_MAX: usize = HEADER_BYTES + MAX_CHUNK_PAYLOAD;
pub const FLAG_COMPRESSED: u8 = 1;
/// Experimental pacing between accepted UDP datagrams. This smooths one frame's
/// packet burst so we can measure whether queue pressure is causing loss.
pub const INTER_PACKET_GAP_US: u64 = 25;
