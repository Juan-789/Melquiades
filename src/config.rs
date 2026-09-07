pub const WIDTH: usize = 640;
pub const HEIGHT: usize = 480;
pub const FRAME_SIZE: usize = WIDTH * HEIGHT * 2; // YUYV = 2 bytes/px
/// Raw capture slots allocated at startup. At 640x480 YUYV this is 2.34 MiB;
/// at 1920x1080 BGRA it is 31.6 MiB.
pub const FRAME_POOL_SLOTS: usize = 4;
pub const MAX_CHUNK_PAYLOAD: usize = 1200;
/// Largest raw image accepted by the experimental LAN receiver: 4K BGRA.
/// This bounds allocations caused by an untrusted or malformed UDP header.
pub const MAX_RAW_FRAME_BYTES: usize = 3840 * 2160 * 4;
pub const MAX_COMPRESSED_FRAME_BYTES: usize = MAX_RAW_FRAME_BYTES + (1 << 20);
/// The current Deflate transport is retained while the H.264 encoder and
/// decoder are built. Its header is deliberately named legacy so it cannot be
/// mistaken for the H.264 access-unit protocol in `wire.rs`.
pub const LEGACY_RAW_HEADER_BYTES: usize = 30;
pub const ECHO_BYTES: usize = 20;
pub const LEGACY_RAW_MAGIC: u16 = 0xADC0;
pub const LEGACY_FLAG_COMPRESSED: u8 = 1;
pub const DATAGRAM_MAX: usize = LEGACY_RAW_HEADER_BYTES + MAX_CHUNK_PAYLOAD;
/// Experimental pacing between accepted UDP datagrams. This smooths one frame's
/// packet burst so we can measure whether queue pressure is causing loss.
pub const INTER_PACKET_GAP_US: u64 = 25;
