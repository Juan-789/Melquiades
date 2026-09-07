/// Codecs supported by an encoded Melquiades media frame.
///
/// The version-one wire protocol carries H.264 only. Keeping the enum inside
/// the process makes a future codec choice explicit without repeating a codec
/// tag in every packet today.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Codec {
    H264,
}

pub struct EncodedFrame {
    pub frame_id: u32,
    pub codec: Codec,
    pub source_pts_us: u64,
    pub is_keyframe: bool,
    pub has_parameter_sets: bool,
    pub bytes: Vec<u8>, // one complete H.264 access unit
}

#[cfg(target_os = "macos")]
pub mod macos;
