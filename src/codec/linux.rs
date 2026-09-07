//! Linux H.264 decode boundary backed by Cisco OpenH264.
//!
//! This is intentionally a correctness-first software decoder. It accepts
//! decoder-ready Annex B access units from the UDP transport and produces
//! tightly packed BGRA pixels for the existing Softbuffer renderer. Hardware
//! decode through VA-API is a later, separately measurable optimization.

use std::error::Error;

use openh264::{decoder::Decoder, formats::YUVSource};

use crate::capture::{PixelFormat, StreamSpec};

/// An owned displayable image decoded from one complete H.264 access unit.
pub struct DecodedH264Frame {
    pub pixels: Vec<u8>,
    pub stream: StreamSpec,
}

/// Persistent OpenH264 decoder plus reusable conversion storage.
///
/// OpenH264 exposes borrowed YUV420 planes, valid only until the next decode.
/// We convert them before returning, so no decoder-owned pointer crosses into
/// the window thread.
pub struct LinuxH264Decoder {
    decoder: Decoder,
    bgra: Vec<u8>,
}

impl LinuxH264Decoder {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            decoder: Decoder::new()?,
            bgra: Vec::new(),
        })
    }

    /// Decodes one complete Annex B access unit.
    ///
    /// A `None` result is normal while the decoder is waiting for a complete
    /// parameter-set/keyframe combination. The UDP receiver enforces that it
    /// starts after a keyframe containing SPS/PPS, rather than treating this
    /// as a displayed black frame.
    pub fn decode_access_unit(
        &mut self,
        annex_b_access_unit: &[u8],
    ) -> Result<Option<DecodedH264Frame>, Box<dyn Error>> {
        let Some(yuv) = self.decoder.decode(annex_b_access_unit)? else {
            return Ok(None);
        };
        let (width, height) = yuv.dimensions();
        let stream = StreamSpec::new(width as u32, height as u32, PixelFormat::Bgra8888)?;
        self.bgra.resize(stream.byte_len, 0);

        // OpenH264's optimized helper writes RGBA. The existing renderer's
        // Bgra8888 path expects BGRA, so exchange R and B in-place after the
        // required YUV420 -> RGB conversion.
        yuv.write_rgba8(&mut self.bgra);
        for pixel in self.bgra.chunks_exact_mut(4) {
            pixel.swap(0, 2);
        }

        // The display channel owns its frame independently of the decoder.
        // This copy is explicit and temporary; an output FramePool is the
        // future optimization that will remove it without leaking decoder
        // borrows across threads.
        Ok(Some(DecodedH264Frame {
            pixels: self.bgra.clone(),
            stream,
        }))
    }
}
