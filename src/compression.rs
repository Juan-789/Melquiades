//! The legacy raw-frame Deflate codec.
//!
//! H.264 is a video codec with stateful, platform-specific encoder and
//! decoder sessions.  It intentionally lives in `crate::codec`, rather than
//! being mixed into these stateless byte helpers.

use std::io::{Read, Write};

use flate2::Compression;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;

pub fn compress(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(bytes)?;
    encoder.finish()
}

pub fn decompress(bytes: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
    out.clear();
    DeflateDecoder::new(bytes).read_to_end(out)?;
    Ok(())
}
