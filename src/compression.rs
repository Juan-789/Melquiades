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
