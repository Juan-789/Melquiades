pub fn yuyv_to_rgb(yuyv: &[u8], out: &mut [u32]) {
    for (index, chunk) in yuyv.chunks_exact(4).enumerate() {
        let (y0, u, y1, v) = (
            chunk[0] as i32,
            chunk[1] as i32,
            chunk[2] as i32,
            chunk[3] as i32,
        );
        out[index * 2] = yuv_to_u32(y0, u, v);
        out[index * 2 + 1] = yuv_to_u32(y1, u, v);
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
