# Melquiades: Current Findings

Measured on August 31, 2026.

## Test configuration

- Sender: ThinkPad T14 Gen 1, Intel i5-10310U, Linux.
- Receiver: Mac mini, macOS, 60 Hz display.
- Source: built-in camera, 640×480 YUYV, 30 FPS.
- Transport: UDP over LAN, with payloads of up to 1,200 bytes.
- Compression: fast DEFLATE.
- Display: `winit` and `softbuffer`.
- Builds: Rust release mode.

The camera must use:

```text
exposure_dynamic_framerate = 0
power_line_frequency = 60 Hz
```

With dynamic frame rate enabled, the camera reported 30 FPS but delivered approximately 15 FPS. Disabling it restored a genuine frame period of approximately 33 ms.

## Current measurements

Representative steady-state medians:

| Stage | Time |
|---|---:|
| Wait for next camera frame | 17–18 ms |
| Fast DEFLATE compression | 8.4–8.8 ms |
| UDP send loop | 6.5–6.7 ms |
| First-to-final chunk arrival | 8.3–8.9 ms |
| DEFLATE decompression | 2.2–2.3 ms |
| Queue and YUYV-to-RGB conversion | 0.18–0.20 ms |
| `softbuffer::present()` call | 0.67–0.69 ms |
| Receiver processing total | 3.1–3.2 ms |

The UDP send loop and network arrival overlap and must not be added as independent sequential stages.

The complete sender iteration is approximately 33 ms, so the pipeline sustains the camera's 30 FPS rate.

## Compression and transport

A raw frame is 614,400 bytes. Fast DEFLATE currently produces approximately:

```text
Compressed payload: 434 KB per frame
Payload reduction:  29%
UDP chunks:          362 per frame
```

In the latest observed run, 900 of 900 frames completed reassembly:

```text
Completion rate:     100%
Dropped frames:      0
Late packets:        0
```

The ordinary UDP path is therefore reliable under the current LAN conditions. This does not guarantee the same result under different network load or hardware.

## Latency estimate

Current estimated camera-to-screen, or glass-to-glass, latency:

```text
Typical: approximately 45–60 ms
```

This is an estimate, not a physical measurement. It combines the measured software stages with estimated camera phase, display refresh, compositor scheduling, and network transit.

The following remain unmeasured:

- Camera exposure and sensor readout latency.
- Exact one-way network transit time.
- macOS compositor buffering.
- Physical display scanout and pixel response.

A high-speed camera or LED-and-photodiode setup is required for a defensible physical glass-to-glass result.

## Other finding

A simultaneous photograph of Linux and Mac displays during Google Meet screen sharing showed a timestamp difference of:

```text
186.7 ms
```

This is one screen-to-screen sample, not a full latency distribution and not a direct comparison with the current camera-to-screen Melquiades pipeline.

## Next engineering milestone

Replace cloned frame buffers and the synchronous channel with a preallocated frame pool and bounded SPSC ownership transfer. This provides the foundation for pinned pipeline stages and native low-latency display backends.
