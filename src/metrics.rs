use std::time::{Duration, Instant};

const REPORT_FRAMES: usize = 300;

pub struct FrameTimings {
    pub t0_compressed_frame_complete: Instant,
    pub t1_decode_begins: Instant,
    pub t2_decode_ends: Instant,
}

pub struct SenderTimings {
    pub s0_capture_begins: Instant,
    pub s1_frame_acquired: Instant,
    pub s2_compression_begins: Instant,
    pub s3_compression_ends: Instant,
    pub s4_first_datagram_accepted: Instant,
    pub s5_final_datagram_accepted: Instant,
}

pub struct SenderStats {
    capture_wait: Vec<f64>,
    handoff_to_compression: Vec<f64>,
    compression: Vec<f64>,
    compression_to_first_send: Vec<f64>,
    send_loop: Vec<f64>,
    sender_total: Vec<f64>,
    frame_path: Vec<f64>,
}

impl SenderStats {
    pub fn new() -> Self {
        Self {
            capture_wait: samples(),
            handoff_to_compression: samples(),
            compression: samples(),
            compression_to_first_send: samples(),
            send_loop: samples(),
            sender_total: samples(),
            frame_path: samples(),
        }
    }

    pub fn record(&mut self, timing: &SenderTimings) {
        self.capture_wait
            .push(between(timing.s0_capture_begins, timing.s1_frame_acquired));
        self.handoff_to_compression.push(between(
            timing.s1_frame_acquired,
            timing.s2_compression_begins,
        ));
        self.compression.push(between(
            timing.s2_compression_begins,
            timing.s3_compression_ends,
        ));
        self.compression_to_first_send.push(between(
            timing.s3_compression_ends,
            timing.s4_first_datagram_accepted,
        ));
        self.send_loop.push(between(
            timing.s4_first_datagram_accepted,
            timing.s5_final_datagram_accepted,
        ));
        self.sender_total.push(between(
            timing.s2_compression_begins,
            timing.s5_final_datagram_accepted,
        ));
        self.frame_path.push(between(
            timing.s0_capture_begins,
            timing.s5_final_datagram_accepted,
        ));

        if self.frame_path.len() == REPORT_FRAMES {
            eprintln!("capture stage over {REPORT_FRAMES} transmitted frames:");
            report("C0→C1 capture_wait", &mut self.capture_wait, "us");

            eprintln!("capture-to-sender handoff over {REPORT_FRAMES} transmitted frames:");
            report(
                "C1→S2 ready_to_compression",
                &mut self.handoff_to_compression,
                "us",
            );

            eprintln!("sender work over {REPORT_FRAMES} transmitted frames:");
            report("S2→S3 compression", &mut self.compression, "us");
            report(
                "S3→S4 packetize_to_first_socket_accept",
                &mut self.compression_to_first_send,
                "us",
            );
            report("S4→S5 send_loop", &mut self.send_loop, "us");
            report("S2→S5 sender_work_total", &mut self.sender_total, "us");

            eprintln!("per-frame path over {REPORT_FRAMES} transmitted frames:");
            report(
                "C0→S5 capture_begin_to_final_socket_accept",
                &mut self.frame_path,
                "us",
            );
        }
    }
}

pub struct CompressionStats {
    compression_us: Vec<f64>,
    compressed_bytes: Vec<f64>,
    application_bytes: Vec<f64>,
    ratios: Vec<f64>,
    reductions: Vec<f64>,
    chunks: Vec<f64>,
}

impl CompressionStats {
    pub fn new() -> Self {
        Self {
            compression_us: samples(),
            compressed_bytes: samples(),
            application_bytes: samples(),
            ratios: samples(),
            reductions: samples(),
            chunks: samples(),
        }
    }

    pub fn record(
        &mut self,
        raw_bytes: usize,
        compressed_bytes: usize,
        chunks: usize,
        header_bytes: usize,
        compression_time: Duration,
    ) {
        self.compression_us.push(micros(compression_time));
        self.compressed_bytes.push(compressed_bytes as f64);
        self.application_bytes
            .push((compressed_bytes + chunks * header_bytes) as f64);
        self.ratios.push(raw_bytes as f64 / compressed_bytes as f64);
        self.reductions
            .push(100.0 * (1.0 - compressed_bytes as f64 / raw_bytes as f64));
        self.chunks.push(chunks as f64);

        if self.compression_us.len() == REPORT_FRAMES {
            eprintln!("compression over {REPORT_FRAMES} transmitted frames (raw={raw_bytes}B):");
            report("compression_time", &mut self.compression_us, "us");
            report("compressed_size", &mut self.compressed_bytes, "B");
            report(
                "application_datagram_bytes",
                &mut self.application_bytes,
                "B",
            );
            report("compression_ratio", &mut self.ratios, "x");
            report("payload_reduction", &mut self.reductions, "%");
            report("chunks", &mut self.chunks, "");
        }
    }
}

pub struct ReassemblyStats {
    arrival_spread: Vec<f64>,
    completed: u64,
    dropped: u64,
    late_packets: u64,
    missing_chunks: u64,
    outcomes: usize,
}

impl ReassemblyStats {
    pub fn new() -> Self {
        Self {
            arrival_spread: samples(),
            completed: 0,
            dropped: 0,
            late_packets: 0,
            missing_chunks: 0,
            outcomes: 0,
        }
    }

    pub fn record_complete(&mut self, r0: Instant, r1: Instant) {
        self.arrival_spread.push(between(r0, r1));
        self.completed += 1;
        self.outcomes += 1;
        self.report_if_ready();
    }

    pub fn record_drop(&mut self, missing_chunks: u16) {
        self.dropped += 1;
        self.missing_chunks += missing_chunks as u64;
        self.outcomes += 1;
        self.report_if_ready();
    }

    pub fn record_late_packet(&mut self) {
        self.late_packets += 1;
    }

    fn report_if_ready(&mut self) {
        if self.outcomes != REPORT_FRAMES {
            return;
        }
        let completion_rate = 100.0 * self.completed as f64 / self.outcomes as f64;
        eprintln!("reassembly over {REPORT_FRAMES} frame outcomes:");
        eprintln!(
            "frames: completed={} dropped={} completion_rate={:.2}% late_packets_ignored={} missing_chunks_at_abandonment={}",
            self.completed, self.dropped, completion_rate, self.late_packets, self.missing_chunks,
        );
        if !self.arrival_spread.is_empty() {
            report("R0→R1 first_to_final_chunk", &mut self.arrival_spread, "us");
        }
        self.completed = 0;
        self.dropped = 0;
        self.late_packets = 0;
        self.missing_chunks = 0;
        self.outcomes = 0;
    }
}

pub struct PipelineStats {
    complete_to_decode: Vec<f64>,
    decode: Vec<f64>,
    decoded_to_submission: Vec<f64>,
    submission_to_present_return: Vec<f64>,
    complete_to_present_return: Vec<f64>,
}

impl PipelineStats {
    pub fn new() -> Self {
        Self {
            complete_to_decode: samples(),
            decode: samples(),
            decoded_to_submission: samples(),
            submission_to_present_return: samples(),
            complete_to_present_return: samples(),
        }
    }

    pub fn record(&mut self, timings: &FrameTimings, t3: Instant, t4: Instant) {
        self.complete_to_decode.push(between(
            timings.t0_compressed_frame_complete,
            timings.t1_decode_begins,
        ));
        self.decode
            .push(between(timings.t1_decode_begins, timings.t2_decode_ends));
        self.decoded_to_submission
            .push(between(timings.t2_decode_ends, t3));
        self.submission_to_present_return.push(between(t3, t4));
        self.complete_to_present_return
            .push(between(timings.t0_compressed_frame_complete, t4));

        if self.complete_to_present_return.len() == REPORT_FRAMES {
            eprintln!("receiver pipeline over {REPORT_FRAMES} displayed frames:");
            report(
                "T0→T1 complete_to_decode",
                &mut self.complete_to_decode,
                "us",
            );
            report("T1→T2 decode", &mut self.decode, "us");
            report(
                "T2→T3 queue_convert_to_submission",
                &mut self.decoded_to_submission,
                "us",
            );
            report(
                "T3→T4 present_call",
                &mut self.submission_to_present_return,
                "us",
            );
            report(
                "T0→T4 receiver_pipeline",
                &mut self.complete_to_present_return,
                "us",
            );
        }
    }
}

fn samples() -> Vec<f64> {
    Vec::with_capacity(REPORT_FRAMES)
}

fn between(start: Instant, end: Instant) -> f64 {
    micros(end.duration_since(start))
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn report(label: &str, samples: &mut Vec<f64>, unit: &str) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let count = samples.len();
    let percentile = |q: f64| samples[((count as f64 - 1.0) * q).round() as usize];
    eprintln!(
        "{label}: p50={:.2}{unit} p90={:.2}{unit} p99={:.2}{unit} min={:.2}{unit} max={:.2}{unit}",
        percentile(0.50),
        percentile(0.90),
        percentile(0.99),
        samples[0],
        samples[count - 1],
    );
    samples.clear();
}
