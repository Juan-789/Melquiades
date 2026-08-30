use std::time::Instant;

pub struct LatencyStats {
    samples: Vec<f64>,
}

pub struct FrameTimings {
    pub t0_compressed_frame_complete: Instant,
    pub t1_decode_begins: Instant,
    pub t2_decode_ends: Instant,
}

pub struct PipelineStats {
    complete_to_decode: Vec<f64>,
    decode: Vec<f64>,
    decoded_to_submission: Vec<f64>,
    submission_to_present_return: Vec<f64>,
    complete_to_present_return: Vec<f64>,
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
            compression_us: Vec::with_capacity(30),
            compressed_bytes: Vec::with_capacity(30),
            application_bytes: Vec::with_capacity(30),
            ratios: Vec::with_capacity(30),
            reductions: Vec::with_capacity(30),
            chunks: Vec::with_capacity(30),
        }
    }

    pub fn record(
        &mut self,
        raw_bytes: usize,
        compressed_bytes: usize,
        chunks: usize,
        header_bytes: usize,
        compression_time: std::time::Duration,
    ) {
        self.compression_us.push(micros(compression_time));
        self.compressed_bytes.push(compressed_bytes as f64);
        self.application_bytes
            .push((compressed_bytes + chunks * header_bytes) as f64);
        self.ratios.push(raw_bytes as f64 / compressed_bytes as f64);
        self.reductions
            .push(100.0 * (1.0 - compressed_bytes as f64 / raw_bytes as f64));
        self.chunks.push(chunks as f64);

        if self.compression_us.len() == 30 {
            eprintln!("compression statistics over 30 captured frames (raw={raw_bytes}B):");
            report_values("compression_time", &mut self.compression_us, "us");
            report_values("compressed_size", &mut self.compressed_bytes, "B");
            report_values(
                "application_datagram_bytes",
                &mut self.application_bytes,
                "B",
            );
            report_values("compression_ratio", &mut self.ratios, "x");
            report_values("payload_reduction", &mut self.reductions, "%");
            report_values("chunks", &mut self.chunks, "");
        }
    }
}

impl PipelineStats {
    pub fn new() -> Self {
        Self {
            complete_to_decode: Vec::with_capacity(30),
            decode: Vec::with_capacity(30),
            decoded_to_submission: Vec::with_capacity(30),
            submission_to_present_return: Vec::with_capacity(30),
            complete_to_present_return: Vec::with_capacity(30),
        }
    }

    pub fn record(&mut self, timings: &FrameTimings, t3: Instant, t4: Instant) {
        self.complete_to_decode.push(micros(
            timings
                .t1_decode_begins
                .duration_since(timings.t0_compressed_frame_complete),
        ));
        self.decode.push(micros(
            timings
                .t2_decode_ends
                .duration_since(timings.t1_decode_begins),
        ));
        self.decoded_to_submission
            .push(micros(t3.duration_since(timings.t2_decode_ends)));
        self.submission_to_present_return
            .push(micros(t4.duration_since(t3)));
        self.complete_to_present_return.push(micros(
            t4.duration_since(timings.t0_compressed_frame_complete),
        ));

        if self.complete_to_present_return.len() == 30 {
            eprintln!("pipeline timings over 30 displayed frames:");
            report_stage("T0→T1 complete_to_decode", &mut self.complete_to_decode);
            report_stage("T1→T2 decode", &mut self.decode);
            report_stage(
                "T2→T3 queue_convert_to_submission",
                &mut self.decoded_to_submission,
            );
            report_stage("T3→T4 present_call", &mut self.submission_to_present_return);
            report_stage(
                "T0→T4 receiver_pipeline",
                &mut self.complete_to_present_return,
            );
        }
    }
}

fn micros(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn report_stage(label: &str, samples: &mut Vec<f64>) {
    report_values(label, samples, "us");
}

fn report_values(label: &str, samples: &mut Vec<f64>, unit: &str) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let count = samples.len();
    let percentile = |q: f64| samples[((count as f64 - 1.0) * q).round() as usize];
    eprintln!(
        "{label}: p50={:.1}{unit} p90={:.1}{unit} p99={:.1}{unit} min={:.1}{unit} max={:.1}{unit}",
        percentile(0.50),
        percentile(0.90),
        percentile(0.99),
        samples[0],
        samples[count - 1],
    );
    samples.clear();
}

impl LatencyStats {
    pub fn new() -> Self {
        Self {
            samples: Vec::with_capacity(64),
        }
    }

    pub fn record(&mut self, milliseconds: f64) {
        self.samples.push(milliseconds);
        if self.samples.len() >= 30 {
            self.report();
        }
    }

    fn report(&mut self) {
        self.samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let count = self.samples.len();
        let percentile = |q: f64| self.samples[((count as f64 - 1.0) * q).round() as usize];
        eprintln!(
            "n={} p50={:.1}ms p90={:.1}ms p99={:.1}ms min={:.1}ms max={:.1}ms",
            count,
            percentile(0.50),
            percentile(0.90),
            percentile(0.99),
            self.samples[0],
            self.samples[count - 1],
        );
        self.samples.clear();
    }
}
