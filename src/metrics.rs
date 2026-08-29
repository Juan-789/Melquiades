pub struct LatencyStats {
    samples: Vec<f64>,
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
