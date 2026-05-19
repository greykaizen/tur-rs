use super::*;

#[derive(Debug, Clone, Copy)]
pub struct ScalerConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub heartbeat_ms: u64,
}

impl Default for ScalerConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 16,
            heartbeat_ms: 2000,
        }
    }
}

pub struct TokenBucket {
    pub quota_bytes_per_sec: Cell<u64>,
    pub tokens: Cell<i64>,
    pub refill_interval_ms: Cell<u64>,
}

impl TokenBucket {
    pub fn new() -> Self {
        Self {
            quota_bytes_per_sec: Cell::new(0),
            tokens: Cell::new(0),
            refill_interval_ms: Cell::new(100),
        }
    }

    pub fn refill(&self, interval_ms: u64) {
        let quota = self.quota_bytes_per_sec.get();
        if quota == 0 {
            self.tokens.set(i64::MAX / 2);
            return;
        }
        let add = (quota * interval_ms / 1000) as i64;
        let cap = (quota * 2) as i64;
        self.tokens.set((self.tokens.get() + add).min(cap));
    }

    pub fn consume(&self, bytes: usize) -> bool {
        let quota = self.quota_bytes_per_sec.get();
        if quota == 0 {
            return true;
        }
        let remaining = self.tokens.get() - bytes as i64;
        self.tokens.set(remaining);
        remaining >= 0
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ScalerAction {
    Grow,
    Shrink,
    Hold,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(crate) enum ProtocolFamily {
    Http1,
    Http2,
    Http3,
    Other,
}

impl Default for ProtocolFamily {
    fn default() -> Self {
        Self::Other
    }
}

impl ProtocolFamily {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
            Self::Http3 => "http3",
            Self::Other => "other",
        }
    }
}

pub(crate) struct Scaler {
    pub ewma_throughput: Cell<f64>,
    pub peak_efficiency: Cell<f64>,
    pub throughput_before_add: Cell<f64>,
    pub n_active: Cell<usize>,
    pub last_action: Cell<ScalerAction>,
    pub slow_start_remaining: Cell<u32>,
    pub config: Rc<RefCell<ScalerConfig>>,
    pub sample_ring: RefCell<[f64; 10]>,
    pub sample_head: Cell<usize>,
    pub sample_count: Cell<usize>,
    pub alpha: Cell<f64>,
    pub cv: Cell<f64>,
    pub ewma_rtt_ms: Cell<f64>,
    pub ewma_handshake_ms: Cell<f64>,
    pub reused_count: Cell<u64>,
    pub total_request_count: Cell<u64>,
    pub reuse_rate: Cell<f64>,
    pub last_reuse_reset: Cell<Instant>,
    pub effective_add_threshold: Cell<f64>,
    pub reused_rtt_samples: Cell<u64>,
    pub skip_growth_sample: Cell<bool>,
    pub reuse_health_low: Cell<bool>,
    pub last_add_was_stream: Cell<bool>,
    pub h2_stream_count: Cell<usize>,
    pub h2_stream_saturated: Cell<bool>,
    pub(super) last_protocol: Cell<ProtocolFamily>,
}

impl Scaler {
    pub fn from_config_handle(config: Rc<RefCell<ScalerConfig>>) -> Rc<Self> {
        Rc::new(Self {
            ewma_throughput: Cell::new(0.0),
            peak_efficiency: Cell::new(0.0),
            throughput_before_add: Cell::new(0.0),
            n_active: Cell::new(1),
            last_action: Cell::new(ScalerAction::Hold),
            slow_start_remaining: Cell::new(3),
            config,
            sample_ring: RefCell::new([0.0; 10]),
            sample_head: Cell::new(0),
            sample_count: Cell::new(0),
            alpha: Cell::new(0.3),
            cv: Cell::new(0.0),
            ewma_rtt_ms: Cell::new(200.0),
            ewma_handshake_ms: Cell::new(50.0),
            reused_count: Cell::new(0),
            total_request_count: Cell::new(0),
            reuse_rate: Cell::new(1.0),
            last_reuse_reset: Cell::new(Instant::now()),
            effective_add_threshold: Cell::new(0.05),
            reused_rtt_samples: Cell::new(0),
            skip_growth_sample: Cell::new(false),
            reuse_health_low: Cell::new(false),
            last_add_was_stream: Cell::new(false),
            h2_stream_count: Cell::new(0),
            h2_stream_saturated: Cell::new(false),
            last_protocol: Cell::new(ProtocolFamily::Other),
        })
    }
}
