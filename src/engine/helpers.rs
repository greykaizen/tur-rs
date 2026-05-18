use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use super::scaler::{ProtocolFamily, Scaler};

/// Estimate current download speed from position deltas.
pub(super) fn estimate_speed_bps(started_at: Instant, start_offset: u64, current_offset: u64) -> f64 {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed <= 0.0 {
        return 0.0;
    }
    current_offset.saturating_sub(start_offset) as f64 / elapsed
}

/// Compute the prefetch trigger threshold for a given protocol and RTT.
pub(super) fn compute_prefetch_trigger_bytes(
    remaining_bytes: u64,
    recent_speed_bps: f64,
    borrow_limit_bytes: u64,
    protocol: ProtocolFamily,
    worker_rtt_ms: f64,
) -> u64 {
    let handshake_ms = protocol_prefetch_handshake_ms(protocol).max(worker_rtt_ms.round() as u64);
    let handshake_bytes = ((recent_speed_bps * (handshake_ms as f64 / 1000.0)).ceil())
        .max((super::LIVE_PREFETCH_MIN_MB * super::MB) as f64) as u64;
    let _ = remaining_bytes;
    handshake_bytes.max(borrow_limit_bytes)
}

/// Compute prefetch handshake time for a given protocol.
fn protocol_prefetch_handshake_ms(protocol: ProtocolFamily) -> u64 {
    super::http::protocol_prefetch_handshake_ms(protocol)
}

/// Determine whether a worker should prefetch more data.
pub(super) fn should_prefetch(
    remaining_bytes: u64,
    recent_speed_bps: f64,
    borrow_limit_bytes: u64,
    protocol: ProtocolFamily,
    worker_rtt_ms: f64,
) -> bool {
    remaining_bytes
        <= compute_prefetch_trigger_bytes(
            remaining_bytes,
            recent_speed_bps,
            borrow_limit_bytes,
            protocol,
            worker_rtt_ms,
        )
}

/// Update scaler signal-tracking ring buffer and return (alpha, cv).
pub(super) fn update_scaler_signal_stats(scaler: &Rc<Scaler>, sample_bps: f64) -> (f64, f64) {
    {
        let mut ring = scaler.sample_ring.borrow_mut();
        ring[scaler.sample_head.get()] = sample_bps;
    }
    scaler
        .sample_head
        .set((scaler.sample_head.get() + 1) % 10);
    scaler
        .sample_count
        .set((scaler.sample_count.get() + 1).min(10));

    let count = scaler.sample_count.get();
    if count < 5 {
        scaler.alpha.set(0.3);
        scaler.cv.set(0.0);
        return (0.3, 0.0);
    }

    let ring = scaler.sample_ring.borrow();
    let values = &ring[..count];
    let mean = values.iter().sum::<f64>() / count as f64;
    if mean < 1.0 {
        scaler.alpha.set(0.3);
        scaler.cv.set(0.0);
        return (0.3, 0.0);
    }

    let variance = values
        .iter()
        .map(|sample| {
            let delta = *sample - mean;
            delta * delta
        })
        .sum::<f64>()
        / count as f64;
    let cv = variance.sqrt() / mean;
    let alpha = (0.5 - 0.4 * cv).clamp(0.10, 0.50);
    scaler.cv.set(cv);
    scaler.alpha.set(alpha);
    (alpha, cv)
}

/// Compute the number of slow-start heartbeats for a newly added connection.
pub(super) fn compute_slow_start_heartbeats(
    scaler: &Rc<Scaler>,
    protocol: ProtocolFamily,
    is_stream_add: bool,
) -> u32 {
    let heartbeat_ms = scaler.config.borrow().heartbeat_ms.max(1) as f64;
    let estimated_ms = scaler.ewma_rtt_ms.get() * 12.0;
    let mut heartbeats = ((estimated_ms / heartbeat_ms).ceil() as u32).clamp(1, 4);

    if is_stream_add {
        return 1;
    }

    match protocol {
        ProtocolFamily::Http1 => {
            if scaler.reuse_rate.get() < 0.60 {
                heartbeats = heartbeats.saturating_add(1).clamp(1, 4);
            }
        }
        ProtocolFamily::Http2 => {
            if scaler.reuse_rate.get() > 0.70 && scaler.reused_rtt_samples.get() >= 2 {
                heartbeats = heartbeats.saturating_sub(1).clamp(1, 4);
            }
        }
        ProtocolFamily::Http3 => {
            heartbeats = heartbeats.min(2);
        }
        ProtocolFamily::Other => {}
    }
    heartbeats
}

/// Compute adaptive heartbeat interval from signal CV.
pub(super) fn compute_heartbeat_ms(scaler: &Rc<Scaler>) -> u64 {
    let cv = scaler.cv.get().max(0.0);
    ((500.0 + (cv * 2500.0)).round() as u64).clamp(500, 3000)
}

/// Compute token-bucket refill interval from global bandwidth cap.
pub(crate) fn compute_refill_interval_ms(global_bandwidth_limit_bps: u64) -> u64 {
    if global_bandwidth_limit_bps == 0 {
        return 50;
    }
    ((256_u64 * 1024).saturating_mul(1000) / global_bandwidth_limit_bps.max(1)).clamp(10, 100)
}

/// Compute the effective connection budget given system memory pressure.
pub(crate) fn compute_effective_connection_budget(
    configured_budget: usize,
    available_mb: u64,
) -> usize {
    let configured_min = configured_budget.min(2).max(1);
    if available_mb < 256 {
        configured_min.max(configured_budget / 4)
    } else if available_mb < 512 {
        configured_min.max(configured_budget / 2)
    } else if available_mb < 1024 {
        configured_min.max(configured_budget.saturating_mul(3) / 4)
    } else {
        configured_budget
    }
}

/// Compute the coefficient of variation across a slice of connection speed samples.
pub(super) fn compute_connection_cv(samples: &[f64]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    if mean < 1.0 {
        return None;
    }
    let variance = samples
        .iter()
        .map(|sample| {
            let delta = *sample - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;
    Some((variance.sqrt() / mean).max(0.0))
}

/// Track and log reuse-rate health; signals degraded / recovered transitions.
pub(super) async fn update_reuse_health(
    scaler: &Rc<Scaler>,
    url: &str,
    log_path: &Path,
    protocol: ProtocolFamily,
) {
    let now = Instant::now();
    if now.duration_since(scaler.last_reuse_reset.get()) >= Duration::from_secs(60) {
        scaler.reused_count.set(0);
        scaler.total_request_count.set(0);
        scaler.reuse_rate.set(1.0);
        scaler.last_reuse_reset.set(now);
    }

    let rate = if scaler.total_request_count.get() == 0 {
        scaler.reuse_rate.get()
    } else {
        scaler.reused_count.get() as f64 / scaler.total_request_count.get() as f64
    };
    scaler.reuse_rate.set(rate);
    let threshold = super::http::protocol_effective_add_threshold(protocol, rate).clamp(0.04, 0.15);
    scaler.effective_add_threshold.set(threshold);
    let (degraded_threshold, recovered_threshold) = super::http::protocol_reuse_thresholds(protocol);

    let was_low = scaler.reuse_health_low.get();
    if !was_low && rate < degraded_threshold {
        scaler.reuse_health_low.set(true);
        log_phase_a_info(
            log_path,
            &format!(
                "reuse health degraded url={} protocol={} reuse_rate={:.2} effective_add_threshold={:.2}",
                url,
                protocol.as_str(),
                rate,
                threshold
            ),
        )
        .await;
    } else if was_low && rate > recovered_threshold {
        scaler.reuse_health_low.set(false);
        log_phase_a_info(
            log_path,
            &format!(
                "reuse health recovered url={} protocol={} reuse_rate={:.2} effective_add_threshold={:.2}",
                url,
                protocol.as_str(),
                rate,
                threshold
            ),
        )
        .await;
    }
}

/// Write a timestamped log line to the phase-a log file.
pub(super) async fn log_phase_a_info(log_path: &Path, msg: &str) {
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await
    {
        let _ = f
            .write_all(format!("[{}] phase_a: {}\n", chrono::Local::now(), msg).as_bytes())
            .await;
    }
}


