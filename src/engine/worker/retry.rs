use super::*;

impl ConnectionWorker {
    pub(super) async fn handle_range_retry(
        &self,
        range: &Rc<ActiveRange>,
        current_start: u64,
        current_end: u64,
        consecutive_failures: u32,
        reason: &str,
        retry_hint: RetryHint,
    ) -> Result<u32> {
        let next_failures = consecutive_failures.saturating_add(1);
        SchedulerMetrics::add(&self.metrics.retry_attempts, 1);

        if matches!(retry_hint, RetryHint::Abort) || next_failures > MAX_RANGE_RETRIES {
            return Err(anyhow!(
                "range#{} failed after {} retries at bytes {}..{}: {}",
                range.id,
                consecutive_failures,
                current_start,
                current_end,
                reason
            ));
        }

        let delay_ms = match retry_hint {
            RetryHint::Immediate => 0,
            RetryHint::Backoff(ms) => ms,
            RetryHint::ReduceWorkers(ms) => {
                self.scaler.skip_growth_sample.set(true);
                self.scaler
                    .slow_start_remaining
                    .set(self.scaler.slow_start_remaining.get().saturating_add(1));
                self.worker_control.stop_requested.set(true);
                self.relinquish_range(range, current_start).await;
                ms
            }
            RetryHint::ShrinkRange(ms) => ms,
            RetryHint::Abort => 0,
        };
        SchedulerMetrics::add(&self.metrics.retry_wait_ms, delay_ms);
        self.log_msg(&format!(
            "{}; retry_hint={:?} retry {}/{} after {}ms on range#{} bytes={}..{}",
            reason,
            retry_hint,
            next_failures,
            MAX_RANGE_RETRIES,
            delay_ms,
            range.id,
            current_start,
            current_end
        ))
        .await;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        Ok(next_failures)
    }
}

pub(super) fn classify_http_status_retry(status: ::http::StatusCode) -> RetryHint {
    match status.as_u16() {
        429 => RetryHint::ReduceWorkers(RETRY_MAX_DELAY_MS),
        503 => RetryHint::Immediate,
        500..=599 => RetryHint::Backoff(RETRY_BASE_DELAY_MS.saturating_mul(2)),
        416 => RetryHint::Abort,
        _ => RetryHint::Backoff(RETRY_BASE_DELAY_MS),
    }
}

pub(super) fn classify_error_retry(reason: &str, made_progress: bool) -> RetryHint {
    let reason = reason.to_ascii_lowercase();
    if reason.contains("tls") || reason.contains("ssl") || reason.contains("handshake") {
        return RetryHint::ReduceWorkers(RETRY_MAX_DELAY_MS);
    }
    if reason.contains("connection reset") || reason.contains("broken pipe") {
        return if made_progress {
            RetryHint::ShrinkRange(RETRY_BASE_DELAY_MS)
        } else {
            RetryHint::ReduceWorkers(RETRY_BASE_DELAY_MS.saturating_mul(4))
        };
    }
    if reason.contains("timed out") || reason.contains("timeout") {
        return if made_progress {
            RetryHint::ShrinkRange(RETRY_BASE_DELAY_MS)
        } else {
            RetryHint::Backoff(RETRY_BASE_DELAY_MS.saturating_mul(3))
        };
    }
    RetryHint::Backoff(RETRY_BASE_DELAY_MS)
}
