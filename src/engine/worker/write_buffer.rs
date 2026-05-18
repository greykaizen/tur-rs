use super::*;

impl ConnectionWorker {
    pub(super) async fn flush_pending_write(
        &self,
        write_tx: &tokio::sync::mpsc::Sender<(u64, Vec<u8>)>,
        recycle_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        pending: &mut PendingWrite,
        attempt_timing: &mut AttemptTiming,
    ) -> Result<()> {
        if pending.data.is_empty() {
            return Ok(());
        }

        let write_started = Instant::now();
        let data_to_write = std::mem::take(&mut pending.data);
        write_tx
            .send((pending.start_offset, data_to_write))
            .await
            .map_err(|_| anyhow::anyhow!("writer task died"))?;

        if let Ok(mut recycled) = recycle_rx.try_recv() {
            recycled.clear();
            pending.data = recycled;
        } else {
            pending.data = Vec::with_capacity(pending.target_bytes.max(WRITE_BUFFER_MIN_BYTES));
        }

        let write_ms = write_started.elapsed().as_millis() as u64;
        attempt_timing.write_ms = attempt_timing.write_ms.saturating_add(write_ms);
        if write_ms > 0 && write_ms <= 500 {
            let sample = write_ms as f64;
            let prev = self.shared_write_latency_ms.get();
            let updated = 0.2 * sample + 0.8 * prev;
            self.shared_write_latency_ms.set(updated);
            let max_x10 = self.metrics.max_ewma_write_latency_x10.get();
            let updated_x10 = (updated * 10.0).round() as u64;
            if updated_x10 > max_x10 {
                self.metrics.max_ewma_write_latency_x10.set(updated_x10);
            }
        }

        self.trim_pending_write(pending);
        Ok(())
    }

    pub(super) fn append_pending_write(&self, pending: &mut PendingWrite, offset: u64, data: &[u8]) {
        if pending.data.is_empty() {
            pending.start_offset = offset;
        }
        pending.data.extend_from_slice(data);
    }

    fn target_write_buffer_bytes(&self, recent_speed_bps: f64) -> usize {
        let speed_bps = recent_speed_bps.max(0.0) as u64;
        let speed_target = if speed_bps >= WRITE_BUFFER_MAX_SPEED_BPS {
            WRITE_BUFFER_MAX_BYTES
        } else if speed_bps >= WRITE_BUFFER_LARGE_SPEED_BPS {
            WRITE_BUFFER_LARGE_BYTES
        } else if speed_bps >= WRITE_BUFFER_MEDIUM_SPEED_BPS {
            WRITE_BUFFER_MEDIUM_BYTES
        } else {
            WRITE_BUFFER_MIN_BYTES
        };
        let latency_ratio = (self.shared_write_latency_ms.get() / 10.0).max(1.0);
        let latency_target = ((WRITE_BUFFER_LARGE_BYTES as f64) * latency_ratio).round() as usize;
        let cap = self.write_buffer_cap_bytes.get().max(WRITE_BUFFER_LARGE_BYTES);
        speed_target
            .max(latency_target)
            .clamp(WRITE_BUFFER_MIN_BYTES, cap)
    }

    fn record_write_buffer_target_metric(&self, target: usize) {
        let target_u64 = target as u64;
        if target_u64 > self.metrics.max_write_buffer_target_bytes.get() {
            self.metrics.max_write_buffer_target_bytes.set(target_u64);
        }
    }

    pub(super) fn update_pending_write_target(&self, pending: &mut PendingWrite, recent_speed_bps: f64) {
        let target = self.target_write_buffer_bytes(recent_speed_bps);
        self.record_write_buffer_target_metric(target);
        pending.target_bytes = target;

        if pending.data.capacity() < pending.target_bytes {
            pending
                .data
                .reserve(pending.target_bytes.saturating_sub(pending.data.capacity()));
        } else if pending.data.is_empty() {
            self.trim_pending_write(pending);
        }
    }

    fn trim_pending_write(&self, pending: &mut PendingWrite) {
        let target = pending.target_bytes.max(WRITE_BUFFER_MIN_BYTES);
        if pending.data.is_empty() && pending.data.capacity() > target.saturating_mul(2) {
            pending.data.shrink_to(target);
        }
    }

    pub(super) fn reset_pending_write_target(&self, pending: &mut PendingWrite) {
        pending.target_bytes = WRITE_BUFFER_MIN_BYTES;
        self.trim_pending_write(pending);
    }
}
