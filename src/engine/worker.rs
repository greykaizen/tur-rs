use super::*;

pub(super) struct ConnectionWorker {
    pub(super) connection_id: u32,
    pub(super) url: String,
    pub(super) origin: String,
    pub(super) file_path: PathBuf,
    pub(super) log_path: PathBuf,
    pub(super) coordinator_tx: mpsc::Sender<WorkRequest>,
    pub(super) global_downloaded: Rc<Cell<u64>>,
    pub(super) control: Rc<RuntimeControl>,
    pub(super) worker_control: Rc<WorkerControl>,
    pub(super) dry_run: bool,
    pub(super) borrow_limit_bytes: u64,
    pub(super) adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
    pub(super) write_buffer_cap_bytes: Rc<Cell<usize>>,
    pub(super) total_size: u64,
    pub(super) origin_memory: Rc<RefCell<OriginMemoryStore>>,
    pub(super) shared_write_latency_ms: Rc<Cell<f64>>,
    pub(super) ewma_connection_rtt_ms: Cell<f64>,
    pub(super) metrics: Rc<SchedulerMetrics>,
    pub(super) client: DownloadHttpClient,
    pub(super) index_state: Rc<IndexStateMap>,
    pub(super) bucket: Rc<TokenBucket>,
    pub(super) scaler: Rc<Scaler>,
    pub(super) storage_config: storage::StorageConfig,
    pub(super) h3_client: Option<Rc<RefCell<crate::quic::H3Client>>>,
}

#[derive(Debug, Default)]
struct AttemptTiming {
    request_setup_ms: u64,
    first_byte_ms: u64,
    stream_ms: u64,
    write_ms: u64,
    bytes_written: u64,
    chunks: u64,
    request_kind: Option<RequestKind>,
    protocol: Option<ProtocolFamily>,
    handshake_cost_ms: u64,
}

#[derive(Debug, Default)]
struct PendingWrite {
    start_offset: u64,
    data: Vec<u8>,
    target_bytes: usize,
}

#[derive(Debug)]
struct StartupProbe {
    worker_started_at: Instant,
    open_file_ms: u64,
    first_assignment_wait_ms: Option<u64>,
    first_request_setup_ms: Option<u64>,
    first_byte_ms: Option<u64>,
    total_to_first_byte_ms: Option<u64>,
    logged: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    Reused,
    Fresh,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RetryHint {
    Immediate,
    Backoff(u64),
    ReduceWorkers(u64),
    ShrinkRange(u64),
    Abort,
}

impl ConnectionWorker {
    fn effective_prefetch_limit_bytes(&self) -> u64 {
        let base_limit = if is_tail_phase_bytes(self.global_downloaded.get(), self.total_size) {
            self.borrow_limit_bytes.min(MB).max(MB)
        } else {
            self.borrow_limit_bytes
        };

        if is_tail_phase_bytes(self.global_downloaded.get(), self.total_size) {
            base_limit
        } else {
            base_limit.max(self.adaptive_minimum_steal_bytes.get())
        }
    }

    fn should_exit_for_scale_down(&self) -> bool {
        self.worker_control.stop_requested.get()
    }

    async fn relinquish_range(&self, range: &Rc<ActiveRange>, local_cursor: u64) {
        range.cursor.set(local_cursor);
        range.assigned_to.set(UNASSIGNED_CONNECTION);
        range.status.set(RANGE_STATUS_PENDING);
        self.log_msg(&format!(
            "range#{} relinquished at byte={} for scale-down",
            range.id,
            local_cursor
        ))
        .await;
    }

    fn note_first_assignment(&self, startup: &mut StartupProbe, wait_started: Instant) {
        if startup.first_assignment_wait_ms.is_none() {
            let waited_ms = wait_started.elapsed().as_millis() as u64;
            startup.first_assignment_wait_ms = Some(waited_ms);
            SchedulerMetrics::add(&self.metrics.startup_first_assignment_wait_ms, waited_ms);
        }
    }

    fn note_first_request_setup(&self, startup: &mut StartupProbe, setup_ms: u64) {
        if startup.first_request_setup_ms.is_none() {
            startup.first_request_setup_ms = Some(setup_ms);
            SchedulerMetrics::add(&self.metrics.startup_first_request_setup_ms, setup_ms);
        }
    }

    async fn note_first_byte(
        &self,
        startup: &mut StartupProbe,
        file_backend: storage::StorageBackendKind,
    ) {
        if startup.logged {
            return;
        }

        let first_byte_ms = startup.first_byte_ms.unwrap_or_default();
        let total_to_first_byte_ms = startup.total_to_first_byte_ms.unwrap_or_default();
        self.log_msg(&format!(
            "startup backend={:?} open_file_ms={} first_assignment_wait_ms={} first_request_setup_ms={} first_byte_ms={} total_to_first_byte_ms={}",
            file_backend,
            startup.open_file_ms,
            startup.first_assignment_wait_ms.unwrap_or_default(),
            startup.first_request_setup_ms.unwrap_or_default(),
            first_byte_ms,
            total_to_first_byte_ms,
        ))
        .await;
        startup.logged = true;
    }

    async fn record_request_classification(
        &self,
        attempt_timing: &mut AttemptTiming,
        total_ttfb_ms: u64,
        protocol: ProtocolFamily,
    ) {
        let was_growth_probe = self.worker_control.pending_growth_probe.replace(false);
        let ttfb_ms = total_ttfb_ms as f64;
        let prev_conn_rtt = self.ewma_connection_rtt_ms.get();
        let updated_conn_rtt = if prev_conn_rtt <= 0.0 {
            ttfb_ms
        } else {
            0.25 * ttfb_ms + 0.75 * prev_conn_rtt
        };
        self.ewma_connection_rtt_ms.set(updated_conn_rtt);
        let ewma_rtt_ms = self.scaler.ewma_rtt_ms.get();
        let threshold_ms = ewma_rtt_ms * 1.5;
        let kind = if ttfb_ms < threshold_ms {
            RequestKind::Reused
        } else {
            RequestKind::Fresh
        };
        attempt_timing.request_kind = Some(kind);
        attempt_timing.protocol = Some(protocol);
        self.scaler.last_protocol.set(protocol);
        self.scaler
            .total_request_count
            .set(self.scaler.total_request_count.get().saturating_add(1));

        match kind {
            RequestKind::Reused => {
                self.scaler
                    .reused_count
                    .set(self.scaler.reused_count.get().saturating_add(1));
                let prev = self.scaler.ewma_rtt_ms.get();
                let updated = if self.scaler.reused_rtt_samples.get() == 0 {
                    ttfb_ms
                } else {
                    0.25 * ttfb_ms + 0.75 * prev
                };
                self.scaler.ewma_rtt_ms.set(updated);
                self.scaler
                    .reused_rtt_samples
                    .set(self.scaler.reused_rtt_samples.get().saturating_add(1));
                SchedulerMetrics::add(&self.metrics.reused_requests, 1);
                match protocol {
                    ProtocolFamily::Http1 => {
                        SchedulerMetrics::add(&self.metrics.http1_reused_requests, 1);
                    }
                    ProtocolFamily::Http2 => {
                        SchedulerMetrics::add(&self.metrics.http2_reused_requests, 1);
                    }
                    ProtocolFamily::Http3 => {}
                    ProtocolFamily::Other => {}
                }
            }
            RequestKind::Fresh => {
                let handshake_cost_ms = (ttfb_ms - ewma_rtt_ms.max(1.0)).max(0.0);
                attempt_timing.handshake_cost_ms = handshake_cost_ms.round() as u64;
                let prev = self.scaler.ewma_handshake_ms.get();
                let updated = 0.25 * handshake_cost_ms + 0.75 * prev;
                self.scaler.ewma_handshake_ms.set(updated);
                SchedulerMetrics::add(&self.metrics.fresh_requests, 1);
                SchedulerMetrics::add(
                    &self.metrics.fresh_handshake_ms,
                    attempt_timing.handshake_cost_ms,
                );
                match protocol {
                    ProtocolFamily::Http1 => {
                        SchedulerMetrics::add(&self.metrics.http1_fresh_requests, 1);
                        SchedulerMetrics::add(
                            &self.metrics.http1_fresh_handshake_ms,
                            attempt_timing.handshake_cost_ms,
                        );
                    }
                    ProtocolFamily::Http2 => {
                        SchedulerMetrics::add(&self.metrics.http2_fresh_requests, 1);
                        SchedulerMetrics::add(
                            &self.metrics.http2_fresh_handshake_ms,
                            attempt_timing.handshake_cost_ms,
                        );
                    }
                    ProtocolFamily::Http3 => {}
                    ProtocolFamily::Other => {}
                }

                if was_growth_probe
                    && self.scaler.last_action.get() == ScalerAction::Grow
                    && self.scaler.reused_rtt_samples.get()
                        >= protocol_growth_shield_min_samples(protocol)
                {
                    self.scaler.skip_growth_sample.set(true);
                    let heartbeat_ms = self.scaler.config.borrow().heartbeat_ms.max(1);
                    let adjusted_handshake_ms = ((attempt_timing.handshake_cost_ms as f64)
                        * protocol_growth_shield_multiplier(protocol))
                        .round() as u64;
                    let extra_heartbeats =
                        ((adjusted_handshake_ms + heartbeat_ms - 1) / heartbeat_ms) as u32;
                    self.scaler.slow_start_remaining.set(
                        self.scaler
                            .slow_start_remaining
                            .get()
                            .saturating_add(extra_heartbeats.max(1)),
                    );
                    self.log_msg(&format!(
                        "growth_probe_shield protocol={} handshake_ms={} extra_heartbeats={} reused_rtt_samples={}",
                        protocol.as_str(),
                        attempt_timing.handshake_cost_ms,
                        extra_heartbeats.max(1),
                        self.scaler.reused_rtt_samples.get(),
                    ))
                    .await;
                }
            }
        }

        let total_requests = self.scaler.total_request_count.get();
        if total_requests > 0 {
            self.scaler.reuse_rate.set(
                self.scaler.reused_count.get() as f64 / total_requests as f64,
            );
        }

        self.log_msg(&format!(
            "request_classified protocol={} kind={:?} total_ttfb_ms={} ewma_rtt_ms={:.1} reuse_rate={:.2} effective_add_threshold={:.2}",
            protocol.as_str(),
            kind,
            total_ttfb_ms,
            self.scaler.ewma_rtt_ms.get(),
            self.scaler.reuse_rate.get(),
            self.scaler.effective_add_threshold.get(),
        ))
        .await;
    }

    /// Live range-download loop using HTTP/3 (QUIC).
    async fn run_live_h3(self) -> Result<()> {
        let mut file = if self.dry_run {
            None
        } else {
            let f = storage::open_download_file_for_write_with_config(&self.file_path, &self.storage_config).await?;
            Some(f)
        };

        let mut current_range: Option<Rc<ActiveRange>> = None;
        let mut local_cursor: u64 = 0;
        let mut max_end: u64 = 0;

        loop {
            if self.control.is_halted() {
                return Ok(());
            }

            if current_range.is_none() {
                current_range = self.request_work(false).await?;
                if let Some(ref range) = current_range {
                    local_cursor = range.cursor.get();
                    max_end = range.end.get();
                    self.log_msg(&format!("h3 range#{} bytes={}..{}", range.id, local_cursor, max_end)).await;
                } else {
                    return Ok(());
                }
            }

            let Some(ref range) = current_range else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };

            if local_cursor >= max_end {
                range.status.set(RANGE_STATUS_FINISHED);
                let _ = self.coordinator_tx.send(WorkRequest {
                    connection_id: self.connection_id,
                    tx: oneshot::channel().0,
                }).await;
                current_range = None;
                continue;
            }

            SchedulerMetrics::add(&self.metrics.http_requests, 1);
            let request_start = Instant::now();

            let range_val = format!("bytes={}-{}", local_cursor, max_end.saturating_sub(1));
            let writer_cursor = Rc::new(Cell::new(local_cursor));
            let body_len = Rc::new(Cell::new(0_u64));
            let first_byte_seen = Rc::new(Cell::new(false));
            let first_byte_ms = Rc::new(Cell::new(0_u64));
            let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(8);

            let mut writer_file = file.take();
            let writer_cursor_task = writer_cursor.clone();
            let body_len_task = body_len.clone();
            let worker_control = self.worker_control.clone();
            let global_downloaded = self.global_downloaded.clone();
            let writer_handle = tokio::task::spawn_local(async move {
                while let Some(chunk) = write_rx.recv().await {
                    let chunk_len = chunk.len() as u64;
                    if chunk_len == 0 {
                        continue;
                    }
                    if let Some(ref mut f) = writer_file {
                        f.write_all_at(writer_cursor_task.get(), &chunk).await?;
                    }
                    writer_cursor_task.set(writer_cursor_task.get().saturating_add(chunk_len));
                    body_len_task.set(body_len_task.get().saturating_add(chunk_len));
                    worker_control.transferred_bytes.set(
                        worker_control.transferred_bytes.get().saturating_add(chunk_len),
                    );
                    global_downloaded.set(global_downloaded.get().saturating_add(chunk_len));
                }
                Ok::<_, anyhow::Error>(writer_file)
            });

            let server_name = Url::parse(&self.url)
                .ok()
                .and_then(|u| u.host_str().map(|s| s.to_owned()))
                .unwrap_or_else(|| self.origin.clone());
            let mut attempt_timing = AttemptTiming::default();
            let h3_result = {
                let client = self.h3_client.as_ref().unwrap().borrow();
                let metrics = self.metrics.clone();
                let bucket = self.bucket.clone();
                let first_byte_seen_cb = first_byte_seen.clone();
                let first_byte_ms_cb = first_byte_ms.clone();
                let write_tx_cb = write_tx.clone();
                client
                    .get_streaming(
                        &self.origin,
                        &server_name,
                        &self.url,
                        Some(&range_val),
                        move |chunk| {
                            let metrics = metrics.clone();
                            let bucket = bucket.clone();
                            let first_byte_seen = first_byte_seen_cb.clone();
                            let first_byte_ms = first_byte_ms_cb.clone();
                            let write_tx = write_tx_cb.clone();
                            Box::pin(async move {
                            let chunk_len = chunk.len();
                            if chunk_len == 0 {
                                return Ok(());
                            }

                            if !first_byte_seen.get() {
                                let observed_ttfb_ms = request_start.elapsed().as_millis() as u64;
                                first_byte_ms.set(observed_ttfb_ms);
                                SchedulerMetrics::add(
                                    &metrics.http_ttfb_ms,
                                    observed_ttfb_ms,
                                );
                                first_byte_seen.set(true);
                            }

                            while !bucket.consume(chunk_len) {
                                tokio::task::yield_now().await;
                            }

                            write_tx.send(chunk).await.map_err(|_| anyhow!("h3 writer stopped"))?;
                            Ok(())
                            })
                        },
                    )
                    .await
            };
            drop(write_tx);

            match h3_result {
                Ok(status) => {
                    file = writer_handle.await??;
                    local_cursor = writer_cursor.get();
                    attempt_timing.first_byte_ms = first_byte_ms.get();
                    attempt_timing.request_setup_ms = if first_byte_seen.get() {
                        attempt_timing.first_byte_ms
                    } else {
                        request_start.elapsed().as_millis() as u64
                    };
                    SchedulerMetrics::add(&self.metrics.http_setup_ms, attempt_timing.request_setup_ms);
                    record_protocol_request_metric(&self.metrics, ProtocolFamily::Http3);

                    if status != 206 && status != 200 {
                        if status == 429 {
                            self.origin_memory.borrow_mut().note_rate_limit(&self.origin);
                        }
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    }

                    attempt_timing.stream_ms = request_start.elapsed().as_millis() as u64;
                    attempt_timing.bytes_written = body_len.get();
                    if attempt_timing.stream_ms >= attempt_timing.first_byte_ms {
                        attempt_timing.stream_ms = attempt_timing
                            .stream_ms
                            .saturating_sub(attempt_timing.first_byte_ms);
                    }
                    if body_len.get() > 0 {
                        SchedulerMetrics::add(&self.metrics.http_stream_ms, attempt_timing.stream_ms);
                    }

                    let total_ttfb_ms =
                        attempt_timing.first_byte_ms.max(attempt_timing.request_setup_ms);
                    self.record_request_classification(
                        &mut attempt_timing,
                        total_ttfb_ms,
                        ProtocolFamily::Http3,
                    )
                    .await;
                }
                Err(e) => {
                    writer_handle.abort();
                    file = None;
                    self.log_msg(&format!("h3 error: {}", e)).await;
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }
    }
    async fn flush_pending_write(
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
        write_tx.send((pending.start_offset, data_to_write)).await
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

    fn append_pending_write(&self, pending: &mut PendingWrite, offset: u64, data: &[u8]) {
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

    fn update_pending_write_target(&self, pending: &mut PendingWrite, recent_speed_bps: f64) {
        let target = self.target_write_buffer_bytes(recent_speed_bps);
        self.record_write_buffer_target_metric(target);
        if pending.target_bytes == 0 {
            pending.target_bytes = target;
        } else {
            pending.target_bytes = target;
        }

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

    fn reset_pending_write_target(&self, pending: &mut PendingWrite) {
        pending.target_bytes = WRITE_BUFFER_MIN_BYTES;
        self.trim_pending_write(pending);
    }

    fn update_range_speed_sample(&self, range: &Rc<ActiveRange>, current_cursor: u64) {
        let now_ms = unix_time_ms();
        let last_at = range.last_sample_at_ms.get();
        let last_cursor = range.last_sample_cursor.get();
        if last_at == 0 {
            range.last_sample_at_ms.set(now_ms);
            range.last_sample_cursor.set(current_cursor);
            return;
        }

        let elapsed_ms = now_ms.saturating_sub(last_at);
        let advanced = current_cursor.saturating_sub(last_cursor);
        if elapsed_ms < 250 || advanced == 0 {
            return;
        }

        let speed_bps = ((advanced as u128) * 1000 / (elapsed_ms as u128)) as u64;
        range.recent_speed_bps.set(speed_bps);
        range.last_sample_at_ms.set(now_ms);
        range.last_sample_cursor.set(current_cursor);
    }

    async fn log_msg(&self, msg: &str) {
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .await
        {
            let _ = f
                .write_all(format!("[{}] conn={}: {}\n", chrono::Local::now(), self.connection_id, msg).as_bytes())
                .await;
        }
    }

    pub(super) async fn run(self) -> Result<()> {
        if self.h3_client.is_some() {
            self.run_live_h3().await
        } else if self.dry_run {
            self.run_dry().await
        } else {
            self.run_live().await
        }
    }

    async fn run_dry(self) -> Result<()> {
        let mut current_range: Option<Rc<ActiveRange>> = None;
        let mut prefetched_range: Option<Rc<ActiveRange>> = None;
        let mut prefetch_handle: Option<JoinHandle<Result<Option<Rc<ActiveRange>>>>> = None;
        let mut no_more_work_hint = false;
        let mut range_started_at = Instant::now();
        let mut range_start_cursor = 0_u64;
        let mut local_cursor = 0_u64;

        loop {
            if self.control.is_halted() || self.should_exit_for_scale_down() {
                break;
            }

            if self
                .collect_prefetch_result(&mut prefetch_handle, &mut prefetched_range, false)
                .await?
            {
                no_more_work_hint = true;
            }

            if current_range.is_none() {
                if let Some(range) = prefetched_range.take() {
                    SchedulerMetrics::add(&self.metrics.prefetch_hits, 1);
                    current_range = Some(range.clone());
                    range_started_at = Instant::now();
                    local_cursor = range.cursor.get();
                    range_start_cursor = local_cursor;
                } else {
                    current_range = self.request_work(false).await?;
                    if let Some(range) = &current_range {
                        range_started_at = Instant::now();
                        local_cursor = range.cursor.get();
                        range_start_cursor = local_cursor;
                    } else if no_more_work_hint {
                        break;
                    } else {
                        break;
                    }
                }
            }

            let range = current_range.as_ref().unwrap().clone();
            let end = range.end.get();
            if local_cursor >= end {
                range.status.set(RANGE_STATUS_FINISHED);
                current_range = None;
                continue;
            }

            let step = DRY_RUN_STEP_BYTES.min(end - local_cursor);
            let new_pos = local_cursor + step;
            range.cursor.set(new_pos);
            self.update_range_speed_sample(&range, new_pos);
            self.global_downloaded
                .set(self.global_downloaded.get().saturating_add(step));
            self.index_state.mark_completed_span(local_cursor, new_pos);
            local_cursor = new_pos;
            self.worker_control
                .transferred_bytes
                .set(self.worker_control.transferred_bytes.get().saturating_add(step));
            let recent_speed_bps = estimate_speed_bps(range_started_at, range_start_cursor, new_pos);

            let remaining = end.saturating_sub(new_pos);
            let prefetch_trigger_bytes = compute_prefetch_trigger_bytes(
                remaining,
                recent_speed_bps,
                self.effective_prefetch_limit_bytes(),
                self.scaler.last_protocol.get(),
                self.ewma_connection_rtt_ms.get(),
            );
            SchedulerMetrics::update_max(&self.metrics.max_prefetch_trigger_bytes, prefetch_trigger_bytes);
            if should_prefetch(
                remaining,
                recent_speed_bps,
                self.effective_prefetch_limit_bytes(),
                self.scaler.last_protocol.get(),
                self.ewma_connection_rtt_ms.get(),
            )
                && prefetch_handle.is_none()
                && prefetched_range.is_none()
                && !no_more_work_hint
            {
                SchedulerMetrics::add(&self.metrics.prefetch_requests, 1);
                prefetch_handle = Some(self.spawn_prefetch_request());
            }

            tokio::time::sleep(Duration::from_millis(DRY_RUN_STEP_DELAY_MS)).await;
        }

        if let Some(handle) = prefetch_handle {
            handle.abort();
        }
        Ok(())
    }

    async fn run_live(self) -> Result<()> {
        let worker_started_at = Instant::now();
        let file_open_started = Instant::now();
        let mut file = storage::open_download_file_for_write_with_config(&self.file_path, &self.storage_config).await?;
        let file_backend = file.backend();
        let open_file_ms = file_open_started.elapsed().as_millis() as u64;

        SchedulerMetrics::add(&self.metrics.startup_open_file_ms, open_file_ms);

        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<(u64, Vec<u8>)>(2);
        let (recycle_tx, mut recycle_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let metrics_clone = std::rc::Rc::clone(&self.metrics);
        let writer_handle = tokio::task::spawn_local(async move {
            while let Some((offset, mut data)) = write_rx.recv().await {
                let write_started = Instant::now();
                if let Err(e) = file.write_all_at(offset, &data).await {
                    return Err(e);
                }
                let write_ms = write_started.elapsed().as_millis() as u64;
                SchedulerMetrics::add(&metrics_clone.file_write_ms, write_ms);
                
                data.clear();
                let _ = recycle_tx.send(data);
            }
            Ok::<(), anyhow::Error>(())
        });
        let mut startup = StartupProbe {
            worker_started_at,
            open_file_ms,
            first_assignment_wait_ms: None,
            first_request_setup_ms: None,
            first_byte_ms: None,
            total_to_first_byte_ms: None,
            logged: false,
        };
        let mut current_range: Option<Rc<ActiveRange>> = None;
        let mut prefetched_range: Option<Rc<ActiveRange>> = None;
        let mut prefetch_handle: Option<JoinHandle<Result<Option<Rc<ActiveRange>>>>> = None;
        let mut no_more_work_hint = false;
        let mut range_started_at = Instant::now();
        let mut range_start_cursor = 0_u64;
        let mut local_cursor = 0_u64;
        let mut current_range_id: Option<u64> = None;
        let mut consecutive_failures = 0_u32;
        let mut range_wait_started = Instant::now();
        let mut pending_write = PendingWrite {
            start_offset: 0,
            data: Vec::with_capacity(WRITE_BUFFER_MIN_BYTES),
            target_bytes: WRITE_BUFFER_MIN_BYTES,
        };

        loop {
            if self.control.is_halted() {
                break;
            }

            if self
                .collect_prefetch_result(&mut prefetch_handle, &mut prefetched_range, false)
                .await?
            {
                no_more_work_hint = true;
            }

            if current_range.is_none() {
                if let Some(range) = prefetched_range.take() {
                    SchedulerMetrics::add(&self.metrics.prefetch_hits, 1);
                    current_range = Some(range.clone());
                    self.note_first_assignment(&mut startup, range_wait_started);
                    range_started_at = Instant::now();
                    local_cursor = range.cursor.get();
                    range_start_cursor = local_cursor;
                    current_range_id = Some(range.id);
                    consecutive_failures = 0;
                    self.log_msg(&format!(
                        "range#{} assigned via prefetch wait_ms={} bytes={}..{} support={}..{}MB",
                        range.id,
                        range_wait_started.elapsed().as_millis(),
                        range.byte_start,
                        range.end.get(),
                        range.label_start_mb,
                        range.label_end_mb
                    ))
                    .await;
                } else {
                    current_range = self.request_work(false).await?;
                    if let Some(range) = &current_range {
                        self.note_first_assignment(&mut startup, range_wait_started);
                        range_started_at = Instant::now();
                        local_cursor = range.cursor.get();
                        range_start_cursor = local_cursor;
                        current_range_id = Some(range.id);
                        consecutive_failures = 0;
                        self.log_msg(&format!(
                            "range#{} assigned wait_ms={} bytes={}..{} support={}..{}MB",
                            range.id,
                            range_wait_started.elapsed().as_millis(),
                            range.byte_start,
                            range.end.get(),
                            range.label_start_mb,
                            range.label_end_mb
                        ))
                        .await;
                    } else if no_more_work_hint {
                        break;
                    } else {
                        break;
                    }
                }
            }

            let range = current_range.as_ref().unwrap().clone();
            if current_range_id != Some(range.id) {
                current_range_id = Some(range.id);
                consecutive_failures = 0;
            }
            let start = local_cursor;
            let end = range.end.get();

            if start >= end {
                range.status.set(RANGE_STATUS_FINISHED);
                current_range = None;
                current_range_id = None;
                consecutive_failures = 0;
                continue;
            }

            SchedulerMetrics::add(&self.metrics.http_requests, 1);
            let request_started = Instant::now();
            let mut made_progress_this_attempt = false;
            let mut attempt_timing = AttemptTiming::default();
            let response = match send_request_follow_redirects(
                &self.client,
                Method::GET,
                &self.url,
                Some((start, end - 1)),
            )
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    let reason = format!("request error: {}", e);
                    let retry_hint = classify_error_retry(&reason, false);
                    consecutive_failures = self
                        .handle_range_retry(
                            &range,
                            start,
                            end,
                            consecutive_failures,
                            &reason,
                            retry_hint,
                        )
                        .await?;
                    continue;
                }
            };
            attempt_timing.request_setup_ms = request_started.elapsed().as_millis() as u64;
            SchedulerMetrics::add(&self.metrics.http_setup_ms, attempt_timing.request_setup_ms);
            self.note_first_request_setup(&mut startup, attempt_timing.request_setup_ms);

            let protocol_family = protocol_family_for_version(response.version());
            let http_version = http_version_label(response.version());
            record_protocol_request_metric(&self.metrics, protocol_family);

            if !response.status().is_success() {
                if response.status().as_u16() == 429 {
                    self.origin_memory
                        .borrow_mut()
                        .note_rate_limit(&self.origin);
                    self.log_msg("origin signalled rate limit (429)").await;
                }
                consecutive_failures = self
                    .handle_range_retry(
                        &range,
                        start,
                        end,
                        consecutive_failures,
                        &format!("HTTP error: {}", response.status()),
                        classify_http_status_retry(response.status()),
                    )
                    .await?;
                continue;
            }

            let supports_ranges = response.status() == ::http::StatusCode::PARTIAL_CONTENT
                || response.headers().contains_key(CONTENT_RANGE)
                || response
                    .headers()
                    .get(ACCEPT_RANGES)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value.eq_ignore_ascii_case("bytes"))
                    .unwrap_or(false);
            self.origin_memory
                .borrow_mut()
                .note_range_support(&self.origin, supports_ranges);

            // Phase 3c: Inspect Alt-Svc header for H3 upgrade hint
            if let Some(h3_port) = crate::quic::parse_alt_svc_h3_port(response.headers()) {
                self.origin_memory
                    .borrow_mut()
                    .note_protocol(&self.origin, ProtocolFamily::Http3);
                self.log_msg(&format!(
                    "alt_svc_h3_cached origin={} port={}",
                    self.origin, h3_port,
                )).await;
            }

            let mut stream = response.into_body();
            let mut stream_failed = None::<String>;
            let stream_started = Instant::now();
            let mut first_chunk_at: Option<Instant> = None;
            while let Some(frame_result) = stream.frame().await {
                if self.control.is_halted() {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    return Ok(());
                }

                let frame = match frame_result {
                    Ok(frame) => frame,
                    Err(e) => {
                        stream_failed = Some(format!("stream error: {}", e));
                        break;
                    }
                };

                let chunk = match frame.into_data() {
                    Ok(chunk) => chunk,
                    Err(_) => continue,
                };

                while !self.bucket.consume(chunk.len()) {
                    tokio::task::yield_now().await;
                }

                if first_chunk_at.is_none() {
                    first_chunk_at = Some(Instant::now());
                    attempt_timing.first_byte_ms = stream_started.elapsed().as_millis() as u64;
                    SchedulerMetrics::add(&self.metrics.http_ttfb_ms, attempt_timing.first_byte_ms);
                    let total_ttfb_ms = attempt_timing
                        .request_setup_ms
                        .saturating_add(attempt_timing.first_byte_ms);
                    self
                        .record_request_classification(
                            &mut attempt_timing,
                            total_ttfb_ms,
                            protocol_family,
                        )
                        .await;
                    if startup.first_byte_ms.is_none() {
                        startup.first_byte_ms = Some(attempt_timing.first_byte_ms);
                        startup.total_to_first_byte_ms =
                            Some(startup.worker_started_at.elapsed().as_millis() as u64);
                        SchedulerMetrics::add(
                            &self.metrics.startup_first_byte_ms,
                            attempt_timing.first_byte_ms,
                        );
                        SchedulerMetrics::add(
                            &self.metrics.startup_total_to_first_byte_ms,
                            startup.total_to_first_byte_ms.unwrap_or_default(),
                        );
                        self.note_first_byte(&mut startup, file_backend).await;
                    }
                }

                let max_end = range.end.get();
                if local_cursor >= max_end {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    current_range = None;
                    break;
                }

                let to_write = (max_end - local_cursor).min(chunk.len() as u64) as usize;
                self.append_pending_write(&mut pending_write, local_cursor, &chunk[..to_write]);

                let new_pos = local_cursor + to_write as u64;
                range.cursor.set(new_pos);
                self.update_range_speed_sample(&range, new_pos);
                self.global_downloaded.set(
                    self.global_downloaded
                        .get()
                        .saturating_add(to_write as u64),
                );
                self.index_state.mark_completed_span(local_cursor, new_pos);
                made_progress_this_attempt = true;
                attempt_timing.bytes_written = attempt_timing.bytes_written.saturating_add(to_write as u64);
                attempt_timing.chunks = attempt_timing.chunks.saturating_add(1);
                self.worker_control.transferred_bytes.set(
                    self.worker_control
                        .transferred_bytes
                        .get()
                        .saturating_add(to_write as u64),
                );
                local_cursor = new_pos;
                let recent_speed_bps = estimate_speed_bps(range_started_at, range_start_cursor, new_pos);
                self.update_pending_write_target(&mut pending_write, recent_speed_bps);
                if pending_write.data.len() >= pending_write.target_bytes {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                }

                let remaining = max_end.saturating_sub(new_pos);
                let prefetch_trigger_bytes = compute_prefetch_trigger_bytes(
                    remaining,
                    recent_speed_bps,
                    self.effective_prefetch_limit_bytes(),
                    self.scaler.last_protocol.get(),
                    self.ewma_connection_rtt_ms.get(),
                );
                SchedulerMetrics::update_max(&self.metrics.max_prefetch_trigger_bytes, prefetch_trigger_bytes);
                if should_prefetch(
                    remaining,
                    recent_speed_bps,
                    self.effective_prefetch_limit_bytes(),
                    self.scaler.last_protocol.get(),
                    self.ewma_connection_rtt_ms.get(),
                )
                    && prefetch_handle.is_none()
                    && prefetched_range.is_none()
                    && !no_more_work_hint
                {
                    SchedulerMetrics::add(&self.metrics.prefetch_requests, 1);
                    prefetch_handle = Some(self.spawn_prefetch_request());
                    self.log_msg(&format!(
                        "prefetch trigger remaining={} trigger_bytes={} recent_speed_bps={:.0}",
                        remaining, prefetch_trigger_bytes, recent_speed_bps
                    ))
                    .await;
                }

                if self
                    .collect_prefetch_result(&mut prefetch_handle, &mut prefetched_range, true)
                    .await?
                {
                    no_more_work_hint = true;
                }

                if self.should_exit_for_scale_down() {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    self.relinquish_range(&range, local_cursor).await;
                    if let Some(handle) = prefetch_handle {
                        handle.abort();
                    }
                    drop(write_tx);
                    let _ = writer_handle.await;
                    return Ok(());
                }

                if to_write < chunk.len() {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    current_range = None;
                    current_range_id = None;
                    consecutive_failures = 0;
                    range_wait_started = Instant::now();
                    self.reset_pending_write_target(&mut pending_write);
                    break;
                }
            }

            attempt_timing.stream_ms = stream_started.elapsed().as_millis() as u64;
            SchedulerMetrics::add(&self.metrics.http_stream_ms, attempt_timing.stream_ms);

            if let Some(reason) = stream_failed {
                self
                    .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                    .await?;
                if made_progress_this_attempt {
                    consecutive_failures = 0;
                    self.log_attempt_summary(&range, start, end, &attempt_timing, "reopen", http_version)
                        .await;
                    self.log_msg(&format!(
                        "{}; reopening range#{} from byte={}",
                        reason,
                        range.id,
                        range.cursor.get()
                    ))
                    .await;
                    self.reset_pending_write_target(&mut pending_write);
                } else {
                    let retry_hint = classify_error_retry(&reason, made_progress_this_attempt);
                    consecutive_failures = self
                        .handle_range_retry(
                            &range,
                            range.cursor.get(),
                            end,
                            consecutive_failures,
                            &reason,
                            retry_hint,
                        )
                        .await?;
                }
                continue;
            }

            if made_progress_this_attempt {
                consecutive_failures = 0;
            }
            if local_cursor >= range.end.get() {
                self
                    .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                    .await?;
                range.status.set(RANGE_STATUS_FINISHED);
                SchedulerMetrics::add(&self.metrics.completed_ranges, 1);
                self.log_attempt_summary(&range, start, end, &attempt_timing, "complete", http_version)
                    .await;
                current_range = None;
                current_range_id = None;
                consecutive_failures = 0;
                range_wait_started = Instant::now();
                self.reset_pending_write_target(&mut pending_write);
            } else if made_progress_this_attempt {
                self
                    .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                    .await?;
                self.log_attempt_summary(&range, start, end, &attempt_timing, "partial", http_version)
                    .await;
                self.reset_pending_write_target(&mut pending_write);
            }
        }

        if !pending_write.data.is_empty() {
            let mut final_timing = AttemptTiming::default();
            self
                .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut final_timing)
                .await?;
        }
        if let Some(handle) = prefetch_handle {
            handle.abort();
        }
        drop(write_tx);
        if let Err(e) = writer_handle.await.unwrap_or(Ok(())) {
            self.log_msg(&format!("Writer task failed: {}", e)).await;
            return Err(e);
        }
        Ok(())
    }

    async fn handle_range_retry(
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
                // Signal the current worker to stop so the scaler releases budget
                self.worker_control.stop_requested.set(true);
                // Relinquish the current range so it can be reassigned to another worker
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

    async fn log_attempt_summary(
        &self,
        range: &Rc<ActiveRange>,
        requested_start: u64,
        requested_end: u64,
        attempt_timing: &AttemptTiming,
        outcome: &str,
        http_version: &str,
    ) {
        self.log_msg(&format!(
            "range#{} {} version={} kind={:?} requested={}..{} advanced_to={} setup_ms={} first_byte_ms={} stream_ms={} write_ms={} handshake_ms={} bytes={} chunks={}",
            range.id,
            outcome,
            http_version,
            attempt_timing.request_kind,
            requested_start,
            requested_end,
            range.cursor.get(),
            attempt_timing.request_setup_ms,
            attempt_timing.first_byte_ms,
            attempt_timing.stream_ms,
            attempt_timing.write_ms,
            attempt_timing.handshake_cost_ms,
            attempt_timing.bytes_written,
            attempt_timing.chunks,
        ))
        .await;
    }

    fn spawn_prefetch_request(&self) -> JoinHandle<Result<Option<Rc<ActiveRange>>>> {
        let coordinator_tx = self.coordinator_tx.clone();
        let connection_id = self.connection_id;
        let metrics = self.metrics.clone();
        tokio::task::spawn_local(async move { request_work_inner(coordinator_tx, connection_id, metrics).await })
    }

    async fn collect_prefetch_result(
        &self,
        handle: &mut Option<JoinHandle<Result<Option<Rc<ActiveRange>>>>>,
        prefetched: &mut Option<Rc<ActiveRange>>,
        wait_even_if_not_finished: bool,
    ) -> Result<bool> {
        let should_take = handle
            .as_ref()
            .map(|task| wait_even_if_not_finished || task.is_finished())
            .unwrap_or(false);

        if should_take {
            let result = handle.take().unwrap().await??;
            if result.is_some() {
                SchedulerMetrics::add(&self.metrics.prefetch_ready, 1);
            }
            let exhausted = result.is_none();
            *prefetched = result;
            return Ok(exhausted);
        }

        Ok(false)
    }

    async fn request_work(&self, prefetched: bool) -> Result<Option<Rc<ActiveRange>>> {
        if prefetched {
            SchedulerMetrics::add(&self.metrics.prefetch_requests, 1);
        }
        self.log_msg(&format!("requesting_work prefetched={}", prefetched))
            .await;
        request_work_inner(
            self.coordinator_tx.clone(),
            self.connection_id,
            self.metrics.clone(),
        )
        .await
    }
}

async fn request_work_inner(
    coordinator_tx: mpsc::Sender<WorkRequest>,
    connection_id: u32,
    metrics: Rc<SchedulerMetrics>,
) -> Result<Option<Rc<ActiveRange>>> {
    let waited = Instant::now();
    SchedulerMetrics::add(&metrics.work_requests, 1);
    let (tx, rx) = oneshot::channel();
    if coordinator_tx
        .send(WorkRequest { connection_id, tx })
        .await
        .is_err()
    {
        return Ok(None);
    }

    let result = match rx.await {
        Ok(range) => range,
        Err(_) => None,
    };
    SchedulerMetrics::add(&metrics.request_wait_ms, waited.elapsed().as_millis() as u64);
    Ok(result)
}

fn classify_http_status_retry(status: ::http::StatusCode) -> RetryHint {
    match status.as_u16() {
        429 => RetryHint::ReduceWorkers(RETRY_MAX_DELAY_MS),
        503 => RetryHint::Immediate,
        500..=599 => RetryHint::Backoff(RETRY_BASE_DELAY_MS.saturating_mul(2)),
        416 => RetryHint::Abort,
        _ => RetryHint::Backoff(RETRY_BASE_DELAY_MS),
    }
}

fn classify_error_retry(reason: &str, made_progress: bool) -> RetryHint {
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
