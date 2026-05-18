use super::*;

impl ConnectionWorker {
    pub(super) async fn run_dry(self) -> Result<()> {
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
            SchedulerMetrics::update_max(
                &self.metrics.max_prefetch_trigger_bytes,
                prefetch_trigger_bytes,
            );
            if should_prefetch(
                remaining,
                recent_speed_bps,
                self.effective_prefetch_limit_bytes(),
                self.scaler.last_protocol.get(),
                self.ewma_connection_rtt_ms.get(),
            ) && prefetch_handle.is_none()
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

    pub(super) fn spawn_prefetch_request(
        &self,
    ) -> JoinHandle<Result<Option<Rc<ActiveRange>>>> {
        let coordinator_tx = self.coordinator_tx.clone();
        let connection_id = self.connection_id;
        let metrics = self.metrics.clone();
        tokio::task::spawn_local(async move {
            request_work_inner(coordinator_tx, connection_id, metrics).await
        })
    }

    pub(super) async fn collect_prefetch_result(
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

    pub(super) async fn request_work(&self, prefetched: bool) -> Result<Option<Rc<ActiveRange>>> {
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
