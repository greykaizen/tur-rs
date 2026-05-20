use super::*;

impl ConnectionWorker {
    /// Live range-download loop using HTTP/3 (QUIC).
    pub(super) async fn run_live_h3(self) -> Result<()> {
        let mut file = if self.dry_run {
            None
        } else {
            let f = storage::open_download_file_for_write_with_config(
                &self.file_path,
                &self.storage_config,
            )
            .await?;
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
                    self.log_msg(&format!(
                        "h3 range#{} bytes={}..{}",
                        range.id, local_cursor, max_end
                    ))
                    .await;
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
                let _ = self
                    .coordinator_tx
                    .send(WorkRequest {
                        connection_id: self.connection_id,
                        tx: oneshot::channel().0,
                    })
                    .await;
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
                        worker_control
                            .transferred_bytes
                            .get()
                            .saturating_add(chunk_len),
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
                                    let observed_ttfb_ms =
                                        request_start.elapsed().as_millis() as u64;
                                    first_byte_ms.set(observed_ttfb_ms);
                                    SchedulerMetrics::add(&metrics.http_ttfb_ms, observed_ttfb_ms);
                                    first_byte_seen.set(true);
                                }

                                while !bucket.consume(chunk_len) {
                                    tokio::task::yield_now().await;
                                }

                                write_tx
                                    .send(chunk)
                                    .await
                                    .map_err(|_| anyhow!("h3 writer stopped"))?;
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
                    SchedulerMetrics::add(
                        &self.metrics.http_setup_ms,
                        attempt_timing.request_setup_ms,
                    );
                    record_protocol_request_metric(&self.metrics, ProtocolFamily::Http3);

                    if status != 206 && status != 200 {
                        if status == 429 {
                            self.origin_memory
                                .borrow_mut()
                                .note_rate_limit(&self.origin);
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
                        SchedulerMetrics::add(
                            &self.metrics.http_stream_ms,
                            attempt_timing.stream_ms,
                        );
                    }

                    let total_ttfb_ms = attempt_timing
                        .first_byte_ms
                        .max(attempt_timing.request_setup_ms);
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
}
