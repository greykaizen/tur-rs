use super::*;

impl DownloadEngine {
    pub async fn run(
        self: Rc<Self>,
        mut cmd_rx: mpsc::Receiver<EngineCommand>,
        cmd_tx: mpsc::Sender<EngineCommand>,
        event_tx: mpsc::Sender<EngineEvent>,
    ) -> Result<()> {
        let mut active_controls: HashMap<Uuid, Rc<RuntimeControl>> = HashMap::new();
        let mut paused_tasks: HashMap<Uuid, TaskSnapshot> = HashMap::new();
        let mut persisted_paths: HashMap<Uuid, PathBuf> = HashMap::new();
        let mut deferred_resumes = HashSet::<Uuid>::new();
        let mut pending_launches = VecDeque::<PendingLaunch>::new();
        let mut last_refill_recompute = Instant::now();
        let mut memory_system = System::new();

        loop {
            let refill_sleep_ms = self.refill_interval_ms.get().max(10);
            let refill_sleep = tokio::time::sleep(Duration::from_millis(refill_sleep_ms));
            tokio::pin!(refill_sleep);
            tokio::select! {
                _ = &mut refill_sleep => {
                    if self.last_memory_check.get().elapsed() >= Duration::from_secs(30) {
                        memory_system.refresh_memory();
                        let available_mb = memory_system.available_memory() / (1024 * 1024);
                        let configured_budget = self.configured_connection_budget.get();
                        let previous_effective = self.effective_connection_budget.get();
                        let new_effective =
                            compute_effective_connection_budget(configured_budget, available_mb);
                        let write_buffer_cap_bytes = if available_mb < 512 {
                            WRITE_BUFFER_LARGE_BYTES
                        } else {
                            4 * MB as usize
                        };
                        self.write_buffer_cap_bytes.set(write_buffer_cap_bytes);
                        if new_effective != previous_effective {
                            let current_available = self.connection_budget.get().min(previous_effective);
                            let active_leases = previous_effective.saturating_sub(current_available);
                            let new_available = new_effective.saturating_sub(active_leases.min(new_effective));
                            self.effective_connection_budget.set(new_effective);
                            self.connection_budget.set(new_available);
                            if new_effective < previous_effective {
                                eprintln!(
                                    "INFO: memory pressure reduced connection budget available_mb={} effective_budget={}/{}",
                                    available_mb,
                                    new_effective,
                                    configured_budget
                                );
                            } else {
                                eprintln!(
                                    "INFO: memory pressure restored connection budget available_mb={} effective_budget={}/{}",
                                    available_mb,
                                    new_effective,
                                    configured_budget
                                );
                            }
                        }
                        self.last_memory_check.set(Instant::now());
                    }

                    while active_controls.len() < self.max_concurrent_tasks {
                        let Some(next_launch) = pending_launches.pop_front() else { break; };
                        match next_launch {
                            PendingLaunch::Fresh(task) => {
                                let control = Rc::new(RuntimeControl::new(ScalerConfig {
                                    min_connections: task.min_connections,
                                    max_connections: task.max_connections,
                                    heartbeat_ms: 2000,
                                }));
                                active_controls.insert(task.id, control.clone());

                                let bucket = Rc::new(TokenBucket::new());
                                self.downloads.borrow_mut().push(DownloadHandle {
                                    id: task.id,
                                    bucket: bucket.clone(),
                                    per_download_limit_bps: task.per_download_bandwidth_limit_bps,
                                });

                                self.spawn_download_task(task, None, control, bucket, cmd_tx.clone(), event_tx.clone());
                            }
                            PendingLaunch::Resume(snapshot) => {
                                let task = snapshot.task.clone();
                                let heartbeat_ms = snapshot.resume_state.heartbeat_ms.max(500);
                                let control = Rc::new(RuntimeControl::new(ScalerConfig {
                                    min_connections: task.min_connections,
                                    max_connections: task.max_connections,
                                    heartbeat_ms,
                                }));
                                active_controls.insert(task.id, control.clone());

                                let bucket = Rc::new(TokenBucket::new());
                                self.downloads.borrow_mut().push(DownloadHandle {
                                    id: task.id,
                                    bucket: bucket.clone(),
                                    per_download_limit_bps: task.per_download_bandwidth_limit_bps,
                                });

                                self.spawn_download_task(task, Some(snapshot), control, bucket, cmd_tx.clone(), event_tx.clone());
                            }
                        }
                    }

                    let downloads = self.downloads.borrow();
                    let n_active = downloads.len();
                    if n_active > 0 {
                        let global_limit = self.global_bandwidth_limit.get();
                        if last_refill_recompute.elapsed() >= Duration::from_secs(5) {
                            self.refill_interval_ms
                                .set(compute_refill_interval_ms(global_limit));
                            last_refill_recompute = Instant::now();
                        }
                        let refill_interval_ms = self.refill_interval_ms.get();
                        let per_download = if global_limit == 0 {
                            0
                        } else {
                            global_limit / n_active as u64
                        };
                        for handle in downloads.iter() {
                            let quota = if per_download == 0 {
                                handle.per_download_limit_bps
                            } else if handle.per_download_limit_bps == 0 {
                                per_download
                            } else {
                                per_download.min(handle.per_download_limit_bps)
                            };
                            handle.bucket.quota_bytes_per_sec.set(quota);
                            handle.bucket.refill_interval_ms.set(refill_interval_ms);
                            handle.bucket.refill(refill_interval_ms);
                        }
                    } else if last_refill_recompute.elapsed() >= Duration::from_secs(5) {
                        self.refill_interval_ms
                            .set(compute_refill_interval_ms(self.global_bandwidth_limit.get()));
                        last_refill_recompute = Instant::now();
                    }
                }
                cmd_opt = cmd_rx.recv() => {
                    let Some(cmd) = cmd_opt else { break; };
                    match cmd {
                        EngineCommand::Add(task) => {
                            pending_launches.push_back(PendingLaunch::Fresh(task));
                        }
                        EngineCommand::Stop(id) => {
                            if let Some(control) = active_controls.get(&id) {
                                // Use drain for stop — workers finish current chunk gracefully.
                                // Hibernating mode lets the runtime keep the snapshot in memory.
                                control.request_drain();
                            }
                        }
                        EngineCommand::Cancel(id) => {
                            if let Some(control) = active_controls.get(&id) {
                                control.request_persist();
                            } else if let Some(snapshot) = paused_tasks.remove(&id) {
                                let path = metadata_path(&snapshot.task);
                                persist_snapshot(&path, &snapshot)?;
                                persisted_paths.insert(snapshot.task.id, path);
                                let _ = event_tx
                                    .send(EngineEvent::StatusChanged(
                                        snapshot.task.id,
                                        DownloadStatus::Stopped,
                                    ))
                                    .await;
                            }
                        }
                        EngineCommand::Resume(id) => {
                            if active_controls.contains_key(&id) {
                                deferred_resumes.insert(id);
                                continue;
                            }

                            let snapshot = if let Some(snapshot) = paused_tasks.remove(&id) {
                                snapshot
                            } else {
                                let path = persisted_paths
                                    .get(&id)
                                    .cloned()
                                    .ok_or_else(|| anyhow!("No paused or persisted task found for {}", id))?;
                                load_snapshot(&path)?
                            };
                            pending_launches.push_back(PendingLaunch::Resume(snapshot));
                        }
                        EngineCommand::UpdateScaling(id, config) => {
                            if config.min_connections > config.max_connections {
                                let _ = event_tx
                                    .send(EngineEvent::StatusChanged(
                                        id,
                                        DownloadStatus::Error(format!(
                                            "invalid scaling update: min_connections {} exceeds max_connections {}",
                                            config.min_connections, config.max_connections
                                        )),
                                    ))
                                    .await;
                                continue;
                            }

                            if let Some(control) = active_controls.get(&id) {
                                *control.scaler_config().borrow_mut() = config;
                            } else if let Some(snapshot) = paused_tasks.get_mut(&id) {
                                snapshot.task.min_connections = config.min_connections;
                                snapshot.task.max_connections = config.max_connections;
                            } else if let Some(path) = persisted_paths.get(&id).cloned() {
                                let mut snapshot = load_snapshot(&path)?;
                                snapshot.task.min_connections = config.min_connections;
                                snapshot.task.max_connections = config.max_connections;
                                persist_snapshot(&path, &snapshot)?;
                            }
                        }
                        EngineCommand::RuntimeStopped(snapshot, halt_mode) => {
                            active_controls.remove(&snapshot.task.id);
                            self.downloads.borrow_mut().retain(|h| h.id != snapshot.task.id);

                            match halt_mode {
                                HaltMode::Hibernating | HaltMode::Draining => {
                                    // Hibernating: pause in memory, ready for warm resume.
                                    // Draining: same treatment — workers finished gracefully.
                                    if deferred_resumes.remove(&snapshot.task.id) {
                                        pending_launches.push_back(PendingLaunch::Resume(snapshot));
                                    } else {
                                        paused_tasks.insert(snapshot.task.id, snapshot.clone());
                                        let label = if matches!(halt_mode, HaltMode::Hibernating) {
                                            "hibernated"
                                        } else {
                                            "drained"
                                        };
                                        coordinator_log_warm_resume(&snapshot, label).await;
                                        let _ = event_tx
                                            .send(EngineEvent::StatusChanged(
                                                snapshot.task.id,
                                                DownloadStatus::Paused,
                                            ))
                                            .await;
                                    }
                                }
                                HaltMode::PersistToDisk => {
                                    let path = metadata_path(&snapshot.task);
                                    persist_snapshot(&path, &snapshot)?;
                                    persisted_paths.insert(snapshot.task.id, path);
                                    coordinator_log_cold_resume(&snapshot).await;
                                    let _ = event_tx
                                        .send(EngineEvent::StatusChanged(
                                            snapshot.task.id,
                                            DownloadStatus::Stopped,
                                        ))
                                        .await;
                                }
                                HaltMode::Running => {}
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn spawn_download_task(
        self: &Rc<Self>,
        task: DownloadTask,
        snapshot: Option<TaskSnapshot>,
        control: Rc<RuntimeControl>,
        bucket: Rc<TokenBucket>,
        cmd_tx: mpsc::Sender<EngineCommand>,
        event_tx: mpsc::Sender<EngineEvent>,
    ) {
        let default_connections = self.connections_per_download;
        let engine = self.clone();
        tokio::task::spawn_local(async move {
            let task_id = task.id;
            let result = run_download_task(
                engine,
                task,
                snapshot,
                control,
                bucket,
                cmd_tx.clone(),
                event_tx.clone(),
                default_connections,
            ).await;
            if let Err(err) = result {
                let _ = event_tx.send(EngineEvent::StatusChanged(
                    task_id,
                    DownloadStatus::Error(err.to_string()),
                )).await;
            }
        });
    }
}

/// Log a warm-resume observation (task paused in memory, ready for fast resume).
async fn coordinator_log_warm_resume(snapshot: &TaskSnapshot, label: &str) {
    let path = crate::engine::persistence::log_path(&snapshot.task);
    if let Ok(mut f) = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        use tokio::io::AsyncWriteExt;
        let _ = f
            .write_all(
                format!(
                    "[{}] runtime_lifecycle event={} task={} downloaded_bytes={} progress_pct={:.1}% target_connections={} protocol_hint={:?} ewma_throughput_bps={:.0} reuse_rate={:.2}\n",
                    chrono::Local::now(),
                    label,
                    snapshot.task.id,
                    snapshot.task.downloaded_size,
                    if snapshot.task.total_size > 0 {
                        snapshot.task.downloaded_size as f64 / snapshot.task.total_size as f64 * 100.0
                    } else {
                        0.0
                    },
                    snapshot.resume_state.target_connections,
                    snapshot.resume_state.protocol_hint,
                    snapshot.resume_state.ewma_throughput_bps,
                    snapshot.resume_state.reuse_rate,
                )
                .as_bytes(),
            )
            .await;
    }
}

/// Log a cold-resume observation (task persisted to disk, needs reload).
async fn coordinator_log_cold_resume(snapshot: &TaskSnapshot) {
    let path = crate::engine::persistence::log_path(&snapshot.task);
    if let Ok(mut f) = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        use tokio::io::AsyncWriteExt;
        let _ = f
            .write_all(
                format!(
                    "[{}] runtime_lifecycle event=persisted task={} downloaded_bytes={} progress_pct={:.1}% target_connections={} protocol_hint={:?} ewma_throughput_bps={:.0} reuse_rate={:.2}\n",
                    chrono::Local::now(),
                    snapshot.task.id,
                    snapshot.task.downloaded_size,
                    if snapshot.task.total_size > 0 {
                        snapshot.task.downloaded_size as f64 / snapshot.task.total_size as f64 * 100.0
                    } else {
                        0.0
                    },
                    snapshot.resume_state.target_connections,
                    snapshot.resume_state.protocol_hint,
                    snapshot.resume_state.ewma_throughput_bps,
                    snapshot.resume_state.reuse_rate,
                )
                .as_bytes(),
            )
            .await;
    }
}
