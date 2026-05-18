use super::*;

#[derive(Debug)]
pub(super) struct RuntimeControl {
    pub(super) halt_mode: Cell<HaltMode>,
    pub(super) cancel_flag: Cell<bool>,
    pub(super) scaler_config: Rc<RefCell<ScalerConfig>>,
}

impl RuntimeControl {
    pub(super) fn new(scaler_config: ScalerConfig) -> Self {
        Self {
            halt_mode: Cell::new(HaltMode::Running),
            cancel_flag: Cell::new(false),
            scaler_config: Rc::new(RefCell::new(scaler_config)),
        }
    }

    pub(super) fn halt_mode(&self) -> HaltMode {
        self.halt_mode.get()
    }

    pub(super) fn request_pause(&self) {
        self.halt_mode.set(HaltMode::PauseMemory);
        self.cancel_flag.set(true);
    }

    pub(super) fn request_persist(&self) {
        self.halt_mode.set(HaltMode::PersistToDisk);
        self.cancel_flag.set(true);
    }

    pub(super) fn is_halted(&self) -> bool {
        self.halt_mode() != HaltMode::Running || self.cancel_flag.get()
    }

    pub(super) fn scaler_config(&self) -> Rc<RefCell<ScalerConfig>> {
        self.scaler_config.clone()
    }
}

enum PendingLaunch {
    Fresh(DownloadTask),
    Resume(TaskSnapshot),
}


pub(crate) struct DownloadHandle {
    pub id: Uuid,
    pub bucket: Rc<TokenBucket>,
    pub per_download_limit_bps: u64,
}

pub(super) struct WorkerControl {
    pub connection_id: u32,
    pub stop_requested: Cell<bool>,
    pub transferred_bytes: Cell<u64>,
    pub pending_growth_probe: Cell<bool>,
}

impl WorkerControl {
    pub(super) fn new(connection_id: u32) -> Rc<Self> {
        Rc::new(Self {
            connection_id,
            stop_requested: Cell::new(false),
            transferred_bytes: Cell::new(0),
            pending_growth_probe: Cell::new(false),
        })
    }
}

pub(super) struct WorkerSlot {
    pub control: Rc<WorkerControl>,
    pub handle: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OriginPhiRatioEntry {
    pub ratio: f64,
    pub last_used_tick: u64,
}

#[derive(Debug, Default)]
pub(super) struct OriginPhiRatioStore {
    pub entries: HashMap<String, OriginPhiRatioEntry>,
    pub usage_tick: u64,
}

impl OriginPhiRatioStore {
    fn next_tick(&mut self) -> u64 {
        self.usage_tick = self.usage_tick.saturating_add(1);
        self.usage_tick
    }

    pub(super) fn ratio_for_origin(&mut self, origin: &str) -> f64 {
        let tick = self.next_tick();
        if let Some(entry) = self.entries.get_mut(origin) {
            entry.last_used_tick = tick;
            entry.ratio
        } else {
            INITIAL_PHI_MAX_RATIO
        }
    }

    pub(super) fn update_origin_ratio(&mut self, origin: String, ratio: f64) {
        let tick = self.next_tick();
        self.entries.insert(
            origin,
            OriginPhiRatioEntry {
                ratio,
                last_used_tick: tick,
            },
        );
        self.prune_lru();
    }

    pub(super) fn current_ratio(&self, origin: &str) -> Option<f64> {
        self.entries.get(origin).map(|entry| entry.ratio)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > ORIGIN_PHI_RATIO_CAPACITY {
            let Some(lru_key) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used_tick)
                .map(|(origin, _)| origin.clone())
            else {
                break;
            };
            self.entries.remove(&lru_key);
        }
    }
}

pub struct DownloadEngine {
    pub connections_per_download: usize,
    pub max_concurrent_tasks: usize,
    pub configured_connection_budget: Cell<usize>,
    pub effective_connection_budget: Cell<usize>,
    pub connection_budget: Cell<usize>,
    pub global_bandwidth_limit: Cell<u64>,
    pub refill_interval_ms: Cell<u64>,
    pub last_memory_check: Cell<Instant>,
    pub(super) origin_phi_ratios: RefCell<OriginPhiRatioStore>,
    pub(super) origin_h2_tunings: RefCell<OriginH2TuningStore>,
    pub(super) origin_memory: Rc<RefCell<OriginMemoryStore>>,
    pub write_buffer_cap_bytes: Rc<Cell<usize>>,
    pub storage_config: StorageConfig,
    pub(crate) downloads: RefCell<Vec<DownloadHandle>>,
}

impl DownloadEngine {
    pub fn new(
        connections_per_download: usize,
        max_concurrent_tasks: usize,
        max_total_connections: usize,
        global_bandwidth_limit_bps: u64,
        enable_origin_memory: bool,
        storage_config: StorageConfig,
    ) -> Rc<Self> {
        let configured_budget = max_total_connections.max(1);
        let origin_memory = OriginMemoryStore::load_enabled(enable_origin_memory);
        let origin_phi_ratios = origin_memory.hydrate_phi_ratios();
        let origin_h2_tunings = origin_memory.hydrate_h2_tunings();
        Rc::new(Self {
            connections_per_download,
            max_concurrent_tasks,
            configured_connection_budget: Cell::new(configured_budget),
            effective_connection_budget: Cell::new(configured_budget),
            connection_budget: Cell::new(configured_budget),
            global_bandwidth_limit: Cell::new(global_bandwidth_limit_bps),
            refill_interval_ms: Cell::new(if global_bandwidth_limit_bps == 0 { 50 } else { 100 }),
            last_memory_check: Cell::new(Instant::now()),
            origin_phi_ratios: RefCell::new(origin_phi_ratios),
            origin_h2_tunings: RefCell::new(origin_h2_tunings),
            origin_memory: Rc::new(RefCell::new(origin_memory)),
            write_buffer_cap_bytes: Rc::new(Cell::new(4 * MB as usize)),
            storage_config,
            downloads: RefCell::new(Vec::new()),
        })
    }

    pub fn request_connection(&self) -> bool {
        let current = self.connection_budget.get();
        if current > 0 {
            self.connection_budget.set(current - 1);
            true
        } else {
            false
        }
    }

    pub fn release_connection(&self) {
        let next = self.connection_budget.get().saturating_add(1);
        self.connection_budget
            .set(next.min(self.effective_connection_budget.get()));
    }

    pub async fn run(
        self: Rc<Self>,
        mut cmd_rx: mpsc::Receiver<EngineCommand>,
        cmd_tx: mpsc::Sender<EngineCommand>,
        event_tx: mpsc::Sender<EngineEvent>,
    ) -> Result<()> {
        let mut active_controls: HashMap<Uuid, Rc<RuntimeControl>> = HashMap::new();
        let mut paused_tasks: HashMap<Uuid, TaskSnapshot> = HashMap::new();
        let mut persisted_paths: HashMap<Uuid, PathBuf> = HashMap::new();
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
                                control.request_pause();
                            }
                        }
                        EngineCommand::Cancel(id) => {
                            if let Some(control) = active_controls.get(&id) {
                                control.request_persist();
                            }
                        }
                        EngineCommand::Resume(id) => {
                            if active_controls.contains_key(&id) {
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
                                HaltMode::PauseMemory => {
                                    paused_tasks.insert(snapshot.task.id, snapshot.clone());
                                    let _ = event_tx
                                        .send(EngineEvent::StatusChanged(
                                            snapshot.task.id,
                                            DownloadStatus::Paused,
                                        ))
                                        .await;
                                }
                                HaltMode::PersistToDisk => {
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
