use super::*;

mod loop_impl;
mod stores;

use stores::OriginPhiRatioEntry;

#[derive(Debug)]
pub(super) struct RuntimeControl {
    pub(super) halt_mode: Cell<HaltMode>,
    pub(super) cancel_flag: Cell<bool>,
    pub(super) scaler_config: Rc<RefCell<ScalerConfig>>,
    /// Set by a worker when a challenge/interstitial page is detected.
    /// The download will be aborted with this reason.
    pub(super) challenge_reason: RefCell<Option<String>>,
}

impl RuntimeControl {
    pub(super) fn new(scaler_config: ScalerConfig) -> Self {
        Self {
            halt_mode: Cell::new(HaltMode::Running),
            cancel_flag: Cell::new(false),
            scaler_config: Rc::new(RefCell::new(scaler_config)),
            challenge_reason: RefCell::new(None),
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

pub(super) enum PendingLaunch {
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
    pub diagnostics: Rc<WorkerDiagnosticsState>,
}

impl WorkerControl {
    pub(super) fn new(connection_id: u32) -> Rc<Self> {
        let diagnostics = Rc::new(WorkerDiagnosticsState::new(connection_id));
        Rc::new(Self {
            connection_id,
            stop_requested: Cell::new(false),
            transferred_bytes: Cell::new(0),
            pending_growth_probe: Cell::new(false),
            diagnostics,
        })
    }
}

pub(super) struct WorkerSlot {
    pub control: Rc<WorkerControl>,
    pub handle: JoinHandle<()>,
}

pub(super) struct WorkerDiagnosticsState {
    connection_id: u32,
    state: Cell<WorkerState>,
    speed_bps: Cell<f64>,
    range_start: Cell<u64>,
    range_end: Cell<u64>,
    range_cursor: Cell<u64>,
    has_range: Cell<bool>,
    detail: RefCell<Option<String>>,
}

impl WorkerDiagnosticsState {
    fn new(connection_id: u32) -> Self {
        Self {
            connection_id,
            state: Cell::new(WorkerState::Connecting),
            speed_bps: Cell::new(0.0),
            range_start: Cell::new(0),
            range_end: Cell::new(0),
            range_cursor: Cell::new(0),
            has_range: Cell::new(false),
            detail: RefCell::new(None),
        }
    }

    pub(super) fn set_state(&self, state: WorkerState) {
        self.state.set(state);
        if !matches!(state, WorkerState::Downloading) {
            self.speed_bps.set(0.0);
        }
    }

    pub(super) fn set_detail(&self, detail: Option<String>) {
        *self.detail.borrow_mut() = detail;
    }

    pub(super) fn set_range(&self, start: u64, end: u64, cursor: u64) {
        self.range_start.set(start);
        self.range_end.set(end);
        self.range_cursor.set(cursor);
        self.has_range.set(true);
    }

    pub(super) fn clear_range(&self) {
        self.has_range.set(false);
        self.range_start.set(0);
        self.range_end.set(0);
        self.range_cursor.set(0);
    }

    pub(super) fn set_speed_bps(&self, speed_bps: f64) {
        self.speed_bps.set(speed_bps.max(0.0));
    }

    pub(super) fn snapshot(&self, transferred_bytes: u64) -> WorkerSnapshot {
        WorkerSnapshot {
            connection_id: self.connection_id,
            state: self.state.get(),
            transferred_bytes,
            speed_bps: self.speed_bps.get(),
            range_start: self.has_range.get().then(|| self.range_start.get()),
            range_end: self.has_range.get().then(|| self.range_end.get()),
            range_cursor: self.has_range.get().then(|| self.range_cursor.get()),
            detail: self.detail.borrow().clone(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct OriginPhiRatioStore {
    entries: HashMap<String, OriginPhiRatioEntry>,
    pub(super) usage_tick: u64,
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

}
