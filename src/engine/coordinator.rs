use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RangeSpec {
    pub id: u64,
    pub label_start_mb: u64,
    pub label_end_mb: u64,
    pub byte_start: u64,
    pub byte_end: u64,
}

pub(super) const INITIAL_PHI_MAX_RATIO: f64 = 1.5;
pub(super) const STORAGE_BLOCK_SIZE: u64 = 4194304; // 4MB
pub(super) const RANGE_STATUS_PENDING: u8 = 0;
pub(super) const RANGE_STATUS_ACTIVE: u8 = 1;
pub(super) const RANGE_STATUS_FINISHED: u8 = 2;
pub(super) const UNASSIGNED_CONNECTION: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BorrowKind {
    Standard,
    Straggler,
    Tail,
}

impl BorrowKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Straggler => "straggler",
            Self::Tail => "tail",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlRangeSnapshot {
    pub(super) id: u64,
    pub(super) label_start_mb: u64,
    pub(super) label_end_mb: u64,
    pub(super) byte_start: u64,
    pub(super) assigned_to: u32,
    pub(super) cursor: u64,
    pub(super) end: u64,
    pub(super) parent_range_id: Option<u64>,
    pub(super) status: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorSnapshot {
    pub(super) dl_ranges: Vec<DlRangeSnapshot>,
    pub(super) next_unassigned_idx: usize,
    pub(super) borrow_limit_bytes: u64,
    pub(super) borrow_cursor: usize,
    pub(super) next_range_id: u64,
    pub(super) index_state_bits: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub(super) task: DownloadTask,
    pub(super) coordinator: CoordinatorSnapshot,
}

#[derive(Debug)]
pub(super) struct Coordinator {
    pub dl_ranges: Vec<Rc<ActiveRange>>,
    pub next_unassigned_idx: usize,
    pub borrow_limit_bytes: u64,
    pub adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
    pub borrow_cursor: usize,
    pub next_range_id: u64,
    pub total_size: u64,
    pub index_state: Rc<IndexStateMap>,
    pub log_file: StdFile,
    pub metrics: Rc<SchedulerMetrics>,
}

#[derive(Debug)]
pub(super) struct IndexStateMap {
    pub(super) total_size: u64,
    pub(super) buckets: Vec<Cell<u8>>,
}

impl IndexStateMap {
    pub(super) fn new(total_size: u64) -> Self {
        let bucket_count = total_size.div_ceil(INDEX_STATE_BYTES) as usize;
        let mut buckets = Vec::with_capacity(bucket_count);
        buckets.resize_with(bucket_count, || Cell::new(0));
        Self { total_size, buckets }
    }

    pub(super) fn from_snapshot(total_size: u64, bits: Vec<u8>) -> Self {
        let bucket_count = total_size.div_ceil(INDEX_STATE_BYTES) as usize;
        let mut buckets = Vec::with_capacity(bucket_count);
        for idx in 0..bucket_count {
            let value = bits.get(idx).copied().unwrap_or(0);
            buckets.push(Cell::new(value));
        }
        Self { total_size, buckets }
    }

    pub(super) fn snapshot_bits(&self) -> Vec<u8> {
        self.buckets
            .iter()
            .map(|bucket| bucket.get())
            .collect()
    }

    pub(super) fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub(super) fn storage_bytes(&self) -> usize {
        self.buckets.len()
    }

    pub(super) fn completed_slices(&self) -> u64 {
        self.buckets
            .iter()
            .enumerate()
            .map(|(idx, bucket)| {
                let raw = bucket.get();
                let mask = valid_slice_mask(self.total_size, idx);
                (raw & mask).count_ones() as u64
            })
            .sum()
    }

    pub(super) fn mark_completed_span(&self, from_byte: u64, to_byte: u64) {
        if to_byte <= from_byte || self.total_size == 0 {
            return;
        }

        let clamped_from = from_byte.min(self.total_size);
        let clamped_to = to_byte.min(self.total_size);
        if clamped_to <= clamped_from {
            return;
        }

        let start_slice = (clamped_from / MB) as usize;
        let end_slice_exclusive = if clamped_to >= self.total_size {
            self.total_size.div_ceil(MB) as usize
        } else {
            (clamped_to / MB) as usize
        };

        for slice_idx in start_slice..end_slice_exclusive {
            let bucket_idx = slice_idx / 8;
            let bit_idx = slice_idx % 8;
            if let Some(bucket) = self.buckets.get(bucket_idx) {
                bucket.set(bucket.get() | (1_u8 << bit_idx));
            }
        }
    }
}


impl Coordinator {
    pub(super) fn new(
        task_id: Uuid,
        total_size: u64,
        log_path: &Path,
        borrow_limit_mb: u64,
        connections: usize,
        dry_run: bool,
        schedule_mode: ScheduleMode,
        metrics: Rc<SchedulerMetrics>,
        adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
        phi_max_ratio: f64,
    ) -> Result<Self> {
        let fib_mb = build_fib_mb();
        let ceil_mb = total_size.div_ceil(MB);
        let support_idx = fib_mb
            .iter()
            .position(|value| *value >= ceil_mb.max(1))
            .ok_or_else(|| anyhow!("Download exceeds generated Fibonacci range table"))?;

        let seed_ranges = match schedule_mode {
            ScheduleMode::FibAdaptive => {
                build_phi_geometric_ranges(total_size, connections.max(1), phi_max_ratio, STORAGE_BLOCK_SIZE)
            }
            ScheduleMode::Fib => {
                let seed_start_idx = choose_seed_start_idx(&fib_mb, support_idx, connections.max(1), dry_run);
                build_seed_ranges(&fib_mb, seed_start_idx, support_idx, total_size)
            }
            ScheduleMode::Equal => build_equal_ranges(total_size, connections.max(1)),
        };

        let geometry_label = if seed_ranges.len() > 1 {
            let mut labels = Vec::new();
            for r in &seed_ranges {
                let pct = (r.byte_end - r.byte_start) as f64 / total_size as f64 * 100.0;
                labels.push(format!("{:.1}%", pct));
            }
            labels.join(" / ")
        } else {
            "100%".to_string()
        };
        let dl_ranges: Vec<Rc<ActiveRange>> = seed_ranges
            .iter()
            .map(|spec| {
                Rc::new(ActiveRange {
                    id: spec.id + 1,
                    label_start_mb: spec.label_start_mb,
                    label_end_mb: spec.label_end_mb,
                    byte_start: spec.byte_start,
                    assigned_to: Cell::new(UNASSIGNED_CONNECTION),
                    cursor: Cell::new(spec.byte_start),
                    end: Cell::new(spec.byte_end),
                    parent_range_id: None,
                    status: Cell::new(RANGE_STATUS_PENDING),
                    last_sample_cursor: Cell::new(spec.byte_start),
                    last_sample_at_ms: Cell::new(0),
                    recent_speed_bps: Cell::new(0),
                })
            })
            .collect();

        let mut coordinator = Self {
            dl_ranges,
            next_unassigned_idx: 0,
            borrow_limit_bytes: borrow_limit_mb.max(1) * MB,
            adaptive_minimum_steal_bytes,
            borrow_cursor: 0,
            next_range_id: seed_ranges.len() as u64 + 1,
            total_size,
            index_state: Rc::new(IndexStateMap::new(total_size)),
            log_file: StdFile::create(log_path)?,
            metrics,
        };

        coordinator.log(&format!(
            "Coordinator started for task={} total_size={}B ceil_mb={} schedule_mode={} seed_floor_mb={} seed_start={}MB support_end={}MB borrow_limit={}MB dry_run={} phi_max_ratio={:.3} index_state_bucket_mb={} index_state_buckets={} index_state_bytes={}",
            task_id,
            total_size,
            ceil_mb,
            schedule_mode.as_str(),
            if dry_run { 1 } else { LIVE_SEED_FLOOR_MB },
            seed_ranges.first().map(|r| r.label_start_mb).unwrap_or(0),
            fib_mb[support_idx],
            borrow_limit_mb.max(1),
            dry_run,
            phi_max_ratio,
            INDEX_STATE_MB,
            coordinator.index_state.bucket_count(),
            coordinator.index_state.storage_bytes(),
        ));
        coordinator.log(&format!("Initial distribution geometry: [{}]", geometry_label));
        let range_lines: Vec<String> = seed_ranges
            .iter()
            .map(|spec| {
                let fit_end_mb = bytes_to_ceiling_mb(spec.byte_end);
                format!(
                "vector range#{} support={}..{}MB fit_end={}MB bytes={}..{}",
                spec.id,
                spec.label_start_mb,
                spec.label_end_mb,
                fit_end_mb,
                spec.byte_start,
                spec.byte_end
            )
            })
            .collect();
        for line in range_lines {
            coordinator.log(&line);
        }

        Ok(coordinator)
    }

    pub(super) fn from_snapshot(
        snapshot: CoordinatorSnapshot,
        total_size: u64,
        log_path: &Path,
        _schedule_mode: ScheduleMode,
        metrics: Rc<SchedulerMetrics>,
        adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
    ) -> Result<Self> {
        let dl_ranges = snapshot
            .dl_ranges
            .into_iter()
            .map(|range| {
                Rc::new(ActiveRange {
                    id: range.id,
                    label_start_mb: range.label_start_mb,
                    label_end_mb: range.label_end_mb,
                    byte_start: range.byte_start,
                    assigned_to: Cell::new(range.assigned_to),
                    cursor: Cell::new(range.cursor),
                    end: Cell::new(range.end),
                    parent_range_id: range.parent_range_id,
                    status: Cell::new(range.status),
                    last_sample_cursor: Cell::new(range.cursor),
                    last_sample_at_ms: Cell::new(0),
                    recent_speed_bps: Cell::new(0),
                })
            })
            .collect();

        let mut coordinator = Self {
            dl_ranges,
            next_unassigned_idx: snapshot.next_unassigned_idx,
            borrow_limit_bytes: snapshot.borrow_limit_bytes,
            adaptive_minimum_steal_bytes,
            borrow_cursor: snapshot.borrow_cursor,
            next_range_id: snapshot.next_range_id,
            total_size,
            index_state: Rc::new(IndexStateMap::from_snapshot(total_size, snapshot.index_state_bits)),
            log_file: std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)?,
            metrics,
        };

        coordinator.log(&format!(
            "Coordinator resumed from snapshot. index_state_buckets={} index_state_bytes={} completed_slices={}",
            coordinator.index_state.bucket_count(),
            coordinator.index_state.storage_bytes(),
            coordinator.index_state.completed_slices(),
        ));
        Ok(coordinator)
    }

    pub(super) fn log(&mut self, msg: &str) {
        let _ = writeln!(self.log_file, "[{}] {}", chrono::Local::now(), msg);
    }

    pub(super) fn log_summary(&mut self, total_size: u64) {
        self.metrics
            .adaptive_min_steal_bytes_final
            .set(self.adaptive_minimum_steal_bytes.get());
        self.log(&format!(
            "{} total_size={} final_ranges={} vector_consumed={} index_state_buckets={} index_state_bytes={} completed_slices={}",
            self.metrics.summary_line(),
            total_size,
            self.dl_ranges.len(),
            self.next_unassigned_idx,
            self.index_state.bucket_count(),
            self.index_state.storage_bytes(),
            self.index_state.completed_slices(),
        ));
    }

    pub(super) async fn run(&mut self, mut work_rx: mpsc::Receiver<WorkRequest>, control: Rc<RuntimeControl>) {
        while let Some(req) = work_rx.recv().await {
            if control.is_halted() {
                let _ = req.tx.send(None);
                continue;
            }

            let work = self.get_work(req.connection_id);
            if work.is_none() {
                self.log(&format!("conn={} no more work", req.connection_id));
            }
            let _ = req.tx.send(work);
        }
        self.log("Coordinator finished.");
    }

    fn get_work(&mut self, connection_id: u32) -> Option<Rc<ActiveRange>> {
        while self.next_unassigned_idx < self.dl_ranges.len() {
            let range = self.dl_ranges[self.next_unassigned_idx].clone();
            self.next_unassigned_idx += 1;
            if range.assigned_to.get() != UNASSIGNED_CONNECTION {
                continue;
            }
            range.assigned_to.set(connection_id);
            range.status.set(RANGE_STATUS_ACTIVE);
            SchedulerMetrics::add(&self.metrics.direct_assignments, 1);
            self.log(&format!(
                "assign conn={} active_range#{} support={}..{}MB bytes={}..{}",
                connection_id,
                range.id,
                range.label_start_mb,
                range.label_end_mb,
                range.byte_start,
                range.end.get()
            ));
            return Some(range);
        }

        for idx in 0..self.dl_ranges.len() {
            let range = self.dl_ranges[idx].clone();
            if range.assigned_to.get() != UNASSIGNED_CONNECTION {
                continue;
            }
            if range.status.get() != RANGE_STATUS_PENDING {
                continue;
            }
            if range.cursor.get() >= range.end.get() {
                continue;
            }
            range.assigned_to.set(connection_id);
            range.status.set(RANGE_STATUS_ACTIVE);
            SchedulerMetrics::add(&self.metrics.direct_assignments, 1);
            self.log(&format!(
                "reassign conn={} active_range#{} support={}..{}MB bytes={}..{}",
                connection_id,
                range.id,
                range.label_start_mb,
                range.label_end_mb,
                range.cursor.get(),
                range.end.get()
            ));
            return Some(range.clone());
        }

        self.borrow_work(connection_id)
    }

    fn borrow_work(&mut self, connection_id: u32) -> Option<Rc<ActiveRange>> {
        if self.dl_ranges.is_empty() {
            return None;
        }

        if let Some((idx, kind)) = self.select_borrow_candidate(connection_id) {
            return self.split_active_range(idx, connection_id, kind);
        }

        None
    }

    fn select_borrow_candidate(&self, connection_id: u32) -> Option<(usize, BorrowKind)> {
        let effective_limit = self.effective_borrow_limit_bytes();
        let mut active_speeds = Vec::new();
        for range in &self.dl_ranges {
            let owner_connection = range.assigned_to.get();
            if owner_connection == connection_id || owner_connection == UNASSIGNED_CONNECTION {
                continue;
            }
            if range.status.get() != RANGE_STATUS_ACTIVE {
                continue;
            }
            let remaining = range.end.get().saturating_sub(range.cursor.get());
            if remaining < effective_limit {
                continue;
            }
            let speed = range.recent_speed_bps.get();
            if speed > 0 {
                active_speeds.push(speed);
            }
        }

        let median_speed = median_u64(&mut active_speeds);
        let mut best_straggler = None::<(usize, u64)>;
        let total = self.dl_ranges.len();
        for offset in 0..total {
            let idx = (self.borrow_cursor + offset) % total;
            let active = &self.dl_ranges[idx];
            let owner_connection = active.assigned_to.get();
            if owner_connection == connection_id || owner_connection == UNASSIGNED_CONNECTION {
                continue;
            }
            if active.status.get() != RANGE_STATUS_ACTIVE {
                continue;
            }
            let remaining = active.end.get().saturating_sub(active.cursor.get());
            if remaining <= effective_limit.saturating_mul(2) {
                continue;
            }

            let speed = active.recent_speed_bps.get();
            if median_speed > 0
                && speed > 0
                && speed.saturating_mul(100) <= median_speed.saturating_mul(60)
                && remaining >= effective_limit.saturating_mul(3)
            {
                match best_straggler {
                    Some((_, best_remaining)) if best_remaining >= remaining => {}
                    _ => best_straggler = Some((idx, remaining)),
                }
            }
        }

        if let Some((idx, _)) = best_straggler {
            return Some((idx, BorrowKind::Straggler));
        }

        let mut best_standard = None::<(usize, u64)>;
        for offset in 0..total {
            let idx = (self.borrow_cursor + offset) % total;
            let active = &self.dl_ranges[idx];
            let owner_connection = active.assigned_to.get();
            if owner_connection == connection_id || owner_connection == UNASSIGNED_CONNECTION {
                continue;
            }
            if active.status.get() != RANGE_STATUS_ACTIVE {
                continue;
            }
            let remaining = active.end.get().saturating_sub(active.cursor.get());
            if remaining <= effective_limit.saturating_mul(2) {
                continue;
            }
            
            match best_standard {
                Some((_, best_remaining)) if best_remaining >= remaining => {}
                _ => best_standard = Some((idx, remaining)),
            }
        }

        if let Some((idx, _)) = best_standard {
            let kind = if self.is_tail_phase() {
                BorrowKind::Tail
            } else {
                BorrowKind::Standard
            };
            return Some((idx, kind));
        }

        None
    }

    fn split_active_range(
        &mut self,
        idx: usize,
        connection_id: u32,
        kind: BorrowKind,
    ) -> Option<Rc<ActiveRange>> {
        let active = self.dl_ranges[idx].clone();
        let owner_connection = active.assigned_to.get();
        let start = active.cursor.get();
        let end = active.end.get();
        let effective_limit = self.effective_borrow_limit_bytes();
        let remaining = end.saturating_sub(start);
        if remaining <= effective_limit.saturating_mul(2) {
            return None;
        }

        let steal_size = match kind {
            BorrowKind::Straggler => remaining / 2,
            BorrowKind::Tail => remaining / 2,
            BorrowKind::Standard => (((remaining as u128) * (GOLDEN_RATIO_NUM as u128))
                / (GOLDEN_RATIO_DEN as u128)) as u64,
        };
        let aligned_split = align_down(end.saturating_sub(steal_size), MB);
        if aligned_split <= start + effective_limit {
            return None;
        }

        let stolen_size = end.saturating_sub(aligned_split);
        if stolen_size < effective_limit {
            return None;
        }

        active.end.set(aligned_split);

        let borrowed = Rc::new(ActiveRange {
            id: self.next_range_id,
            label_start_mb: active.label_start_mb,
            label_end_mb: active.label_end_mb,
            byte_start: aligned_split,
            assigned_to: Cell::new(connection_id),
            cursor: Cell::new(aligned_split),
            end: Cell::new(end),
            parent_range_id: Some(active.id),
            status: Cell::new(RANGE_STATUS_ACTIVE),
            last_sample_cursor: Cell::new(aligned_split),
            last_sample_at_ms: Cell::new(0),
            recent_speed_bps: Cell::new(0),
        });
        self.next_range_id += 1;
        SchedulerMetrics::add(&self.metrics.borrow_assignments, 1);
        SchedulerMetrics::add(&self.metrics.bytes_borrowed, stolen_size);
        if kind == BorrowKind::Straggler {
            SchedulerMetrics::add(&self.metrics.straggler_splits, 1);
        }
        if kind == BorrowKind::Tail {
            SchedulerMetrics::add(&self.metrics.tail_splits, 1);
        }
        let donor_id = active.id;
        let donor_label_start = active.label_start_mb;
        let donor_label_end = active.label_end_mb;
        let donor_speed = active.recent_speed_bps.get();
        self.dl_ranges.push(borrowed.clone());
        self.borrow_cursor = idx + 1;

        self.log(&format!(
            "borrow kind={} conn={} from_conn={} donor_range#{} new_range#{} support={}..{}MB bytes={}..{} donor_speed_Bps={}",
            kind.as_str(),
            connection_id,
            owner_connection,
            donor_id,
            borrowed.id,
            donor_label_start,
            donor_label_end,
            aligned_split,
            end,
            donor_speed,
        ));
        Some(borrowed)
    }

    fn effective_borrow_limit_bytes(&self) -> u64 {
        let base_limit = if self.is_tail_phase() {
            self.borrow_limit_bytes.min(MB).max(MB)
        } else {
            self.borrow_limit_bytes
        };
        if self.is_tail_phase() {
            base_limit
        } else {
            base_limit.max(self.adaptive_minimum_steal_bytes.get())
        }
    }

    fn is_tail_phase(&self) -> bool {
        is_tail_phase_bytes(snapshot_downloaded(self, self.total_size), self.total_size)
    }

    pub(super) fn snapshot(&self) -> CoordinatorSnapshot {
        CoordinatorSnapshot {
            dl_ranges: self
                .dl_ranges
                .iter()
                .map(|range| DlRangeSnapshot {
                    id: range.id,
                    label_start_mb: range.label_start_mb,
                    label_end_mb: range.label_end_mb,
                    byte_start: range.byte_start,
                    assigned_to: range.assigned_to.get(),
                    cursor: range.cursor.get(),
                    end: range.end.get(),
                    parent_range_id: range.parent_range_id,
                    status: range.status.get(),
                })
                .collect(),
            next_unassigned_idx: self.next_unassigned_idx,
            borrow_limit_bytes: self.borrow_limit_bytes,
            borrow_cursor: self.borrow_cursor,
            next_range_id: self.next_range_id,
            index_state_bits: self.index_state.snapshot_bits(),
        }
    }
}

pub(super) fn snapshot_downloaded(coordinator: &Coordinator, total_size: u64) -> u64 {
    let downloaded = coordinator
        .dl_ranges
        .iter()
        .map(|range| {
            let cursor = range.cursor.get();
            let end = range.end.get();
            cursor.min(end).saturating_sub(range.byte_start)
        })
        .sum::<u64>();

    downloaded.min(total_size)
}

pub(super) fn is_tail_phase_bytes(downloaded: u64, total_size: u64) -> bool {
    if total_size == 0 {
        return false;
    }

    downloaded.saturating_mul(100) >= total_size.saturating_mul(95)
}

fn build_fib_mb() -> Vec<u64> {
    let mut fib = vec![1_u64, 2_u64];
    let max_mb = u64::MAX / MB;
    loop {
        let len = fib.len();
        let next = match fib[len - 1].checked_add(fib[len - 2]) {
            Some(v) if v <= max_mb => v,
            _ => break,
        };
        fib.push(next);
    }
    fib
}

fn choose_seed_start_idx(fib_mb: &[u64], support_idx: usize, connections: usize, dry_run: bool) -> usize {
    if dry_run || support_idx == 0 {
        return 0;
    }

    let desired_start = fib_mb
        .iter()
        .position(|value| *value >= LIVE_SEED_FLOOR_MB)
        .unwrap_or(0);
    let max_start_with_enough_lanes = support_idx.saturating_sub(connections.max(1));
    desired_start.min(max_start_with_enough_lanes)
}

fn build_seed_ranges(
    fib_mb: &[u64],
    seed_start_idx: usize,
    support_idx: usize,
    total_size: u64,
) -> Vec<RangeSpec> {
    let mut ranges = Vec::with_capacity(support_idx.saturating_sub(seed_start_idx));
    let mut byte_start = 0_u64;

    for idx in seed_start_idx..support_idx {
        let label_start_mb = fib_mb[idx];
        let label_end_mb = fib_mb[idx + 1];
        let byte_end = ((label_end_mb as u128) * (MB as u128))
            .min(total_size as u128) as u64;

        if byte_end <= byte_start {
            continue;
        }

        ranges.push(RangeSpec {
            id: ranges.len() as u64,
            label_start_mb,
            label_end_mb,
            byte_start,
            byte_end,
        });

        byte_start = byte_end;
        if byte_start >= total_size {
            break;
        }
    }

    if let Some(last) = ranges.last_mut() {
        last.byte_end = total_size;
    }

    ranges
}

fn build_equal_ranges(total_size: u64, connections: usize) -> Vec<RangeSpec> {
    if total_size == 0 || connections == 0 {
        return Vec::new();
    }

    let lanes = connections.min(total_size.div_ceil(MB) as usize).max(1);
    let mut ranges = Vec::with_capacity(lanes);
    let mut start = 0_u64;

    for idx in 0..lanes {
        let remaining_bytes = total_size.saturating_sub(start);
        let remaining_lanes = (lanes - idx) as u64;
        let chunk_size = remaining_bytes.div_ceil(remaining_lanes);
        let end = if idx + 1 == lanes {
            total_size
        } else {
            (start + chunk_size).min(total_size)
        };

        if end <= start {
            continue;
        }

        ranges.push(RangeSpec {
            id: ranges.len() as u64,
            label_start_mb: bytes_to_floor_mb(start),
            label_end_mb: bytes_to_ceiling_mb(end),
            byte_start: start,
            byte_end: end,
        });
        start = end;
    }

    ranges
}

fn build_phi_geometric_ranges(
    file_size: u64,
    n: usize,
    max_ratio: f64,
    block_size: u64,
) -> Vec<RangeSpec> {
    if file_size == 0 || n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![RangeSpec {
            id: 0,
            label_start_mb: bytes_to_floor_mb(0),
            label_end_mb: bytes_to_ceiling_mb(file_size),
            byte_start: 0,
            byte_end: file_size,
        }];
    }

    let phi: f64 = 1.6180339887498948482;
    let alpha = max_ratio.ln() / ((n - 1) as f64 * phi.ln());

    let mut weights = Vec::with_capacity(n);
    let mut w_sum = 0.0;
    for i in 0..n {
        let w = phi.powf(alpha * (i as f64));
        weights.push(w);
        w_sum += w;
    }

    let mut boundaries = Vec::with_capacity(n + 1);
    boundaries.push(0_u64);

    let mut cumsum = 0.0;
    for i in 0..n - 1 {
        cumsum += file_size as f64 * weights[i] / w_sum;
        let mut boundary = (cumsum / block_size as f64).round() as u64 * block_size;
        boundary = boundary.clamp(boundaries[i], file_size);
        boundaries.push(boundary);
    }
    boundaries.push(file_size);

    if boundaries[1] - boundaries[0] < block_size {
        return build_equal_ranges(file_size, n);
    }

    let mut ranges = Vec::with_capacity(n);
    for i in 0..n {
        let byte_start = boundaries[i];
        let byte_end = boundaries[i + 1];
        if byte_end > byte_start {
            ranges.push(RangeSpec {
                id: i as u64,
                label_start_mb: bytes_to_floor_mb(byte_start),
                label_end_mb: bytes_to_ceiling_mb(byte_end),
                byte_start,
                byte_end,
            });
        }
    }

    ranges
}


fn valid_slice_mask(total_size: u64, bucket_idx: usize) -> u8 {
    if total_size == 0 {
        return 0;
    }

    let total_slices = total_size.div_ceil(MB) as usize;
    let bucket_start_slice = bucket_idx * 8;
    if bucket_start_slice >= total_slices {
        return 0;
    }

    let remaining = total_slices - bucket_start_slice;
    if remaining >= 8 {
        0xFF
    } else {
        ((1_u16 << remaining) - 1) as u8
    }
}

fn median_u64(values: &mut [u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}
