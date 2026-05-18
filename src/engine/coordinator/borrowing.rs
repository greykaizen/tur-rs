use super::*;

impl Coordinator {
    pub(super) fn borrow_work(&mut self, connection_id: u32) -> Option<Rc<ActiveRange>> {
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
            BorrowKind::Straggler | BorrowKind::Tail => remaining / 2,
            BorrowKind::Standard => {
                (((remaining as u128) * (GOLDEN_RATIO_NUM as u128)) / (GOLDEN_RATIO_DEN as u128))
                    as u64
            }
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
        super::ranges::is_tail_phase_bytes(
            super::ranges::snapshot_downloaded(self, self.total_size),
            self.total_size,
        )
    }
}

fn median_u64(values: &mut [u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}
