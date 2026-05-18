use super::*;

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
