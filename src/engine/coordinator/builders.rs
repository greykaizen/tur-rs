use super::*;

pub(super) fn build_fib_mb() -> Vec<u64> {
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

pub(super) fn choose_seed_start_idx(
    fib_mb: &[u64],
    support_idx: usize,
    connections: usize,
    dry_run: bool,
) -> usize {
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

pub(super) fn build_seed_ranges(
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
        let byte_end = ((label_end_mb as u128) * (MB as u128)).min(total_size as u128) as u64;

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

pub(super) fn build_equal_ranges(total_size: u64, connections: usize) -> Vec<RangeSpec> {
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

pub(super) fn build_phi_geometric_ranges(
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
