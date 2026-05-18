#[derive(Debug, Clone, Copy)]
pub(super) struct OriginPhiRatioEntry {
    pub ratio: f64,
    pub last_used_tick: u64,
}
