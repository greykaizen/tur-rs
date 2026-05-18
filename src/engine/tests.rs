use super::*;
use crate::engine::http::compute_http2_client_tuning;

#[test]
fn memory_budget_scales_by_available_memory() {
    assert_eq!(compute_effective_connection_budget(32, 2048), 32);
    assert_eq!(compute_effective_connection_budget(32, 900), 24);
    assert_eq!(compute_effective_connection_budget(32, 400), 16);
    assert_eq!(compute_effective_connection_budget(32, 200), 8);
    assert_eq!(compute_effective_connection_budget(1, 200), 1);
}

#[test]
fn connection_cv_requires_meaningful_samples() {
    assert!(compute_connection_cv(&[]).is_none());
    assert!(compute_connection_cv(&[0.0, 0.0]).is_none());
    let cv = compute_connection_cv(&[100.0, 100.0, 200.0, 200.0]).unwrap();
    assert!(cv > 0.0);
}

#[test]
fn origin_key_normalizes_scheme_host_and_port() {
    assert_eq!(origin_key("https://example.com/path"), "https://example.com:443");
    assert_eq!(origin_key("http://example.com/test"), "http://example.com:80");
    assert_eq!(origin_key("not a url"), "not a url");
}

#[test]
fn origin_phi_ratio_store_prunes_least_recently_used_entries() {
    let mut store = OriginPhiRatioStore::default();
    for idx in 0..ORIGIN_PHI_RATIO_CAPACITY {
        store.update_origin_ratio(format!("https://host{idx}.example:443"), 1.1);
    }
    assert_eq!(store.len(), ORIGIN_PHI_RATIO_CAPACITY);

    let keep_key = "https://host0.example:443";
    assert_eq!(store.ratio_for_origin(keep_key), 1.1);

    store.update_origin_ratio("https://new.example:443".to_string(), 1.9);
    assert_eq!(store.len(), ORIGIN_PHI_RATIO_CAPACITY);
    assert_eq!(store.current_ratio(keep_key), Some(1.1));
    assert_eq!(store.current_ratio("https://new.example:443"), Some(1.9));
}

#[test]
fn protocol_thresholds_are_directionally_sensible() {
    assert!(
        protocol_effective_add_threshold(ProtocolFamily::Http1, 0.30)
            > protocol_effective_add_threshold(ProtocolFamily::Http2, 0.30)
    );
    assert!(
        compute_protocol_aware_steal_floor_bytes(ProtocolFamily::Http2, 0.80, 0.0)
            < compute_protocol_aware_steal_floor_bytes(ProtocolFamily::Http1, 0.80, 0.0)
    );
    assert_eq!(protocol_prefetch_handshake_ms(ProtocolFamily::Http2), 250);
}

#[test]
fn http2_client_tuning_scales_with_expected_concurrency() {
    let small = compute_http2_client_tuning(2);
    let large = compute_http2_client_tuning(16);
    assert!(large.http2_stream_window_bytes >= small.http2_stream_window_bytes);
    assert!(large.http2_connection_window_bytes >= small.http2_connection_window_bytes);
    assert!(large.http2_max_send_buffer_bytes >= small.http2_max_send_buffer_bytes);
}

#[test]
fn learned_http2_tuning_is_origin_scoped_and_pruned() {
    let mut store = OriginH2TuningStore::default();
    let tuning = learn_http2_client_tuning(4, 3, 120.0, 12.0 * MB as f64);
    store.update_origin_tuning("https://example.com:443".to_string(), tuning);
    assert_eq!(
        store.current_tuning("https://example.com:443").unwrap().source,
        H2TuningSource::LearnedOrigin
    );

    for idx in 0..ORIGIN_PHI_RATIO_CAPACITY {
        store.update_origin_tuning(
            format!("https://host{idx}.example:443"),
            compute_http2_client_tuning(2),
        );
    }
    assert!(store.len() <= ORIGIN_PHI_RATIO_CAPACITY);
}
