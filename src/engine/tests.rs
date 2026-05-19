use super::*;
use crate::engine::coordinator::{CoordinatorSnapshot, DlRangeSnapshot, RANGE_STATUS_ACTIVE};
use crate::engine::http::compute_http2_client_tuning;
use crate::engine::http::{extract_origin, strip_sensitive_headers};

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
    assert_eq!(
        origin_key("https://example.com/path"),
        "https://example.com:443"
    );
    assert_eq!(
        origin_key("http://example.com/test"),
        "http://example.com:80"
    );
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
fn resume_prior_policy_uses_age_buckets() {
    let fresh = compute_resume_prior_policy(1_000, 31_000);
    assert_eq!(fresh.kind, ResumePriorKind::Fresh);
    assert_eq!(fresh.weight, 1.0);

    let decayed = compute_resume_prior_policy(1_000, 91_000);
    assert_eq!(decayed.kind, ResumePriorKind::Decayed);
    assert_eq!(decayed.weight, 0.5);

    let stale = compute_resume_prior_policy(1_000, 400_001);
    assert_eq!(stale.kind, ResumePriorKind::Stale);
    assert_eq!(stale.weight, 0.0);
}

#[test]
fn blend_resume_prior_respects_weight() {
    assert_eq!(blend_resume_prior(10.0, 100.0, 0.0), 10.0);
    assert_eq!(blend_resume_prior(10.0, 100.0, 1.0), 100.0);
    assert_eq!(blend_resume_prior(10.0, 100.0, 0.5), 55.0);
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
        store
            .current_tuning("https://example.com:443")
            .unwrap()
            .source,
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

#[test]
fn extract_origin_parses_scheme_host_port() {
    assert_eq!(
        extract_origin("https://example.com/file.zip"),
        "https://example.com:443"
    );
    assert_eq!(
        extract_origin("http://example.com:8080/file.zip"),
        "http://example.com:8080"
    );
    assert_eq!(
        extract_origin("https://cdn.example.com/path?a=1"),
        "https://cdn.example.com:443"
    );
    assert_eq!(extract_origin("not a url"), "");
    // Same origin detection
    assert_eq!(
        extract_origin("https://example.com:443/foo"),
        extract_origin("https://example.com/bar"),
    );
    // Different origins
    assert_ne!(
        extract_origin("https://example.com/page"),
        extract_origin("https://other.example.com/page"),
    );
}

#[test]
fn strip_sensitive_headers_removes_auth_cookie_referer() {
    use ::http::HeaderMap;

    let mut headers = HeaderMap::new();
    headers.insert("x-custom", "keep".parse().unwrap());
    headers.insert(
        ::http::header::AUTHORIZATION,
        "Bearer token".parse().unwrap(),
    );
    headers.insert(::http::header::COOKIE, "session=abc".parse().unwrap());
    headers.insert(
        ::http::header::REFERER,
        "https://origin.com".parse().unwrap(),
    );
    headers.insert(::http::header::ACCEPT, "text/html".parse().unwrap());

    let safe = strip_sensitive_headers(&headers);

    assert!(safe.get("x-custom").is_some());
    assert!(safe.get(::http::header::ACCEPT).is_some());
    assert!(safe.get(::http::header::AUTHORIZATION).is_none());
    assert!(safe.get(::http::header::COOKIE).is_none());
    assert!(safe.get(::http::header::REFERER).is_none());
    assert_eq!(safe.len(), 2); // x-custom + accept
}

#[test]
fn classify_response_body_returns_none_for_binary_data() {
    assert!(classify_response_body(&[0u8; 100]).is_none());
    assert!(classify_response_body(b"{\"key\": \"value\"}").is_none());
}

#[test]
fn classify_response_body_returns_none_for_short_data() {
    assert!(classify_response_body(b"<html>").is_none());
}

#[test]
fn classify_response_body_detects_cloudflare_challenge() {
    let body = b"<html><body>Checking your browser before accessing... <title>Attention Required! | Cloudflare</title></body></html>";
    let result = classify_response_body(body);
    assert!(matches!(result, Some(ChallengeKind::CloudflareChallenge)));
}

#[test]
fn classify_response_body_detects_captcha() {
    let body = b"<html><body><div class=\"g-recaptcha\">Please complete the CAPTCHA challenge</div></body></html>";
    let result = classify_response_body(body);
    assert!(matches!(result, Some(ChallengeKind::CaptchaChallenge)));
}

#[test]
fn classify_response_body_returns_unexpected_html_for_unknown_html() {
    let body = b"<html><body><h1>Welcome</h1><p>Some content here</p></body></html>";
    let result = classify_response_body(body);
    assert!(matches!(result, Some(ChallengeKind::UnexpectedHtml)));
}

#[test]
fn cookie_jar_match_url_filters_by_domain_path_and_secure() {
    let mut jar = crate::service::CookieJar::new();
    jar.insert(crate::service::CookieEntry::new(
        "session",
        "abc",
        "example.com",
    ));
    jar.insert(crate::service::CookieEntry::new(
        "tracking", "xyz", "ads.com",
    ));

    let url = url::Url::parse("https://example.com/page").unwrap();
    let matched = jar.match_url(&url);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].name, "session");

    let header_val = jar.header_value_for_url(&url);
    assert_eq!(header_val, Some("session=abc".to_string()));

    // Non-matching URL
    let other_url = url::Url::parse("https://other.com/page").unwrap();
    assert!(jar.match_url(&other_url).is_empty());
}

#[test]
fn cookie_jar_header_value_for_url_returns_none_when_empty() {
    let jar = crate::service::CookieJar::new();
    let url = url::Url::parse("https://example.com").unwrap();
    assert!(jar.header_value_for_url(&url).is_none());
}

#[test]
fn challenge_requires_browser_session_returns_false_for_non_aborting_types() {
    // UnexpectedHtml and AuthInterstitial do NOT trigger the abort path.
    // Only known-challenge types (Cloudflare, CAPTCHA, BrowserCheck) abort.
    assert!(!ChallengeKind::UnexpectedHtml.requires_browser_session());
    assert!(!ChallengeKind::AuthInterstitial.requires_browser_session());

    // Known challenge types DO abort.
    assert!(ChallengeKind::CloudflareChallenge.requires_browser_session());
    assert!(ChallengeKind::CaptchaChallenge.requires_browser_session());
    assert!(ChallengeKind::BrowserCheck.requires_browser_session());
}

#[test]
fn service_cookie_jar_merges_into_request_context_with_dedup() {
    use crate::service::{CookieEntry, CookieJar, RequestContext};
    use url::Url;

    // Setup: service cookie jar with cookies for example.com
    let mut jar = CookieJar::new();
    jar.insert(CookieEntry::new("session", "abc", "example.com"));
    jar.insert(CookieEntry::new("tracking", "xyz", "example.com"));

    // Setup: per-request cookies (user-supplied)
    let mut ctx = RequestContext::new();
    ctx.cookies = Some(vec![
        CookieEntry::new("session", "override", "example.com"), // same name/domain — should shadow jar
        CookieEntry::new("custom", "val", "example.com"),
    ]);

    // Simulate the merge logic from TurService::add_download:
    // match URL, collect jar cookies, dedup against existing per-request cookies
    let url = Url::parse("https://example.com/page").unwrap();
    let jar_cookies: Vec<CookieEntry> = jar.match_url(&url).into_iter().cloned().collect();

    let mut existing = ctx.cookies.take().unwrap_or_default();
    for c in jar_cookies {
        if !existing
            .iter()
            .any(|ec| ec.name == c.name && ec.domain == c.domain && ec.path == c.path)
        {
            existing.push(c);
        }
    }
    ctx.cookies = Some(existing);

    // Verify: 3 cookies total (custom, session(override), tracking)
    let cookies = ctx.cookies.as_ref().unwrap();
    assert_eq!(
        cookies.len(),
        3,
        "should have custom + session(override) + tracking"
    );

    // Verify per-request "session" override was NOT shadowed by jar's "session"
    let session = cookies.iter().find(|c| c.name == "session").unwrap();
    assert_eq!(
        session.value, "override",
        "per-request session cookie should take priority over jar session cookie"
    );

    // Verify tracking cookie from jar was merged in
    assert!(
        cookies.iter().any(|c| c.name == "tracking"),
        "jar tracking cookie should be present"
    );

    // Verify custom cookie from per-request context is preserved
    assert!(
        cookies.iter().any(|c| c.name == "custom"),
        "per-request custom cookie should be present"
    );
}

#[test]
fn coordinator_resume_clears_dead_assignments_and_active_status() {
    let snapshot = CoordinatorSnapshot {
        dl_ranges: vec![
            DlRangeSnapshot {
                id: 1,
                label_start_mb: 0,
                label_end_mb: 64,
                byte_start: 0,
                assigned_to: 3,
                cursor: 8 * MB,
                end: 16 * MB,
                parent_range_id: None,
                status: RANGE_STATUS_ACTIVE,
            },
            DlRangeSnapshot {
                id: 2,
                label_start_mb: 64,
                label_end_mb: 128,
                byte_start: 64 * MB,
                assigned_to: 7,
                cursor: 72 * MB,
                end: 80 * MB,
                parent_range_id: Some(1),
                status: RANGE_STATUS_PENDING,
            },
            DlRangeSnapshot {
                id: 3,
                label_start_mb: 128,
                label_end_mb: 192,
                byte_start: 128 * MB,
                assigned_to: 11,
                cursor: 160 * MB,
                end: 160 * MB,
                parent_range_id: None,
                status: RANGE_STATUS_FINISHED,
            },
        ],
        next_unassigned_idx: 2,
        borrow_limit_bytes: 2 * MB,
        borrow_cursor: 0,
        next_range_id: 4,
        index_state_bits: vec![0],
    };

    let log_path =
        std::env::temp_dir().join(format!("tur-coordinator-resume-{}.log", std::process::id()));
    let metrics = Rc::new(SchedulerMetrics::default());
    let adaptive_minimum_steal_bytes = Rc::new(Cell::new(2 * STORAGE_BLOCK_SIZE));

    let coordinator = Coordinator::from_snapshot(
        snapshot,
        192 * MB,
        &log_path,
        ScheduleMode::FibAdaptive,
        metrics,
        adaptive_minimum_steal_bytes,
    )
    .expect("coordinator resumes");

    let first = coordinator.dl_ranges[0].clone();
    assert_eq!(first.assigned_to.get(), UNASSIGNED_CONNECTION);
    assert_eq!(first.status.get(), RANGE_STATUS_PENDING);

    let second = coordinator.dl_ranges[1].clone();
    assert_eq!(second.assigned_to.get(), UNASSIGNED_CONNECTION);
    assert_eq!(second.status.get(), RANGE_STATUS_PENDING);

    let third = coordinator.dl_ranges[2].clone();
    assert_eq!(third.status.get(), RANGE_STATUS_FINISHED);
    assert_eq!(third.assigned_to.get(), 11);

    let _ = std::fs::remove_file(log_path);
}
