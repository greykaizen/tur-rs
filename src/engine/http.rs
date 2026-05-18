use super::*;

pub(super) fn compute_http2_client_tuning(expected_concurrency: usize) -> ClientTuning {
    let concurrency = expected_concurrency.max(1);
    let stream_window_bytes =
        ((concurrency as u32).saturating_mul(MB as u32)).clamp(4 * MB as u32, 16 * MB as u32);
    let connection_window_bytes = stream_window_bytes
        .saturating_mul(concurrency as u32)
        .clamp(16 * MB as u32, 64 * MB as u32);
    let max_send_buffer_bytes =
        (concurrency.saturating_mul(512 * 1024)).clamp(2 * MB as usize, 8 * MB as usize);
    ClientTuning {
        expected_concurrency: concurrency,
        http2_stream_window_bytes: stream_window_bytes,
        http2_connection_window_bytes: connection_window_bytes,
        http2_max_send_buffer_bytes: max_send_buffer_bytes,
        source: H2TuningSource::Default,
    }
}

pub(super) fn learn_http2_client_tuning(
    expected_concurrency: usize,
    max_active_connections_observed: usize,
    ewma_rtt_ms: f64,
    ewma_throughput_bps: f64,
) -> ClientTuning {
    let observed_concurrency = max_active_connections_observed.max(1);
    let per_stream_throughput = (ewma_throughput_bps / observed_concurrency as f64).max(1.0);
    let bdp_bytes = (per_stream_throughput * (ewma_rtt_ms.max(1.0) / 1000.0)).round();
    let stream_window_bytes =
        ((bdp_bytes as u32).saturating_mul(4)).clamp(MB as u32, 16 * MB as u32);
    let connection_window_bytes = stream_window_bytes
        .saturating_mul(expected_concurrency.max(observed_concurrency) as u32)
        .clamp(16 * MB as u32, 64 * MB as u32);
    let max_send_buffer_bytes = ((stream_window_bytes as usize)
        .saturating_mul(expected_concurrency.max(observed_concurrency))
        / 2)
        .clamp(2 * MB as usize, 8 * MB as usize);
    ClientTuning {
        expected_concurrency: expected_concurrency.max(observed_concurrency),
        http2_stream_window_bytes: stream_window_bytes,
        http2_connection_window_bytes: connection_window_bytes,
        http2_max_send_buffer_bytes: max_send_buffer_bytes,
        source: H2TuningSource::LearnedOrigin,
    }
}

pub(super) fn build_http_client(
    http_mode: HttpMode,
    expected_concurrency: usize,
    learned_tuning: Option<ClientTuning>,
) -> (DownloadHttpClient, ClientTuning) {
    let http = crate::connector::TunedConnector::new();
    let tuning = learned_tuning.unwrap_or_else(|| compute_http2_client_tuning(expected_concurrency));
    let https = match http_mode {
        HttpMode::Http3 => {
            eprintln!("WARNING: HTTP/3 mode selected but the http3 feature is not enabled. Falling back to HTTP/1.1. Rebuild with --features http3 to enable QUIC support.");
            HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .wrap_connector(http)
        }
        HttpMode::Auto => HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http),
        HttpMode::Http1 => HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(http),
        HttpMode::Http2 => HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http2()
            .wrap_connector(http),
    };

    let mut builder = HyperClient::builder(TokioExecutor::new());
    builder.pool_timer(TokioTimer::new());
    builder.pool_idle_timeout(Duration::from_secs(30));
    builder.pool_max_idle_per_host(32);
    builder.retry_canceled_requests(true);
    builder.http1_writev(true);
    builder.http2_adaptive_window(true);
    builder.http2_initial_stream_window_size(Some(tuning.http2_stream_window_bytes));
    builder.http2_initial_connection_window_size(Some(tuning.http2_connection_window_bytes));
    builder.http2_max_frame_size(Some(HTTP2_MAX_FRAME_BYTES));
    builder.http2_max_send_buf_size(tuning.http2_max_send_buffer_bytes);
    builder.http2_keep_alive_interval(Some(Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS)));
    builder.http2_keep_alive_timeout(Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS * 2));
    builder.http2_keep_alive_while_idle(true);
    builder.timer(TokioTimer::new());
    (builder.build(https), tuning)
}

pub(super) async fn send_request_follow_redirects(
    client: &DownloadHttpClient,
    method: Method,
    url: &str,
    range: Option<(u64, u64)>,
) -> Result<hyper::Response<Incoming>> {
    let mut current_url = url.to_owned();

    for _ in 0..=MAX_REDIRECTS {
        let uri: Uri = current_url.parse()?;
        let mut builder = Request::builder()
            .method(method.clone())
            .uri(uri)
            .header(USER_AGENT, USER_AGENT_VALUE)
            .header(ACCEPT, "*/*");
        if let Some((start, end)) = range {
            builder = builder.header(RANGE, format!("bytes={}-{}", start, end));
        }
        let request = builder.body(Empty::<Bytes>::new())?;
        let response: hyper::Response<Incoming> = client.request(request).await?;

        if response.status().is_redirection() {
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value: &::http::HeaderValue| value.to_str().ok())
                .ok_or_else(|| anyhow!("redirect missing location header"))?;
            current_url = resolve_redirect_url(&current_url, location)?;
            continue;
        }

        return Ok(response);
    }

    Err(anyhow!("too many redirects for {}", url))
}

pub(super) fn resolve_redirect_url(base: &str, location: &str) -> Result<String> {
    let base = Url::parse(base)?;
    Ok(base.join(location)?.to_string())
}

pub(super) fn align_down(value: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        return value;
    }
    (value / alignment) * alignment
}

pub(super) fn bytes_to_ceiling_mb(bytes: u64) -> u64 {
    bytes.div_ceil(MB)
}

pub(super) fn bytes_to_floor_mb(bytes: u64) -> u64 {
    bytes / MB
}

pub(super) fn http_version_label(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/?",
    }
}

pub(super) fn protocol_family_for_version(version: Version) -> ProtocolFamily {
    match version {
        Version::HTTP_09 | Version::HTTP_10 | Version::HTTP_11 => ProtocolFamily::Http1,
        Version::HTTP_2 => ProtocolFamily::Http2,
        Version::HTTP_3 => ProtocolFamily::Http3,
        _ => ProtocolFamily::Other,
    }
}

pub(super) fn dominant_protocol_from_metrics(
    metrics: &Rc<SchedulerMetrics>,
    fallback: ProtocolFamily,
) -> ProtocolFamily {
    let http1 = metrics.http1_requests.get();
    let http2 = metrics.http2_requests.get();
    let http3 = metrics.http3_requests.get();
    if http1 == 0 && http2 == 0 && http3 == 0 {
        fallback
    } else if http3 > http2 && http3 > http1 {
        ProtocolFamily::Http3
    } else if http2 > http1 {
        ProtocolFamily::Http2
    } else {
        ProtocolFamily::Http1
    }
}

pub(super) fn record_protocol_request_metric(
    metrics: &Rc<SchedulerMetrics>,
    protocol: ProtocolFamily,
) {
    match protocol {
        ProtocolFamily::Http1 => SchedulerMetrics::add(&metrics.http1_requests, 1),
        ProtocolFamily::Http2 => SchedulerMetrics::add(&metrics.http2_requests, 1),
        ProtocolFamily::Http3 => SchedulerMetrics::add(&metrics.http3_requests, 1),
        ProtocolFamily::Other => SchedulerMetrics::add(&metrics.http_other_requests, 1),
    }
}

pub(super) fn record_protocol_scale_metric(
    metrics: &Rc<SchedulerMetrics>,
    protocol: ProtocolFamily,
    action: ScalerAction,
    is_stream_add: bool,
) {
    match (protocol, action) {
        (ProtocolFamily::Http1, ScalerAction::Grow) => {
            SchedulerMetrics::add(&metrics.http1_scale_adds, 1)
        }
        (ProtocolFamily::Http1, ScalerAction::Shrink) => {
            SchedulerMetrics::add(&metrics.http1_scale_drops, 1)
        }
        (ProtocolFamily::Http2, ScalerAction::Grow) if is_stream_add => {
            SchedulerMetrics::add(&metrics.http2_stream_adds, 1);
            SchedulerMetrics::add(&metrics.http2_scale_adds, 1)
        }
        (ProtocolFamily::Http2, ScalerAction::Grow) => {
            SchedulerMetrics::add(&metrics.http2_conn_adds, 1);
            SchedulerMetrics::add(&metrics.http2_scale_adds, 1)
        }
        (ProtocolFamily::Http2, ScalerAction::Shrink) => {
            SchedulerMetrics::add(&metrics.http2_scale_drops, 1)
        }
        (ProtocolFamily::Http3, ScalerAction::Grow) => {
            SchedulerMetrics::add(&metrics.http3_scale_adds, 1)
        }
        (ProtocolFamily::Http3, ScalerAction::Shrink) => {
            SchedulerMetrics::add(&metrics.http3_scale_drops, 1)
        }
        _ => {}
    }
}

pub(super) fn record_max_active_connections(metrics: &Rc<SchedulerMetrics>, n_active: usize) {
    let observed = n_active as u64;
    if observed > metrics.max_active_connections_observed.get() {
        metrics.max_active_connections_observed.set(observed);
    }
}

pub(super) fn protocol_prefetch_handshake_ms(protocol: ProtocolFamily) -> u64 {
    match protocol {
        ProtocolFamily::Http2 => 250,
        ProtocolFamily::Http3 => 200,
        ProtocolFamily::Http1 | ProtocolFamily::Other => LIVE_PREFETCH_HANDSHAKE_MS,
    }
}

pub(super) fn protocol_growth_shield_min_samples(protocol: ProtocolFamily) -> u64 {
    match protocol {
        ProtocolFamily::Http1 => 1,
        ProtocolFamily::Http2 => MIN_REUSED_RTT_SAMPLES_FOR_GROWTH_SHIELD,
        ProtocolFamily::Http3 => 0,
        ProtocolFamily::Other => MIN_REUSED_RTT_SAMPLES_FOR_GROWTH_SHIELD,
    }
}

pub(super) fn protocol_growth_shield_multiplier(protocol: ProtocolFamily) -> f64 {
    match protocol {
        ProtocolFamily::Http1 => 1.5,
        ProtocolFamily::Http2 => 1.0,
        ProtocolFamily::Http3 => 1.0,
        ProtocolFamily::Other => 1.0,
    }
}

pub(super) fn protocol_reuse_thresholds(protocol: ProtocolFamily) -> (f64, f64) {
    match protocol {
        ProtocolFamily::Http1 => (0.60, 0.80),
        ProtocolFamily::Http2 => (0.35, 0.60),
        ProtocolFamily::Http3 => (0.30, 0.55),
        ProtocolFamily::Other => (0.50, 0.70),
    }
}

pub(super) fn protocol_effective_add_threshold(protocol: ProtocolFamily, reuse_rate: f64) -> f64 {
    match protocol {
        ProtocolFamily::Http1 => {
            if reuse_rate < 0.60 {
                0.12
            } else if reuse_rate < 0.80 {
                0.08
            } else {
                0.06
            }
        }
        ProtocolFamily::Http2 => {
            if reuse_rate < 0.35 {
                0.08
            } else if reuse_rate < 0.60 {
                0.06
            } else {
                0.04
            }
        }
        ProtocolFamily::Http3 => {
            if reuse_rate < 0.30 {
                0.20
            } else if reuse_rate < 0.55 {
                0.12
            } else {
                0.06
            }
        }
        ProtocolFamily::Other => {
            if reuse_rate < 0.50 {
                0.10
            } else {
                0.05
            }
        }
    }
}

pub(super) fn compute_protocol_aware_steal_floor_bytes(
    protocol: ProtocolFamily,
    reuse_rate: f64,
    bytes_per_heartbeat: f64,
) -> u64 {
    let modifier = match protocol {
        ProtocolFamily::Http1 => {
            if reuse_rate < 0.60 { 1.15 } else { 1.0 }
        }
        ProtocolFamily::Http2 => {
            if reuse_rate > 0.60 { 0.75 } else { 0.90 }
        }
        ProtocolFamily::Http3 => {
            if reuse_rate > 0.55 { 0.70 } else { 0.85 }
        }
        ProtocolFamily::Other => 1.0,
    };
    let floor = match protocol {
        ProtocolFamily::Http2 | ProtocolFamily::Http3 => STORAGE_BLOCK_SIZE,
        ProtocolFamily::Http1 | ProtocolFamily::Other => 2 * STORAGE_BLOCK_SIZE,
    };
    (((bytes_per_heartbeat / 4.0) * modifier).round() as u64).max(floor)
}
