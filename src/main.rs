//! tur-rs binary entry point.
//!
//! This is deliberately thin — all real logic lives in the library crate
//! (`src/lib.rs`). The binary only handles CLI parsing, runtime setup, and
//! dispatching to the headless or TUI frontend.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use uuid::Uuid;

use tur_rs::cli::Cli;
use tur_rs::engine::{
    DownloadEngine, DownloadStatus, DownloadTask, EngineCommand, EngineEvent, HttpMode,
    ScheduleMode,
};
use tur_rs::service::RequestContext;
use tur_rs::storage::StorageConfig;

#[cfg(feature = "tui")]
use tur_rs::tui::TuiApp;

fn main() -> Result<()> {
    // Install the ring-based CryptoProvider for rustls before any TLS code runs.
    // Required when the http3 feature is enabled because rustls 0.23 ships with
    // both aws-lc-rs and ring, and cannot auto-determine which to use.
    #[cfg(feature = "http3")]
    {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    let cli = Cli::parse();
    if cli.runtime_threads > 1 {
        eprintln!(
            "WARNING: --runtime-threads={} is currently orchestration-only. Worker execution still runs on the LocalSet, so tur is using a single-threaded runtime until the 0.6.x Send-safe worker split lands.",
            cli.runtime_threads
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = LocalSet::new();
    local.block_on(&runtime, async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    if cli.headless {
        return run_headless(cli).await;
    }

    #[cfg(feature = "tui")]
    {
        return run_tui(cli).await;
    }

    #[cfg(not(feature = "tui"))]
    {
        eprintln!("TUI mode requires the 'tui' feature. Falling back to headless mode.");
        run_headless(cli).await
    }
}

fn build_request_context(cli: &Cli) -> Option<RequestContext> {
    let mut ctx = RequestContext::new();

    // Parse --header flags
    for h in &cli.header {
        if let Some(eq_pos) = h.find(':') {
            let name = h[..eq_pos].trim().to_string();
            let value = h[eq_pos + 1..].trim().to_string();
            if !name.is_empty() {
                ctx = ctx.header(name, value);
            }
        } else {
            eprintln!("WARNING: ignoring malformed --header value (expected \"Name: value\"): {}", h);
        }
    }

    // --referer
    if let Some(ref referer) = cli.referer {
        ctx = ctx.referer(referer.clone());
    }

    // --auth-bearer
    if let Some(ref token) = cli.auth_bearer {
        ctx = ctx.auth(format!("Bearer {}", token));
    }

    // Return None if nothing was set
    if ctx.headers.is_empty() && ctx.auth.is_none() && ctx.referer.is_none() {
        return None;
    }
    Some(ctx)
}

async fn import_cookie_file(ctx: &mut Option<RequestContext>, path: &Option<String>, urls: &[String]) {
    let Some(path_str) = path else { return };
    let path = PathBuf::from(path_str);
    let contents = match tokio::fs::read_to_string(&path).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("WARNING: failed to read cookie file {}: {}", path.display(), e);
            return;
        }
    };

    let mut cookie_vec = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 7 {
            let domain = parts[0].trim_start_matches('.');
            let path = parts[2];
            let secure = parts[3] == "TRUE";
            let name = parts[5];
            let value = parts[6];
            cookie_vec.push(tur_rs::CookieEntry {
                name: name.to_string(),
                value: value.to_string(),
                domain: domain.to_string(),
                path: path.to_string(),
                secure,
                expires: None,
            });
        } else if let Some(eq_pos) = line.find('=') {
            let name = line[..eq_pos].trim();
            let value = line[eq_pos + 1..].trim();
            if !name.is_empty() {
                // Derive domain from the first download URL so the cookie has
                // meaningful domain context for origin-memory persistence.
                let domain = urls.first()
                    .and_then(|u| url::Url::parse(u).ok())
                    .and_then(|u| u.host_str().map(|h| h.to_string()))
                    .unwrap_or_default();
                cookie_vec.push(tur_rs::CookieEntry::new(name, value, &domain));
            }
        }
    }

    if !cookie_vec.is_empty() {
        let count = cookie_vec.len();
        let ctx_ref = ctx.get_or_insert_with(RequestContext::new);
        ctx_ref.cookies = Some(cookie_vec);
        eprintln!("INFO: imported {} cookie(s) from {}", count, path.display());
    }
}

#[cfg(feature = "tui")]
async fn run_tui(cli: Cli) -> Result<()> {
    let schedule_mode = ScheduleMode::parse(&cli.schedule_mode)?;
    let http_mode = HttpMode::parse(&cli.http_mode)?;
    let (connections, min_connections, max_connections) = resolve_connection_settings(&cli)?;
    let tasks_limit = cli.tasks;
    let max_total_connections = cli.max_total_connections.max(1);
    let global_bandwidth_limit_bps = mbps_to_bps(cli.bandwidth_limit);
    let per_download_bandwidth_limit_bps = mbps_to_bps(cli.per_download_limit);

    let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(100);
    let (event_tx, event_rx) = mpsc::channel::<EngineEvent>(100);

    let engine_cmd_tx = engine_tx.clone();
    let storage_config = StorageConfig {
        use_pwrite: !cli.no_pwrite,
        use_splice: !cli.no_splice,
        no_io_uring: cli.no_io_uring,
        no_direct_io: cli.no_direct_io,
    };
    let engine = DownloadEngine::new(
        connections,
        tasks_limit,
        max_total_connections,
        global_bandwidth_limit_bps,
        !cli.no_origin_memory,
        storage_config,
    );
    tokio::task::spawn_local(async move {
        if let Err(e) = engine.run(engine_rx, engine_cmd_tx, event_tx).await {
            eprintln!("Engine error: {}", e);
        }
    });

    let mut request_context = build_request_context(&cli);
    import_cookie_file(&mut request_context, &cli.cookie_file, &cli.url).await;

    let mut app = TuiApp::new(
        engine_tx.clone(),
        connections,
        min_connections,
        max_connections,
        per_download_bandwidth_limit_bps,
        cli.dry_run,
        cli.dry_run_size_mb,
        cli.borrow_limit_mb,
        schedule_mode,
        http_mode,
        cli.log_root.clone().map(PathBuf::from),
        request_context,
    );

    for url in cli.url {
        app.add_task(url, PathBuf::from(&cli.dir));
    }

    if let Err(e) = app.run(event_rx).await {
        eprintln!("TUI error: {}", e);
    }

    drop(engine_tx);
    Ok(())
}

async fn run_headless(cli: Cli) -> Result<()> {
    let schedule_mode = ScheduleMode::parse(&cli.schedule_mode)?;
    let http_mode = HttpMode::parse(&cli.http_mode)?;
    let (connections, min_connections, max_connections) = resolve_connection_settings(&cli)?;
    let tasks_limit = cli.tasks;
    let max_total_connections = cli.max_total_connections.max(1);
    let global_bandwidth_limit_bps = mbps_to_bps(cli.bandwidth_limit);
    let per_download_bandwidth_limit_bps = mbps_to_bps(cli.per_download_limit);

    let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(100);
    let (event_tx, mut event_rx) = mpsc::channel::<EngineEvent>(100);

    let engine_cmd_tx = engine_tx.clone();
    let storage_config = StorageConfig {
        use_pwrite: !cli.no_pwrite,
        use_splice: !cli.no_splice,
        no_io_uring: cli.no_io_uring,
        no_direct_io: cli.no_direct_io,
    };
    let engine = DownloadEngine::new(
        connections,
        tasks_limit,
        max_total_connections,
        global_bandwidth_limit_bps,
        !cli.no_origin_memory,
        storage_config,
    );
    tokio::task::spawn_local(async move {
        if let Err(e) = engine.run(engine_rx, engine_cmd_tx, event_tx).await {
            eprintln!("Engine error: {}", e);
        }
    });

    let mut request_context = build_request_context(&cli);
    import_cookie_file(&mut request_context, &cli.cookie_file, &cli.url).await;

    let dir = PathBuf::from(&cli.dir);
    let log_root = cli.log_root.clone().map(PathBuf::from);
    let tasks: Vec<DownloadTask> = cli
        .url
        .into_iter()
        .map(|url| DownloadTask {
            id: Uuid::new_v4(),
            filename: url.split('/').last().unwrap_or("unknown").to_string(),
            url,
            dir: dir.clone(),
            total_size: 0,
            downloaded_size: 0,
            connections,
            status: DownloadStatus::Queued,
            speed: 0.0,
            dry_run: cli.dry_run,
            dry_run_size_mb: cli.dry_run_size_mb,
            borrow_limit_mb: cli.borrow_limit_mb,
            min_connections,
            max_connections,
            per_download_bandwidth_limit_bps,
            schedule_mode,
            http_mode,
            log_root: log_root.clone(),
            request_context: request_context.clone(),
        })
        .collect();

    let task_ids: std::collections::HashSet<Uuid> = tasks.iter().map(|task| task.id).collect();

    for task in tasks {
        let _ = engine_tx.send(EngineCommand::Add(task)).await;
    }

    let mut finished = std::collections::HashSet::new();
    let mut saw_error = false;
    while let Some(event) = event_rx.recv().await {
        match event {
            EngineEvent::Progress(id, downloaded, speed) => {
                println!("progress task={} downloaded={} speed_bps={:.0}", id, downloaded, speed);
            }
            EngineEvent::TotalSize(id, total) => {
                println!("size task={} total_bytes={}", id, total);
            }
            EngineEvent::StatusChanged(id, status) => {
                println!("status task={} {:?}", id, status);
                if matches!(status, DownloadStatus::Error(_)) {
                    saw_error = true;
                }
                if matches!(
                    status,
                    DownloadStatus::Completed
                        | DownloadStatus::Stopped
                        | DownloadStatus::Paused
                        | DownloadStatus::Error(_)
                ) {
                    finished.insert(id);
                    if finished.len() == task_ids.len() {
                        break;
                    }
                }
            }
        }
    }

    drop(engine_tx);
    if saw_error {
        return Err(anyhow::anyhow!("one or more headless downloads failed"));
    }
    Ok(())
}

fn mbps_to_bps(mbps: u64) -> u64 {
    mbps.saturating_mul(1_000_000) / 8
}

fn resolve_connection_settings(cli: &tur_rs::cli::Cli) -> Result<(usize, usize, usize)> {
    let default_initial = 8usize;
    let default_min = 1usize;
    let default_max = 16usize;

    match (cli.connections, cli.min_connections, cli.max_connections) {
        (Some(connections), None, None) => {
            let n = connections.max(1);
            Ok((n, n, n))
        }
        (connections, min_connections, max_connections) => {
            let initial = connections.unwrap_or(default_initial).max(1);
            let min_c = min_connections.unwrap_or(default_min).max(1);
            let max_c = max_connections.unwrap_or(default_max).max(1);
            if min_c > max_c {
                return Err(anyhow::anyhow!(
                    "invalid connection settings: min_connections {} exceeds max_connections {}",
                    min_c,
                    max_c
                ));
            }
            Ok((initial.clamp(min_c, max_c), min_c, max_c))
        }
    }
}
