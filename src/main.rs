pub mod cli;
pub mod engine;
pub mod tui;

use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;
use uuid::Uuid;

use cli::Cli;
use engine::{DownloadEngine, DownloadStatus, DownloadTask, EngineCommand, EngineEvent, HttpMode, ScheduleMode};
use tui::TuiApp;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.headless {
        return run_headless(cli).await;
    }

    let schedule_mode = ScheduleMode::parse(&cli.schedule_mode)?;
    let http_mode = HttpMode::parse(&cli.http_mode)?;

    let connections = cli.connections;
    let tasks_limit = cli.tasks;

    let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(100);
    let (event_tx, event_rx) = mpsc::channel::<EngineEvent>(100);

    let engine = DownloadEngine::new(connections, tasks_limit);
    let engine_cmd_tx = engine_tx.clone();
    let engine_handle = tokio::spawn(async move {
        if let Err(e) = engine.run(engine_rx, engine_cmd_tx, event_tx).await {
            eprintln!("Engine error: {}", e);
        }
    });

    let mut app = TuiApp::new(
        engine_tx.clone(),
        connections,
        cli.dry_run,
        cli.dry_run_size_mb,
        cli.borrow_limit_mb,
        schedule_mode,
        http_mode,
        cli.log_root.clone().map(PathBuf::from),
    );

    for url in cli.url {
        app.add_task(url, PathBuf::from(&cli.dir));
    }

    if let Err(e) = app.run(event_rx).await {
        eprintln!("TUI error: {}", e);
    }

    engine_handle.abort();
    Ok(())
}

async fn run_headless(cli: Cli) -> Result<()> {
    let schedule_mode = ScheduleMode::parse(&cli.schedule_mode)?;
    let http_mode = HttpMode::parse(&cli.http_mode)?;
    let connections = cli.connections;
    let tasks_limit = cli.tasks;

    let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(100);
    let (event_tx, mut event_rx) = mpsc::channel::<EngineEvent>(100);

    let engine = DownloadEngine::new(connections, tasks_limit);
    let engine_cmd_tx = engine_tx.clone();
    let engine_handle = tokio::spawn(async move {
        if let Err(e) = engine.run(engine_rx, engine_cmd_tx, event_tx).await {
            eprintln!("Engine error: {}", e);
        }
    });

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
            schedule_mode,
            http_mode,
            log_root: log_root.clone(),
        })
        .collect();

    let task_ids: HashSet<Uuid> = tasks.iter().map(|task| task.id).collect();

    for task in tasks {
        let _ = engine_tx.send(EngineCommand::Add(task)).await;
    }

    let mut finished = HashSet::new();
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

    engine_handle.abort();
    if saw_error {
        return Err(anyhow::anyhow!("one or more headless downloads failed"));
    }
    Ok(())
}
