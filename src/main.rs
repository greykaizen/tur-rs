pub mod engine;
pub mod tui;
pub mod cli;

use clap::Parser;
use cli::Cli;
use anyhow::Result;
use tokio::sync::mpsc;
use engine::{DownloadEngine, EngineCommand, EngineEvent};
use tui::TuiApp;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    let connections = cli.connections;
    let tasks_limit = cli.tasks;
    
    let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(100);
    let (tui_tx, tui_rx) = mpsc::channel::<EngineEvent>(100);
    
    let engine = DownloadEngine::new(connections, tasks_limit);
    let mut app = TuiApp::new(engine_tx);

    // Add initial URLs from CLI
    for url in cli.url {
        app.add_task(url, PathBuf::from(&cli.dir));
    }

    tokio::select! {
        res = engine.run(engine_rx, tui_tx) => {
            if let Err(e) = res {
                eprintln!("Engine error: {}", e);
            }
        }
        res = app.run(tui_rx) => {
            if let Err(e) = res {
                eprintln!("TUI error: {}", e);
            }
        }
    }

    Ok(())
}
