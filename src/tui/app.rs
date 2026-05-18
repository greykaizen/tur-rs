use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    widgets::ListState,
    Terminal,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::engine::{
    DownloadTask, DownloadStatus, EngineEvent, EngineCommand, HttpMode, ScheduleMode,
};

use super::input::InputMode;

pub struct TuiApp {
    pub(super) tasks: Vec<DownloadTask>,
    pub(super) list_state: ListState,
    pub(super) input_mode: InputMode,
    pub(super) url_buffer: String,
    pub(super) dir_buffer: String,
    engine_tx: mpsc::Sender<EngineCommand>,
    default_connections: usize,
    min_connections: usize,
    max_connections: usize,
    per_download_bandwidth_limit_bps: u64,
    dry_run: bool,
    dry_run_size_mb: Option<u64>,
    borrow_limit_mb: u64,
    schedule_mode: ScheduleMode,
    http_mode: HttpMode,
    log_root: Option<PathBuf>,
}

impl TuiApp {
    pub fn new(
        engine_tx: mpsc::Sender<EngineCommand>,
        default_connections: usize,
        min_connections: usize,
        max_connections: usize,
        per_download_bandwidth_limit_bps: u64,
        dry_run: bool,
        dry_run_size_mb: Option<u64>,
        borrow_limit_mb: u64,
        schedule_mode: ScheduleMode,
        http_mode: HttpMode,
        log_root: Option<PathBuf>,
    ) -> Self {
        Self {
            tasks: Vec::new(),
            list_state: ListState::default(),
            input_mode: InputMode::Normal,
            url_buffer: String::new(),
            dir_buffer: String::new(),
            engine_tx,
            default_connections,
            min_connections,
            max_connections,
            per_download_bandwidth_limit_bps,
            dry_run,
            dry_run_size_mb,
            borrow_limit_mb,
            schedule_mode,
            http_mode,
            log_root,
        }
    }

    pub fn add_task(&mut self, url: String, dir: PathBuf) {
        let filename = url.split('/').last().unwrap_or("unknown").to_string();
        let task = DownloadTask {
            id: Uuid::new_v4(),
            url,
            filename,
            dir,
            total_size: 0,
            downloaded_size: 0,
            connections: self.default_connections,
            status: DownloadStatus::Queued,
            speed: 0.0,
            dry_run: self.dry_run,
            dry_run_size_mb: self.dry_run_size_mb,
            borrow_limit_mb: self.borrow_limit_mb,
            min_connections: self.min_connections,
            max_connections: self.max_connections,
            per_download_bandwidth_limit_bps: self.per_download_bandwidth_limit_bps,
            schedule_mode: self.schedule_mode,
            http_mode: self.http_mode,
            log_root: self.log_root.clone(),
        };
        self.tasks.push(task.clone());
        let _ = self.engine_tx.try_send(EngineCommand::Add(task));
        if self.tasks.len() == 1 {
            self.list_state.select(Some(0));
        }
    }

    pub async fn run(
        &mut self,
        mut rx: mpsc::Receiver<EngineEvent>,
    ) -> Result<()> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let tick_rate = Duration::from_millis(100);
        let mut last_tick = Instant::now();

        loop {
            terminal.draw(|f| self.draw(f))?;

            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));

            if event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }
                    match self.input_mode {
                        InputMode::Normal => match key.code {
                            KeyCode::Char('q') => break,
                            KeyCode::Up => self.prev(),
                            KeyCode::Down => self.next(),
                            KeyCode::Char('n') | KeyCode::Char('N') => {
                                self.input_mode = InputMode::UrlInput;
                                self.url_buffer.clear();
                            }
                            KeyCode::Char('s') | KeyCode::Char('S') => {
                                self.send_command(EngineCommand::Stop)
                            }
                            KeyCode::Char('r') | KeyCode::Char('R') => {
                                self.send_command(EngineCommand::Resume)
                            }
                            KeyCode::Char('c') | KeyCode::Char('C') => {
                                self.send_command(EngineCommand::Cancel)
                            }
                            _ => {}
                        },
                        InputMode::UrlInput => match key.code {
                            KeyCode::Enter => {
                                self.input_mode = InputMode::DirInput;
                                self.dir_buffer.clear();
                            }
                            KeyCode::Esc => self.input_mode = InputMode::Normal,
                            KeyCode::Char(c) => self.url_buffer.push(c),
                            KeyCode::Backspace => {
                                self.url_buffer.pop();
                            }
                            _ => {}
                        },
                        InputMode::DirInput => match key.code {
                            KeyCode::Enter => {
                                let url = self.url_buffer.clone();
                                let dir = if self.dir_buffer.is_empty() {
                                    PathBuf::from(".")
                                } else {
                                    PathBuf::from(&self.dir_buffer)
                                };
                                self.add_task(url, dir);
                                self.input_mode = InputMode::Normal;
                            }
                            KeyCode::Esc => self.input_mode = InputMode::Normal,
                            KeyCode::Char(c) => self.dir_buffer.push(c),
                            KeyCode::Backspace => {
                                self.dir_buffer.pop();
                            }
                            _ => {}
                        },
                    }
                }
            }

            while let Ok(event) = rx.try_recv() {
                self.handle_engine_event(event);
            }

            if last_tick.elapsed() >= tick_rate {
                last_tick = Instant::now();
            }
        }

        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        Ok(())
    }

    fn handle_engine_event(&mut self, event: EngineEvent) {
        match event {
            EngineEvent::Progress(id, downloaded, speed) => {
                if let Some(task) = self.tasks.iter_mut().find(|t| t.id == id) {
                    task.downloaded_size = downloaded;
                    task.speed = speed;
                }
            }
            EngineEvent::StatusChanged(id, status) => {
                if let Some(task) = self.tasks.iter_mut().find(|t| t.id == id) {
                    task.status = status;
                }
            }
            EngineEvent::TotalSize(id, size) => {
                if let Some(task) = self.tasks.iter_mut().find(|t| t.id == id) {
                    task.total_size = size;
                }
            }
        }
    }

    fn send_command(&self, cmd_type: fn(Uuid) -> EngineCommand) {
        if let Some(i) = self.list_state.selected() {
            let task = &self.tasks[i];
            let _ = self.engine_tx.try_send(cmd_type(task.id));
        }
    }

    fn next(&mut self) {
        let i = match self.list_state.selected() {
            Some(i) => {
                if i >= self.tasks.len().saturating_sub(1) {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        if !self.tasks.is_empty() {
            self.list_state.select(Some(i));
        }
    }

    fn prev(&mut self) {
        let i = match self.list_state.selected() {
            Some(i) => {
                if i == 0 {
                    self.tasks.len().saturating_sub(1)
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        if !self.tasks.is_empty() {
            self.list_state.select(Some(i));
        }
    }
}
