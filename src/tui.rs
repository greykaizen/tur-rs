use std::io;
use std::time::{Duration, Instant};
use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, Gauge, Paragraph, List, ListItem, ListState},
    Terminal,
    style::{Color, Modifier, Style},
};
use tokio::sync::mpsc;
use crate::engine::{DownloadTask, DownloadStatus, EngineEvent, EngineCommand};
use uuid::Uuid;
use std::path::PathBuf;

#[derive(PartialEq)]
enum InputMode {
    Normal,
    UrlInput,
    DirInput,
}

pub struct TuiApp {
    tasks: Vec<DownloadTask>,
    list_state: ListState,
    input_mode: InputMode,
    url_buffer: String,
    dir_buffer: String,
    engine_tx: mpsc::Sender<EngineCommand>,
    default_connections: usize,
    dry_run: bool,
    dry_run_size_mb: Option<u64>,
    borrow_limit_mb: u64,
    log_root: Option<PathBuf>,
}

impl TuiApp {
    pub fn new(
        engine_tx: mpsc::Sender<EngineCommand>,
        default_connections: usize,
        dry_run: bool,
        dry_run_size_mb: Option<u64>,
        borrow_limit_mb: u64,
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
            dry_run,
            dry_run_size_mb,
            borrow_limit_mb,
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
                            KeyCode::Char('s') | KeyCode::Char('S') => self.send_command(EngineCommand::Stop),
                            KeyCode::Char('r') | KeyCode::Char('R') => self.send_command(EngineCommand::Resume),
                            KeyCode::Char('c') | KeyCode::Char('C') => self.send_command(EngineCommand::Cancel),

                            _ => {}
                        },
                        InputMode::UrlInput => match key.code {
                            KeyCode::Enter => {
                                self.input_mode = InputMode::DirInput;
                                self.dir_buffer.clear();
                            }
                            KeyCode::Esc => self.input_mode = InputMode::Normal,
                            KeyCode::Char(c) => self.url_buffer.push(c),
                            KeyCode::Backspace => { self.url_buffer.pop(); }
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
                            KeyCode::Backspace => { self.dir_buffer.pop(); }
                            _ => {}
                        }
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

    fn draw(&self, f: &mut ratatui::Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(3),
                Constraint::Length(3),
            ])
            .split(f.area());

        // Title
        let title = Paragraph::new(" Tur Download Manager ")
            .block(Block::default().borders(Borders::ALL))
            .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
        f.render_widget(title, chunks[0]);

        // Tasks List
        let items: Vec<ListItem> = self.tasks.iter().map(|t| {
            let name = if t.filename.len() > 20 {
                format!("{}...", &t.filename[..17])
            } else {
                t.filename.clone()
            };
            let status = format!("{:?}", t.status);
            let mode = if t.dry_run { "dry" } else { "live" };
            ListItem::new(format!(
                "{:<20} | {:<12} | {:<4} | {:.2} MB/s",
                name,
                status,
                mode,
                t.speed / 1_000_000.0
            ))
        }).collect();

        let tasks_list = List::new(items)
            .block(Block::default().title("Downloads").borders(Borders::ALL))
            .highlight_style(Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD))
            .highlight_symbol(">> ");
        f.render_stateful_widget(tasks_list, chunks[1], &mut self.list_state.clone());

        // Progress of selected
        if let Some(i) = self.list_state.selected() {
            let task = &self.tasks[i];
            let percent = if task.total_size > 0 {
                (task.downloaded_size as f64 / task.total_size as f64 * 100.0) as u16
            } else {
                0
            };
            let gauge = Gauge::default()
                .block(Block::default().title("Selected Task Progress").borders(Borders::ALL))
                .gauge_style(Style::default().fg(Color::Cyan))
                .percent(percent);
            f.render_widget(gauge, chunks[2]);
        }

        // Input or Commands
        let help_text = match self.input_mode {
            InputMode::Normal => "[q]uit [n]ew [s]pause [r]resume [c]persist-stop ↑↓ move",
            InputMode::UrlInput => &format!("Enter URL: {}_", self.url_buffer),
            InputMode::DirInput => &format!("Enter Dir (empty for current): {}_", self.dir_buffer),
        };
        let help = Paragraph::new(help_text)
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(help, chunks[3]);
    }
}
