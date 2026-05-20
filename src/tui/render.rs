use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
};

use super::app::TuiApp;
use super::input::{FocusPane, InputMode};
use crate::engine::WorkerState;

impl TuiApp {
    pub(super) fn draw(&self, f: &mut ratatui::Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(3),
                Constraint::Length(if self.show_details { 10 } else { 3 }),
                Constraint::Length(3),
            ])
            .split(f.area());

        // Title
        let title = Paragraph::new(" Tur Download Manager ")
            .block(Block::default().borders(Borders::ALL))
            .style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            );
        f.render_widget(title, chunks[0]);

        // Tasks List
        let items: Vec<ListItem> = self
            .tasks
            .iter()
            .map(|t| {
                let name = if t.filename.len() > 20 {
                    format!("{}...", &t.filename[..17])
                } else {
                    t.filename.clone()
                };
                let status = format!("{:?}", t.status);
                let mode = if t.dry_run { "dry" } else { "live" };
                let protocol_label = self
                    .protocol_infos
                    .get(&t.id)
                    .map(|info| info.display_label())
                    .unwrap_or_else(|| "auto".to_string());
                let worker_count = self
                    .worker_snapshots
                    .get(&t.id)
                    .map(|workers| workers.len())
                    .unwrap_or(0);
                ListItem::new(format!(
                    "{:<20} | {:<12} | {:<4} | {:<4} | {:>2} conn | {:.2} MB/s",
                    name,
                    status,
                    mode,
                    protocol_label,
                    worker_count,
                    t.speed / 1_000_000.0
                ))
            })
            .collect();

        let tasks_list = List::new(items)
            .block(Block::default().title("Downloads").borders(Borders::ALL))
            .highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
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
                .block(
                    Block::default()
                        .title("Selected Task Progress")
                        .borders(Borders::ALL),
                )
                .gauge_style(Style::default().fg(Color::Cyan))
                .percent(percent);
            f.render_widget(gauge, chunks[2]);
        }

        // Worker details / summary
        let detail_text = if self.show_details {
            if let Some(i) = self.list_state.selected() {
                let task = &self.tasks[i];
                let workers = self.worker_snapshots.get(&task.id);
                render_worker_details(workers)
            } else {
                "No task selected".to_string()
            }
        } else {
            "Worker details hidden. Press [d] to expand.".to_string()
        };
        let detail_title = match self.focus_pane {
            FocusPane::TaskList => "Connections",
            FocusPane::Details => "Connections [focused]",
        };
        let details = Paragraph::new(detail_text)
            .block(Block::default().title(detail_title).borders(Borders::ALL))
            .scroll((self.detail_scroll as u16, 0));
        f.render_widget(details, chunks[3]);

        // Input or Commands
        let help_text = match self.input_mode {
            InputMode::Normal => {
                "[q]uit [n]ew [d]etails [tab] focus [p]ause [r]esume [c]ancel/stop ↑↓ move/scroll"
            }
            InputMode::UrlInput => &format!("Enter URL: {}_", self.url_buffer),
            InputMode::DirInput => &format!("Enter Dir (empty for current): {}_", self.dir_buffer),
        };
        let help = Paragraph::new(help_text).block(Block::default().borders(Borders::ALL));
        f.render_widget(help, chunks[4]);
    }
}

fn render_worker_details(workers: Option<&Vec<crate::engine::WorkerSnapshot>>) -> String {
    let Some(workers) = workers else {
        return "No worker diagnostics yet.".to_string();
    };
    if workers.is_empty() {
        return "No active worker snapshots.".to_string();
    }

    let mut lines = Vec::with_capacity(workers.len() + 1);
    lines.push("id  state            speed       bytes      range".to_string());
    let mut workers = workers.iter().collect::<Vec<_>>();
    workers.sort_by_key(|worker| worker.connection_id);
    for worker in workers {
        let state = match worker.state {
            WorkerState::Connecting => "connecting",
            WorkerState::WaitingForWork => "waiting",
            WorkerState::Downloading => "downloading",
            WorkerState::Retrying => "retrying",
            WorkerState::Paused => "paused",
            WorkerState::Stopped => "stopped",
            WorkerState::Finished => "finished",
        };
        let speed = if worker.speed_bps > 0.0 {
            format!("{:.2} MB/s", worker.speed_bps / 1_000_000.0)
        } else {
            "0.00 MB/s".to_string()
        };
        let bytes = format!(
            "{:.1} MB",
            worker.transferred_bytes as f64 / (1024.0 * 1024.0)
        );
        let range = match (worker.range_start, worker.range_cursor, worker.range_end) {
            (Some(start), Some(cursor), Some(end)) => {
                format!(
                    "{}..{} / {}",
                    start / (1024 * 1024),
                    cursor / (1024 * 1024),
                    end / (1024 * 1024)
                )
            }
            _ => "-".to_string(),
        };
        let mut line = format!(
            "{:<3} {:<15} {:<10} {:<10} {}",
            worker.connection_id, state, speed, bytes, range
        );
        if let Some(detail) = &worker.detail {
            if !detail.is_empty() {
                line.push_str(&format!(" ({detail})"));
            }
        }
        lines.push(line);
    }
    lines.join("\n")
}
