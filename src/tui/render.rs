use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
};

use super::app::TuiApp;
use super::input::InputMode;

impl TuiApp {
    pub(super) fn draw(&self, f: &mut ratatui::Frame) {
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
                ListItem::new(format!(
                    "{:<20} | {:<12} | {:<4} | {:.2} MB/s",
                    name,
                    status,
                    mode,
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

        // Input or Commands
        let help_text = match self.input_mode {
            InputMode::Normal => {
                "[q]uit [n]ew [s]pause [r]resume [c]persist-stop ↑↓ move"
            }
            InputMode::UrlInput => &format!("Enter URL: {}_", self.url_buffer),
            InputMode::DirInput => {
                &format!("Enter Dir (empty for current): {}_", self.dir_buffer)
            }
        };
        let help = Paragraph::new(help_text).block(Block::default().borders(Borders::ALL));
        f.render_widget(help, chunks[3]);
    }
}
