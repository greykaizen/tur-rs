use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Layout};
use ratatui::prelude::{Alignment, Frame, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, List, ListItem, Paragraph, Wrap};
use std::time::Duration;

use crate::types::{AppState, DownloadStatus, Panel, StreamFormat, ProbeState};

#[derive(PartialEq)]
pub enum Action {
    None,
    Quit,
    Enter,
    Copy,
    ToggleNoise,
    TestDownload,
}

pub fn render(frame: &mut Frame, state: &AppState) {
    let area = frame.area();
    let [header_area, body_area] =
        Layout::vertical([Constraint::Length(2), Constraint::Fill(1)]).areas(area);
    render_header(frame, header_area);
    let [top_area, bottom_area] =
        Layout::vertical([Constraint::Fill(2), Constraint::Fill(1)]).areas(body_area);
    let [left_area, right_area] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).areas(top_area);
    render_stream_list(frame, left_area, state);
    render_metadata(frame, right_area, state);
    render_downloads(frame, bottom_area, state);
}

fn render_header(frame: &mut Frame, area: Rect) {
    let text = Paragraph::new("SPECTUR — Media Stream Sniffer & Downloader")
        .block(Block::bordered())
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .alignment(Alignment::Center);
    frame.render_widget(text, area);
}

fn render_stream_list(frame: &mut Frame, area: Rect, state: &AppState) {
    let mut items: Vec<ListItem> = Vec::new();
    for (tab_idx, tab) in state.tabs.iter().enumerate() {
        let tab_label = if tab.page_title.is_empty() { &tab.page_url } else { &tab.page_title };
        let fs_len = tab.filtered_streams().len();
        let total_len = tab.streams.len();
        let streams_count_label = if fs_len == total_len {
            format!("{} streams", total_len)
        } else {
            format!("{}/{} streams", fs_len, total_len)
        };
        let noise_label = if tab.show_noise { " [noise ON: n]" } else { " [n=filter]" };
        let tab_line = if tab_idx == state.selected_tab_index {
            Line::from(vec![Span::styled(
                format!("▶ Tab {}: {} ({}){}", tab_idx + 1, tab_label, streams_count_label, noise_label),
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )])
        } else {
            Line::from(format!("  Tab {}: {} ({}){}", tab_idx + 1, tab_label, streams_count_label, noise_label))
        };
        items.push(ListItem::new(tab_line));
        if tab_idx == state.selected_tab_index {
            let fs = tab.filtered_streams();
            for (stream_idx, stream) in fs.iter().enumerate() {
                let stream_prefix = if stream_idx == state.selected_stream_index { " > " } else { "   " };
                let format_str = match stream.format {
                    StreamFormat::Hls => "HLS", StreamFormat::Dash => "DASH",
                    StreamFormat::Mp4 => "MP4", StreamFormat::Ts => "TS",
                    StreamFormat::Youtube => "YT", StreamFormat::Unknown => "?",
                };
                let status = match &stream.probe_state {
                    ProbeState::Done(_) => "✓", ProbeState::Probing => "…", ProbeState::Failed(_) => "✗",
                };
                let url_display = if stream.url.len() > 50 { format!("{}…", &stream.url[..47]) } else { stream.url.clone() };
                let stream_line = format!("{} {} [{}] {}", stream_prefix, status, format_str, url_display);
                if stream_idx == state.selected_stream_index && state.focused_panel == Panel::Streams {
                    items.push(ListItem::new(Line::from(vec![
                        Span::styled(stream_line, Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                    ])));
                } else {
                    items.push(ListItem::new(stream_line));
                }
            }
        }
    }
    if items.is_empty() {
        items.push(ListItem::new(Span::styled(
            "Waiting for media streams… (browse to a page with video)",
            Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
        )));
    }
    let list = List::new(items).block(
        Block::bordered().title(" Streams ").border_style(
            if state.focused_panel == Panel::Streams { Style::default().fg(Color::Green) } else { Style::default() }
        ),
    );
    frame.render_widget(list, area);
}

fn render_metadata(frame: &mut Frame, area: Rect, state: &AppState) {
    let mut lines = Vec::new();

    let current_yt_formats: &[crate::types::YtFormat] = if state.tabs.is_empty() {
        &[]
    } else {
        &state.tabs[state.selected_tab_index].yt_formats
    };

    if !current_yt_formats.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("YouTube Formats Detected", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        ]));
        lines.push(Line::from(""));
        let video_formats: Vec<(usize, &crate::types::YtFormat)> = current_yt_formats.iter().enumerate().filter(|(_, f)| f.is_video()).collect();
        let audio_formats: Vec<(usize, &crate::types::YtFormat)> = current_yt_formats.iter().enumerate().filter(|(_, f)| f.is_audio_only()).collect();

        if !video_formats.is_empty() {
            lines.push(Line::from(vec![Span::styled("Video (Tab→Up/Down to select, Enter to download):", Style::default().fg(Color::Cyan))]));
            for &(idx, f) in &video_formats {
                let is_selected = state.focused_panel == Panel::Metadata && idx == state.selected_yt_format_index;
                let prefix = if is_selected { " > " } else { "   " };
                let style = if is_selected {
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
                } else { Style::default() };
                let mut detail = f.resolution_label();
                let mime_short = f.mime_type.split(';').next().unwrap_or("");
                if !mime_short.is_empty() { detail.push_str(&format!(" [{}]", mime_short)); }
                if let Some(b) = f.bitrate { detail.push_str(&format!(" {}kbps", b / 1000)); }
                if let Some(fps) = f.fps { detail.push_str(&format!(" {}fps", fps)); }
                lines.push(Line::from(vec![Span::styled(format!("{}{} (itag {})", prefix, detail, f.itag), style)]));
            }
            lines.push(Line::from(""));
        }
        if !audio_formats.is_empty() {
            lines.push(Line::from(vec![Span::styled("Audio:", Style::default().fg(Color::Cyan))]));
            for &(idx, f) in &audio_formats {
                let is_selected = state.focused_panel == Panel::Metadata && idx == state.selected_yt_format_index;
                let prefix = if is_selected { " > " } else { "   " };
                let style = if is_selected {
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
                } else { Style::default() };
                let detail = format!("{} (itag {})", f.resolution_label(), f.itag);
                lines.push(Line::from(vec![Span::styled(format!("{}{}", prefix, detail), style)]));
            }
        }
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("[ Enter to download selected YT format, t to test ]", Style::default().fg(Color::Green)),
        ]));
    }

    if let Some(stream) = state.selected_stream() {
        match &stream.probe_state {
            ProbeState::Done(meta) => {
                if lines.is_empty() {
                    lines.push(Line::from(vec![
                        Span::raw("Format: "),
                        Span::styled(match stream.format {
                        StreamFormat::Hls => "HLS Manifest", StreamFormat::Dash => "DASH MPD",
                        StreamFormat::Mp4 => "MP4 Progressive", StreamFormat::Ts => "TS Segment",
                        StreamFormat::Youtube => "YouTube Video", StreamFormat::Unknown => "Unknown",
                    }, Style::default().fg(Color::Yellow)),
                    ]));
                    lines.push(Line::from(""));
                }
                if meta.duration_seconds > 0.0 {
                    let mins = (meta.duration_seconds / 60.0) as u32;
                    let secs = (meta.duration_seconds % 60.0) as u32;
                    lines.push(Line::from(format!("Duration: {:02}:{:02}", mins, secs)));
                }
                if let Some(bytes) = meta.size_bytes {
                    let size_str = if bytes >= 1024 * 1024 * 1024 {
                        format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
                    } else if bytes >= 1024 * 1024 {
                        format!("{:.2} MB", bytes as f64 / 1024.0 / 1024.0)
                    } else {
                        format!("{:.2} KB", bytes as f64 / 1024.0)
                    };
                    lines.push(Line::from(format!("File Size: {}", size_str)));
                }
                if meta.total_segments > 1 {
                    lines.push(Line::from(format!("Total Segments: {}", meta.total_segments)));
                }
                if !meta.resolutions.is_empty() {
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled("Resolutions:", Style::default().fg(Color::Cyan))]));
                    for (i, res) in meta.resolutions.iter().enumerate() {
                        let mut detail = res.label.clone();
                        if res.bandwidth > 0 { detail.push_str(&format!(" ({} kbps)", res.bandwidth / 1000)); }
                        if let Some(ref codecs) = res.codecs { detail.push_str(&format!(" [{}]", codecs)); }
                        if let Some(ref fr) = res.frame_rate { detail.push_str(&format!(" {}fps", fr)); }
                        let prefix = if state.focused_panel == Panel::Metadata && i == state.selected_resolution_index { " > " } else { "   " };
                        let style = if state.focused_panel == Panel::Metadata && i == state.selected_resolution_index {
                            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
                        } else { Style::default() };
                        lines.push(Line::from(vec![Span::styled(format!("{}{}", prefix, detail), style)]));
                    }
                }
                if !meta.audio_tracks.is_empty() {
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled("Audio Tracks:", Style::default().fg(Color::Cyan))]));
                    for (i, track) in meta.audio_tracks.iter().enumerate() {
                        lines.push(Line::from(format!("  [Audio {:02}] {}", i + 1, track)));
                    }
                }
                if !meta.keys.is_empty() {
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled("Encryption Keys:", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))]));
                    for key in &meta.keys {
                        lines.push(Line::from(format!("  Method: {}", key.method)));
                        if let Some(ref uri) = key.uri { lines.push(Line::from(format!("  Key URI: {}", uri))); }
                        if let Some(ref iv) = key.iv { lines.push(Line::from(format!("  IV: {}", iv))); }
                        if let Some(ref kf) = key.keyformat { lines.push(Line::from(format!("  Key Format: {}", kf))); }
                        if let Some(ref hex) = key.key_hex { lines.push(Line::from(format!("  Key: {}", hex))); }
                        lines.push(Line::from(""));
                    }
                }
                if !meta.drm.is_empty() {
                    lines.push(Line::from(vec![Span::styled("DRM:", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))]));
                    for drm in &meta.drm {
                        lines.push(Line::from(format!("  {} ({})", drm.system, drm.scheme_id_uri)));
                        if let Some(ref kid) = drm.default_kid { lines.push(Line::from(format!("  KID: {}", kid))); }
                        if let Some(ref url) = drm.license_url { lines.push(Line::from(format!("  License: {}", url))); }
                    }
                }
                lines.push(Line::from(""));
                lines.push(Line::from(vec![Span::styled("[ Enter to download, t to test ]", Style::default().fg(Color::Green))]));
            }
            ProbeState::Probing => {
                if lines.is_empty() {
                    lines.push(Line::from(vec![Span::raw("URL: "), Span::styled(&stream.url, Style::default().fg(Color::Blue))]));
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled("Probing manifest…", Style::default().fg(Color::Yellow).add_modifier(Modifier::ITALIC))]));
                }
            }
            ProbeState::Failed(err) => {
                if lines.is_empty() {
                    lines.push(Line::from(vec![Span::raw("URL: "), Span::styled(&stream.url, Style::default().fg(Color::Blue))]));
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled("Probe failed:", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))]));
                    lines.push(Line::from(vec![Span::styled(err, Style::default().fg(Color::Red))]));
                }
            }
        }
    } else if lines.is_empty() {
        lines.push(Line::from(vec![Span::styled("No stream selected", Style::default().fg(Color::DarkGray))]));
    }

    let paragraph = Paragraph::new(Text::from(lines))
        .block(Block::bordered().title(" Metadata ").border_style(
            if state.focused_panel == Panel::Metadata { Style::default().fg(Color::Green) } else { Style::default() }
        ))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn render_downloads(frame: &mut Frame, area: Rect, state: &AppState) {
    let [progress_area, log_area] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).areas(area);
    render_progress(frame, progress_area, state);
    render_download_logs(frame, log_area, state);
}

fn render_progress(frame: &mut Frame, area: Rect, state: &AppState) {
    let mut lines: Vec<Line> = Vec::new();
    if state.downloads.is_empty() {
        lines.push(Line::from(vec![Span::styled("No active downloads", Style::default().fg(Color::DarkGray))]));
    } else {
        for (i, task) in state.downloads.iter().enumerate() {
            let (status_str, status_color) = match &task.status {
                DownloadStatus::Queued => ("QUEUED", Color::Yellow),
                DownloadStatus::Running => ("RUNNING", Color::Cyan),
                DownloadStatus::Finished => ("DONE", Color::Green),
                DownloadStatus::Failed(_) => ("FAILED", Color::Red),
            };
            let prefix = if state.focused_panel == Panel::Downloads && i == state.selected_download_index { " > " } else { "   " };
            let style = if state.focused_panel == Panel::Downloads && i == state.selected_download_index {
                Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
            } else { Style::default() };
            let file_name = std::path::Path::new(&task.output_path)
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("unknown");
            lines.push(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(format!("[{}]", status_str), Style::default().fg(status_color).add_modifier(Modifier::BOLD)),
                Span::raw(format!(" #{}: {} ({}%) {:.1}MB/s", task.id + 1, file_name, task.progress, task.speed_mbps)),
            ]));
        }
    }
    let paragraph = Paragraph::new(Text::from(lines))
        .block(Block::bordered().title(" Progress ").border_style(
            if state.focused_panel == Panel::Downloads { Style::default().fg(Color::Green) } else { Style::default() }
        ))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn render_download_logs(frame: &mut Frame, area: Rect, state: &AppState) {
    let mut log_lines = Vec::new();
    if let Some(task) = state.downloads.get(state.selected_download_index) {
        log_lines.push(Line::from(vec![
            Span::styled(format!("Download #{} logs (last 50 lines):", task.id + 1), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        ]));
        log_lines.push(Line::from(""));
        
        let start = if task.log_lines.len() > 50 { task.log_lines.len() - 50 } else { 0 };
        for l in &task.log_lines[start..] {
            if l.contains("error") || l.contains("Error") || l.contains("FAILED") || l.contains("failed") {
                log_lines.push(Line::from(vec![Span::styled(l, Style::default().fg(Color::Red))]));
            } else if l.contains("complete") || l.contains("Done") || l.contains("Finished") {
                log_lines.push(Line::from(vec![Span::styled(l, Style::default().fg(Color::Green))]));
            } else {
                log_lines.push(Line::from(vec![Span::raw(l)]));
            }
        }
    } else {
        // Fallback to displaying general TUI logs if no download is selected
        log_lines.push(Line::from(vec![
            Span::styled("System Logs:", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        ]));
        log_lines.push(Line::from(""));
        for l in state.tui_logs.iter().rev().take(12) {
            if l.contains("error") || l.contains("Error") || l.contains("FAILED") {
                log_lines.push(Line::from(vec![Span::styled(l, Style::default().fg(Color::Red))]));
            } else if l.contains("complete") || l.contains("Done") {
                log_lines.push(Line::from(vec![Span::styled(l, Style::default().fg(Color::Green))]));
            } else {
                log_lines.push(Line::from(vec![Span::raw(l)]));
            }
        }
    }
    
    let list = List::new(log_lines).block(Block::bordered().title(" Logs "));
    frame.render_widget(list, area);
}

pub fn handle_events(state: &mut AppState) -> std::io::Result<Action> {
    if !event::poll(Duration::from_millis(50))? { return Ok(Action::None); }
    match event::read()? {
        Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(Action::Quit),
            KeyCode::Tab => {
                state.focused_panel = match state.focused_panel {
                    Panel::Streams => Panel::Metadata,
                    Panel::Metadata => Panel::Downloads,
                    Panel::Downloads => Panel::Streams,
                };
            }
            KeyCode::Up => {
                if state.focused_panel == Panel::Streams { state.prev_stream(); }
                else if state.focused_panel == Panel::Metadata {
                    let current_yt_formats: &[crate::types::YtFormat] = if state.tabs.is_empty() {
                        &[]
                    } else {
                        &state.tabs[state.selected_tab_index].yt_formats
                    };
                    if !current_yt_formats.is_empty() {
                        if state.selected_yt_format_index > 0 {
                            state.selected_yt_format_index -= 1;
                        }
                    } else {
                        state.prev_resolution();
                    }
                }
                else if state.focused_panel == Panel::Downloads {
                    if state.selected_download_index > 0 {
                        state.selected_download_index -= 1;
                    }
                }
            }
            KeyCode::Down => {
                if state.focused_panel == Panel::Streams { state.next_stream(); }
                else if state.focused_panel == Panel::Metadata {
                    let current_yt_formats: &[crate::types::YtFormat] = if state.tabs.is_empty() {
                        &[]
                    } else {
                        &state.tabs[state.selected_tab_index].yt_formats
                    };
                    if !current_yt_formats.is_empty() {
                        if state.selected_yt_format_index + 1 < current_yt_formats.len() {
                            state.selected_yt_format_index += 1;
                        }
                    } else {
                        state.next_resolution();
                    }
                }
                else if state.focused_panel == Panel::Downloads {
                    if state.selected_download_index + 1 < state.downloads.len() {
                        state.selected_download_index += 1;
                    }
                }
            }
            KeyCode::Left => { if state.focused_panel == Panel::Streams { state.prev_tab(); } }
            KeyCode::Right => { if state.focused_panel == Panel::Streams { state.next_tab(); } }
            KeyCode::Enter => return Ok(Action::Enter),
            KeyCode::Char('t') | KeyCode::Char('T') => return Ok(Action::TestDownload),
            KeyCode::Char('c') | KeyCode::Char('C') => return Ok(Action::Copy),
            KeyCode::Char('n') | KeyCode::Char('N') => return Ok(Action::ToggleNoise),
            _ => {}
        },
        _ => {}
    }
    Ok(Action::None)
}
