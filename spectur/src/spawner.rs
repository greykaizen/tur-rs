use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use regex::Regex;

use crate::types::{AppState, DownloadStatus, YtFormat};

pub async fn spawn_yt_format_download(
    state: Arc<Mutex<AppState>>,
    url: String,
    format: YtFormat,
    is_test: bool,
) {
    let output_dir = "/tmp/spectur-downloads";
    let _ = std::fs::create_dir_all(output_dir);

    let task_id: usize;
    {
        let mut app = state.lock().await;
        task_id = app.downloads.len();
        app.downloads.push(crate::types::DownloadTask {
            id: task_id,
            stream_url: url.clone(),
            output_path: String::new(),
            progress: 0,
            speed_mbps: 0.0,
            log_lines: Vec::new(),
            status: DownloadStatus::Running,
        });
        app.tui_logs.push(format!("Starting YT{} download: {} (itag {})", if is_test { " test" } else { "" }, format.resolution_label(), format.itag));
    }

    let mut args = vec![
        url.to_string(),
        "-o".into(),
        if is_test {
            format!("/tmp/spectur-downloads/%(title)s_test.%(ext)s")
        } else {
            format!("/tmp/spectur-downloads/%(title)s.%(ext)s")
        },
        "-f".into(),
        format.itag.to_string(),
    ];

    if is_test {
        args.push("--download-sections".into());
        args.push("*0:00-0:10".into());
    }

    let result = spawn_and_stream("yt-dlp", &args, state.clone(), task_id).await;

    let mut app = state.lock().await;
    if let Some(task) = app.downloads.get_mut(task_id) {
        match result {
            Ok(()) => {
                task.status = DownloadStatus::Finished;
                task.progress = 100;
                app.tui_logs.push("YT download complete".into());
            }
            Err(e) => {
                task.status = DownloadStatus::Failed(e.clone());
                app.tui_logs.push(format!("YT download failed: {}", e));
            }
        }
    }
}

fn clean_filename(filename: &str, format: &crate::types::StreamFormat) -> String {
    let has_video_ext = filename.ends_with(".mp4")
        || filename.ends_with(".mkv")
        || filename.ends_with(".webm")
        || filename.ends_with(".ts")
        || filename.ends_with(".mov")
        || filename.ends_with(".avi")
        || filename.ends_with(".mp3")
        || filename.ends_with(".m4a")
        || filename.ends_with(".aac");

    if has_video_ext {
        return filename.to_string();
    }

    let mut name = filename.to_string();
    if name.ends_with(".html") || name.ends_with(".htm") || name.ends_with(".js") || name.ends_with(".css") || name.ends_with(".txt") {
        if let Some(pos) = name.rfind('.') {
            name.truncate(pos);
        }
    }

    match format {
        crate::types::StreamFormat::Hls | crate::types::StreamFormat::Dash => {
            name
        }
        crate::types::StreamFormat::Ts => {
            name.push_str(".ts");
            name
        }
        _ => {
            name.push_str(".mp4");
            name
        }
    }
}

fn get_unique_output_path(dir: &str, base: &str, ext: &str) -> String {
    let mut path = format!("{}/{}{}", dir, base, ext);
    if !std::path::Path::new(&path).exists() {
        return path;
    }
    let mut counter = 1;
    loop {
        path = format!("{}/{}({}){}", dir, base, counter, ext);
        if !std::path::Path::new(&path).exists() {
            return path;
        }
        counter += 1;
    }
}

pub async fn spawn_download(
    state: Arc<Mutex<AppState>>,
    stream_url: String,
    resolution: Option<String>,
    is_test: bool,
) {
    let output_dir = "/tmp/spectur-downloads";
    let _ = std::fs::create_dir_all(output_dir);

    let (request_headers, keys, is_yt, yt_format, format) = {
        let app = state.lock().await;
        
        let mut matching_tab = None;
        let mut matching_stream = None;
        for tab in &app.tabs {
            if let Some(s) = tab.streams.iter().find(|s| s.url == stream_url) {
                matching_tab = Some(tab.clone());
                matching_stream = Some(s.clone());
                break;
            }
        }

        let headers = matching_stream.as_ref().map(|s| s.request_headers.clone()).unwrap_or_default();
        let keys = matching_stream.as_ref().and_then(|s| match &s.probe_state {
            crate::types::ProbeState::Done(m) => Some(m.keys.clone()),
            _ => None,
        }).unwrap_or_default();
        let is_yt = stream_url.contains("youtube.com") || stream_url.contains("youtu.be");
        let yt_fmt = if is_yt && resolution.is_some() {
            let res = resolution.as_deref().unwrap_or("");
            app.tabs.iter()
                .find(|t| t.page_url == stream_url)
                .and_then(|t| t.yt_formats.iter().find(|f| &f.short_label() == res).cloned())
        } else {
            None
        };
        let format = matching_stream.as_ref().map(|s| s.format.clone()).unwrap_or(crate::types::StreamFormat::Unknown);

        let mut final_headers = headers;
        let has_ua = final_headers.keys().any(|k| k.to_lowercase() == "user-agent");
        if !has_ua {
            final_headers.insert(
                "User-Agent".to_string(),
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36".to_string(),
                );
        }
        if let Some(tab) = matching_tab {
            let has_referer = final_headers.keys().any(|k| k.to_lowercase() == "referer");
            if !has_referer && !tab.page_url.is_empty() {
                final_headers.insert("Referer".to_string(), tab.page_url.clone());
            }
            let has_origin = final_headers.keys().any(|k| k.to_lowercase() == "origin");
            if !has_origin && !tab.page_url.is_empty() {
                if let Ok(parsed) = url::Url::parse(&tab.page_url) {
                    let origin_val = format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or(""));
                    final_headers.insert("Origin".to_string(), origin_val);
                }
            }
        }

        (final_headers, keys, is_yt, yt_fmt, format)
    };

    let filename = stream_url
        .split('/')
        .last()
        .unwrap_or("download")
        .split('?')
        .next()
        .unwrap_or("download");

    let cleaned_name = clean_filename(filename, &format);
    let base_name: String;
    let ext: String;
    if let Some(pos) = cleaned_name.rfind('.') {
        base_name = cleaned_name[..pos].to_string();
        ext = cleaned_name[pos..].to_string();
    } else {
        base_name = cleaned_name;
        ext = String::new();
    }

    let final_base = if is_test {
        format!("{}_test", base_name)
    } else {
        base_name
    };

    let output_path = get_unique_output_path(output_dir, &final_base, &ext);

    let task_id: usize;
    {
        let mut app = state.lock().await;
        task_id = app.downloads.len();
        app.downloads.push(crate::types::DownloadTask {
            id: task_id,
            stream_url: stream_url.clone(),
            output_path: output_path.clone(),
            progress: 0,
            speed_mbps: 0.0,
            log_lines: Vec::new(),
            status: DownloadStatus::Running,
        });
        app.tui_logs.push(format!("Starting{} download: {}", if is_test { " test" } else { "" }, stream_url));
    }

    let result = if is_yt {
        spawn_yt_download(&stream_url, &output_path, state.clone(), task_id, yt_format, is_test).await
    } else {
        run_downloader(&stream_url, &output_path, state.clone(), task_id, resolution, request_headers, keys, is_test, format).await
    };

    let mut app = state.lock().await;
    if let Some(task) = app.downloads.get_mut(task_id) {
        let output_path = task.output_path.clone();
        match result {
            Ok(()) => {
                task.status = DownloadStatus::Finished;
                task.progress = 100;
                app.tui_logs.push(format!("Download complete: {}", output_path));
            }
            Err(e) => {
                task.status = DownloadStatus::Failed(e.clone());
                app.tui_logs.push(format!("Download failed: {}", e));
            }
        }
    }
}

async fn spawn_yt_download(
    url: &str,
    output_path: &str,
    state: Arc<Mutex<AppState>>,
    task_id: usize,
    format: Option<YtFormat>,
    is_test: bool,
) -> Result<(), String> {
    let path = std::path::Path::new(output_path);
    let parent = path.parent().and_then(|p| p.to_str()).unwrap_or("/tmp/spectur-downloads");

    let mut args = vec![
        url.to_string(),
        "-o".into(),
        format!("{}/%(title)s{}.%(ext)s", parent, if is_test { "_test" } else { "" }),
    ];

    if let Some(ref fmt) = format {
        args.push("-f".into());
        args.push(fmt.itag.to_string());
    } else {
        args.push("-f".into());
        args.push("bestvideo+bestaudio/best".into());
    }

    if is_test {
        args.push("--download-sections".into());
        args.push("*0:00-0:10".into());
    }

    spawn_and_stream("yt-dlp", &args, state, task_id).await
}

async fn run_downloader(
    url: &str,
    output_path: &str,
    state: Arc<Mutex<AppState>>,
    task_id: usize,
    resolution: Option<String>,
    headers: std::collections::HashMap<String, String>,
    keys: Vec<crate::types::KeyInfo>,
    is_test: bool,
    format: crate::types::StreamFormat,
) -> Result<(), String> {
    let tool = select_downloader(url, &format);
    let args = build_args(&tool, url, output_path, resolution, &headers, &keys, is_test);

    spawn_and_stream(tool.binary, &args, state, task_id).await
}

async fn spawn_and_stream(
    binary: &str,
    args: &[String],
    state: Arc<Mutex<AppState>>,
    task_id: usize,
) -> Result<(), String> {
    let mut child = Command::new(binary)
        .args(args)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to spawn {}: {}", binary, e))?;

    let stdout = child.stdout.take().ok_or("no stdout pipe")?;
    let stderr = child.stderr.take().ok_or("no stderr pipe")?;

    let progress_re = Regex::new(r"([0-9.]+)%").unwrap();

    let stdout_task = tokio::spawn(read_and_parse(
        BufReader::new(stdout),
        state.clone(),
        task_id,
        progress_re.clone(),
    ));
    let stderr_task = tokio::spawn(read_and_parse(
        BufReader::new(stderr),
        state.clone(),
        task_id,
        progress_re,
    ));

    let status = child.wait().await.map_err(|e| e.to_string())?;

    stdout_task.abort();
    stderr_task.abort();

    if !status.success() {
        return Err(format!("{} exited with status: {}", binary, status));
    }

    Ok(())
}

async fn read_and_parse<R: tokio::io::AsyncRead + Unpin>(
    reader: BufReader<R>,
    state: Arc<Mutex<AppState>>,
    task_id: usize,
    progress_re: Regex,
) {
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut app = state.lock().await;
        if let Some(task) = app.downloads.get_mut(task_id) {
            task.log_lines.push(line.clone());
            if task.log_lines.len() > 200 {
                task.log_lines.remove(0);
            }
            if let Some(cap) = progress_re.captures(&line) {
                if let Ok(pct) = cap[1].parse::<f32>() {
                    task.progress = pct.min(100.0) as u8;
                }
            }
        }
    }
}

struct Downloader {
    binary: &'static str,
}

fn select_downloader(url: &str, format: &crate::types::StreamFormat) -> Downloader {
    match format {
        crate::types::StreamFormat::Hls | crate::types::StreamFormat::Dash => {
            Downloader { binary: "N_m3u8DL-RE" }
        }
        crate::types::StreamFormat::Mp4 | crate::types::StreamFormat::Ts => {
            Downloader { binary: "aria2c" }
        }
        _ => {
            let lower = url.to_lowercase();
            if lower.contains(".m3u8") || lower.contains(".mpd") {
                Downloader { binary: "N_m3u8DL-RE" }
            } else {
                Downloader { binary: "aria2c" }
            }
        }
    }
}

fn build_args(
    tool: &Downloader,
    url: &str,
    output_path: &str,
    resolution: Option<String>,
    headers: &std::collections::HashMap<String, String>,
    keys: &[crate::types::KeyInfo],
    is_test: bool,
) -> Vec<String> {
    let mut args = Vec::new();
    let header_flag = if tool.binary == "aria2c" { "--header=" } else { "-H" };
    let need_header_separate = tool.binary == "N_m3u8DL-RE" || tool.binary == "ffmpeg";

    for (k, v) in headers {
        let kl = k.to_lowercase();
        if kl == "host" || kl == "accept-encoding" || kl == "content-length" || kl == "connection" {
            continue;
        }
        if need_header_separate && kl == "cookie" {
            args.push("-H".into());
            args.push(format!("Cookie: {}", v));
        } else if need_header_separate {
            args.push("-H".into());
            args.push(format!("{}: {}", k, v));
        } else {
            args.push(format!("{}{}: {}", header_flag, k, v));
        }
    }

    match tool.binary {
        "N_m3u8DL-RE" => {
            let path = std::path::Path::new(output_path);
            let parent = path.parent().and_then(|p| p.to_str()).unwrap_or("/tmp/spectur-downloads");
            let file_stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("download");
            args.push(url.to_string());
            args.push("--save-dir".into());
            args.push(parent.to_string());
            args.push("--save-name".into());
            args.push(file_stem.to_string());
            args.push("--tmp-dir".into());
            args.push(format!("{}/tmp-{}", parent, file_stem));
            args.push("--auto-select".into());
            args.push("--check-segments-count=false".into());
            if is_test {
                args.push("--custom-range".into());
                args.push("0-5".into());
            }
            if let Some(res) = resolution {
                args.push("-sv".into());
                args.push(format!("res={}", res));
            }
            for key in keys {
                if let Some(ref hex) = key.key_hex {
                    args.push("--key".into());
                    args.push(hex.clone());
                }
            }
        }
        "aria2c" => {
            let path = std::path::Path::new(output_path);
            let parent = path.parent().and_then(|p| p.to_str()).unwrap_or("/tmp/spectur-downloads");
            let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("download.mp4");
            args.push(url.to_string());
            args.push("-d".into());
            args.push(parent.to_string());
            args.push("-o".into());
            args.push(file_name.to_string());
            args.push("--continue=true".into());
            args.push("-x16".into());
            args.push("-s16".into());
        }
        "ffmpeg" => {
            args.push("-i".into());
            args.push(url.to_string());
            args.push("-c".into());
            args.push("copy".into());
            args.push(output_path.to_string());
        }
        _ => {
            args.push(url.to_string());
            args.push("-o".into());
            args.push(output_path.to_string());
        }
    }

    args
}
