use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;
use futures_util::StreamExt;

use crate::types::{AppState, WsMessage, YtFormat};

pub async fn start_ws_server(state: Arc<Mutex<AppState>>) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:9117").await?;

    loop {
        let (stream, _addr) = listener.accept().await?;
        let state = Arc::clone(&state);

        tokio::spawn(async move {
            if let Ok(ws_stream) = accept_async(stream).await {
                handle_connection(ws_stream, state).await;
            }
        });
    }
}

async fn handle_connection(
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    state: Arc<Mutex<AppState>>,
) {
    let (_, mut rx) = ws_stream.split();

    while let Some(result) = rx.next().await {
        match result {
            Ok(msg) => {
                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                    match serde_json::from_str::<WsMessage>(&text) {
                        Ok(ws_msg) => {
                            if ws_msg.is_key_intercepted() {
                                if let Some(key_payload) = ws_msg.to_key_payload() {
                                    let mut app = state.lock().await;
                                    let key_hex: String = key_payload.key.iter()
                                        .map(|b| format!("{:02x}", b))
                                        .collect();
                                    let href = key_payload.href.clone();
                                    let page_url = key_payload.page_url.clone();
                                    app.intercepted_keys.push(key_payload);
                                    app.tui_logs.push(format!("AES-128 key intercepted: {} from {}", key_hex, href));

                                    // Let's associate it with any existing streams in the matching tab
                                    let matching_keys: Vec<String> = app.intercepted_keys.iter()
                                        .filter(|k| k.page_url == page_url)
                                        .map(|k| k.key.iter().map(|b| format!("{:02x}", b)).collect())
                                        .collect();

                                    let mut association_logs = Vec::new();
                                    if let Some(tab) = app.tabs.iter_mut().find(|t| t.page_url == page_url) {
                                        for stream in &mut tab.streams {
                                            if let crate::types::ProbeState::Done(ref mut meta) = stream.probe_state {
                                                for (i, key_info) in meta.keys.iter_mut().enumerate() {
                                                    if let Some(hex) = matching_keys.get(i).or_else(|| matching_keys.first()) {
                                                        if key_info.key_hex.as_ref() != Some(hex) {
                                                            key_info.key_hex = Some(hex.clone());
                                                            association_logs.push(format!("Associated key {} with stream {}", hex, stream.url));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    app.tui_logs.extend(association_logs);
                                }
                            } else if ws_msg.is_youtube_formats() {
                                if let Some(data) = &ws_msg.streaming_data {
                                    let formats = parse_yt_formats(data);
                                    let mut app = state.lock().await;
                                    let page_url = ws_msg.page_url.clone().unwrap_or_else(|| "https://youtube.com/".to_string());
                                    let page_title = ws_msg.page_title.clone().unwrap_or_else(|| "YouTube Video".to_string());
                                    let tab_idx = if let Some(idx) = app.tabs.iter().position(|t| t.page_url == page_url) {
                                        app.tabs[idx].yt_formats = formats;
                                        idx
                                    } else {
                                        let idx = app.tabs.len();
                                        app.tabs.push(crate::types::TabSession {
                                            page_url: page_url.clone(),
                                            page_title: page_title.clone(),
                                            streams: Vec::new(),
                                            show_noise: false,
                                            yt_formats: formats,
                                        });
                                        idx
                                    };
                                    let has_yt = app.tabs.get(tab_idx).map_or(false, |t| {
                                        t.streams.iter().any(|s| s.format == crate::types::StreamFormat::Youtube)
                                    });
                                    if !has_yt {
                                        let new_id = app.next_stream_id;
                                        app.next_stream_id += 1;
                                        if let Some(tab) = app.tabs.get_mut(tab_idx) {
                                            tab.streams.push(crate::types::CapturedStream {
                                                stream_id: new_id,
                                            url: page_url.clone(),
                                            method: "GET".to_string(),
                                            request_headers: std::collections::HashMap::new(),
                                            server_ip: "".to_string(),
                                            format: crate::types::StreamFormat::Youtube,
                                            probe_state: crate::types::ProbeState::Done(crate::types::StreamMetadata {
                                                duration_seconds: 0.0,
                                                total_segments: 0,
                                                resolutions: Vec::new(),
                                                audio_tracks: Vec::new(),
                                                keys: Vec::new(),
                                                drm: Vec::new(),
                                                segment_base_url: None,
                                                size_bytes: None,
                                            }),
                                            manifest_content: None,
                                        });
                                        } // close if let Some(tab)
                                    } // close if !has_yt
                                    app.tui_logs.push(format!("YouTube formats parsed for {}", page_title));
                                }
                            } else if let Some(payload) = ws_msg.to_stream_payload() {
                                let url = payload.url.clone();
                                let headers = payload.request_headers.clone();
                                let manifest_content = payload.manifest_content.clone();

                                let mut app = state.lock().await;
                                let (_, stream_id, exists) = app.add_stream(payload);

                                if !exists {
                                    let analyzer_state = Arc::clone(&state);
                                    tokio::spawn(async move {
                                        crate::analyzer::analyze_manifest(analyzer_state, stream_id, url, headers, manifest_content).await;
                                    });
                                }
                            }
                        }
                        Err(e) => {
                            let mut app = state.lock().await;
                            app.tui_logs.push(format!("WS parse error: {}", e));
                        }
                    }
                }
            }
            Err(e) => {
                let mut app = state.lock().await;
                app.tui_logs.push(format!("WS error: {}", e));
                break;
            }
        }
    }
}

fn parse_yt_formats(data: &serde_json::Value) -> Vec<YtFormat> {
    let mut formats = Vec::new();
    let mut extract = |arr: &serde_json::Value| {
        if let Some(items) = arr.as_array() {
            for item in items {
                let itag = item.get("itag").and_then(|v| v.as_i64()).unwrap_or(0);
                let mime_type = item.get("mimeType")
                    .or_else(|| item.get("mime_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let bitrate = item.get("bitrate").and_then(|v| v.as_i64());
                let width = item.get("width").and_then(|v| v.as_i64());
                let height = item.get("height").and_then(|v| v.as_i64());
                let fps = item.get("fps").and_then(|v| v.as_i64());
                let quality_label = item.get("qualityLabel")
                    .or_else(|| item.get("quality_label"))
                    .or_else(|| item.get("quality"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let content_length = item.get("contentLength")
                    .or_else(|| item.get("content_length"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let approx_duration_ms = item.get("approxDurationMs")
                    .or_else(|| item.get("approx_duration_ms"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let audio_channels = item.get("audioChannels")
                    .or_else(|| item.get("audio_channels"))
                    .and_then(|v| v.as_i64());
                let audio_sample_rate = item.get("audioSampleRate")
                    .or_else(|| item.get("audio_sample_rate"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                formats.push(YtFormat {
                    itag,
                    mime_type,
                    bitrate,
                    width,
                    height,
                    fps,
                    quality_label,
                    content_length,
                    approx_duration_ms,
                    audio_channels,
                    audio_sample_rate,
                });
            }
        }
    };
    if let Some(formats_arr) = data.get("formats") { extract(formats_arr); }
    if let Some(adaptive_arr) = data.get("adaptiveFormats") { extract(adaptive_arr); }
    formats
}
