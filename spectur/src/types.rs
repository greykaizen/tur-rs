use std::collections::HashMap;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct StreamPayload {
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub url: String,
    pub method: String,
    #[serde(rename = "requestHeaders")]
    pub request_headers: HashMap<String, String>,
    #[serde(rename = "responseHeaders")]
    pub response_headers: HashMap<String, String>,
    #[serde(rename = "serverIp")]
    pub server_ip: String,
    #[serde(rename = "pageUrl")]
    pub page_url: String,
    #[serde(rename = "pageTitle")]
    pub page_title: String,
    pub timestamp: u64,
    #[serde(rename = "manifestContent")]
    pub manifest_content: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct WsMessage {
    #[serde(rename = "type")]
    pub msg_type: Option<String>,
    #[serde(rename = "requestId")]
    pub request_id: Option<String>,
    pub url: Option<String>,
    pub method: Option<String>,
    #[serde(rename = "requestHeaders")]
    pub request_headers: Option<HashMap<String, String>>,
    #[serde(rename = "responseHeaders")]
    pub response_headers: Option<HashMap<String, String>>,
    #[serde(rename = "serverIp")]
    pub server_ip: Option<String>,
    #[serde(rename = "pageUrl")]
    pub page_url: Option<String>,
    #[serde(rename = "pageTitle")]
    pub page_title: Option<String>,
    pub timestamp: Option<u64>,
    #[serde(rename = "manifestContent")]
    pub manifest_content: Option<String>,
    pub key: Option<Vec<u8>>,
    pub href: Option<String>,
    #[serde(rename = "streamingData")]
    pub streaming_data: Option<serde_json::Value>,
}

impl WsMessage {
    pub fn is_stream_payload(&self) -> bool {
        self.msg_type.is_none() && self.url.is_some()
    }

    pub fn is_key_intercepted(&self) -> bool {
        self.msg_type.as_deref() == Some("keyIntercepted")
    }

    pub fn is_youtube_formats(&self) -> bool {
        self.msg_type.as_deref() == Some("youtubeFormats")
    }

    pub fn to_stream_payload(&self) -> Option<StreamPayload> {
        Some(StreamPayload {
            request_id: self.request_id.clone()?,
            url: self.url.clone()?,
            method: self.method.clone().unwrap_or_default(),
            request_headers: self.request_headers.clone().unwrap_or_default(),
            response_headers: self.response_headers.clone().unwrap_or_default(),
            server_ip: self.server_ip.clone().unwrap_or_default(),
            page_url: self.page_url.clone().unwrap_or_default(),
            page_title: self.page_title.clone().unwrap_or_default(),
            timestamp: self.timestamp.unwrap_or(0),
            manifest_content: self.manifest_content.clone(),
        })
    }

    pub fn to_key_payload(&self) -> Option<KeyPayload> {
        Some(KeyPayload {
            key: self.key.clone()?,
            href: self.href.clone().unwrap_or_default(),
            page_url: self.page_url.clone().unwrap_or_default(),
            page_title: self.page_title.clone().unwrap_or_default(),
            timestamp: self.timestamp.unwrap_or(0),
        })
    }
}

#[derive(Debug, Clone)]
pub struct KeyPayload {
    pub key: Vec<u8>,
    pub href: String,
    pub page_url: String,
    pub page_title: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub next_stream_id: u64,
    pub intercepted_keys: Vec<KeyPayload>,
    pub selected_yt_format_index: usize,
    pub selected_tab_index: usize,
    pub selected_stream_index: usize,
    pub selected_resolution_index: usize,
    pub selected_download_index: usize,
    pub tabs: Vec<TabSession>,
    pub downloads: Vec<DownloadTask>,
    pub tui_logs: Vec<String>,
    pub focused_panel: Panel,
}

#[derive(Debug, Clone)]
pub struct TabSession {
    pub page_url: String,
    pub page_title: String,
    pub streams: Vec<CapturedStream>,
    pub show_noise: bool,
    pub yt_formats: Vec<YtFormat>,
}

impl TabSession {
    pub fn filtered_streams(&self) -> Vec<&CapturedStream> {
        self.streams.iter()
            .filter(|s| self.show_noise || !s.is_noise())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProbeState {
    Probing,
    Done(StreamMetadata),
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct CapturedStream {
    pub stream_id: u64,
    pub url: String,
    pub method: String,
    pub request_headers: HashMap<String, String>,
    pub server_ip: String,
    pub format: StreamFormat,
    pub probe_state: ProbeState,
    pub manifest_content: Option<String>,
}

impl CapturedStream {
    pub fn is_noise(&self) -> bool {
        if self.format == StreamFormat::Ts {
            return true;
        }
        let url_lower = self.url.to_lowercase();
        if url_lower.contains(".m4s") 
            || url_lower.contains("/segment") 
            || url_lower.contains("/fragment") 
            || url_lower.contains("/chunk") 
            || url_lower.contains("/init-") 
            || url_lower.contains("seg-") 
            || url_lower.contains("/range/") 
            || url_lower.contains("/bytes/") 
        {
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamFormat {
    Hls,
    Dash,
    Mp4,
    Ts,
    Youtube,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamMetadata {
    pub duration_seconds: f32,
    pub total_segments: usize,
    pub resolutions: Vec<ResolutionInfo>,
    pub audio_tracks: Vec<String>,
    pub keys: Vec<KeyInfo>,
    pub drm: Vec<DrmInfo>,
    pub segment_base_url: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolutionInfo {
    pub label: String,
    pub bandwidth: u64,
    pub codecs: Option<String>,
    pub frame_rate: Option<String>,
    pub mime_type: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyInfo {
    pub method: String,
    pub uri: Option<String>,
    pub iv: Option<String>,
    pub keyformat: Option<String>,
    pub key_hex: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct YtFormat {
    pub itag: i64,
    pub mime_type: String,
    pub bitrate: Option<i64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub fps: Option<i64>,
    pub quality_label: Option<String>,
    pub content_length: Option<String>,
    pub approx_duration_ms: Option<String>,
    pub audio_channels: Option<i64>,
    pub audio_sample_rate: Option<String>,
}

impl YtFormat {
    pub fn is_video(&self) -> bool {
        self.width.is_some() && self.height.is_some()
    }

    pub fn is_audio_only(&self) -> bool {
        self.mime_type.starts_with("audio/")
    }

    pub fn resolution_label(&self) -> String {
        if let (Some(w), Some(h)) = (self.width, self.height) {
            if let Some(ref ql) = self.quality_label {
                format!("{}x{} ({})", w, h, ql)
            } else {
                format!("{}x{}", w, h)
            }
        } else if let Some(ref ql) = self.quality_label {
            ql.clone()
        } else if self.is_audio_only() {
            format!("Audio (itag {})", self.itag)
        } else {
            format!("itag {}", self.itag)
        }
    }

    pub fn short_label(&self) -> String {
        if let Some(ref ql) = self.quality_label {
            ql.clone()
        } else if self.is_audio_only() {
            "Audio".into()
        } else {
            format!("itag {}", self.itag)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DrmInfo {
    pub system: String,
    pub scheme_id_uri: String,
    pub pssh_data: Option<String>,
    pub default_kid: Option<String>,
    pub license_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub id: usize,
    pub stream_url: String,
    pub output_path: String,
    pub progress: u8,
    pub speed_mbps: f32,
    pub log_lines: Vec<String>,
    pub status: DownloadStatus,
}

#[derive(Debug, Clone)]
pub enum DownloadStatus {
    Queued,
    Running,
    Finished,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Panel {
    Streams,
    Metadata,
    Downloads,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            next_stream_id: 1,
            selected_tab_index: 0,
            selected_stream_index: 0,
            selected_resolution_index: 0,
            selected_yt_format_index: 0,
            selected_download_index: 0,
            tabs: Vec::new(),
            downloads: Vec::new(),
            tui_logs: Vec::new(),
            intercepted_keys: Vec::new(),
            focused_panel: Panel::Streams,
        }
    }

    pub fn add_stream(&mut self, payload: StreamPayload) -> (usize, u64, bool) {
        let format = detect_format(&payload.url, &payload.response_headers);
        let dedup = path_dedup(&payload.url);

        let tab_pos = self
            .tabs
            .iter()
            .position(|t| t.page_url == payload.page_url);

        if let Some(idx) = tab_pos {
            let tab = &mut self.tabs[idx];
            if let Some(existing) = tab.streams.iter_mut().find(|s| {
                if s.url == payload.url {
                    return true;
                }
                if format != StreamFormat::Mp4 && s.format == format && path_dedup(&s.url) == dedup {
                    return true;
                }
                false
            }) {
                let stream_id = existing.stream_id;
                let should_analyze = if payload.manifest_content.is_some() {
                    match &existing.probe_state {
                        ProbeState::Done(_) => false,
                        _ => {
                            existing.manifest_content = payload.manifest_content.clone();
                            existing.probe_state = ProbeState::Probing;
                            true
                        }
                    }
                } else {
                    false
                };
                existing.url = payload.url;
                existing.request_headers = payload.request_headers;
                (idx, stream_id, !should_analyze)
            } else {
                let stream_id = self.next_stream_id;
                self.next_stream_id += 1;
                let captured = CapturedStream {
                    stream_id,
                    url: payload.url.clone(),
                    method: payload.method,
                    request_headers: payload.request_headers.clone(),
                    server_ip: payload.server_ip,
                    format,
                    probe_state: ProbeState::Probing,
                    manifest_content: payload.manifest_content.clone(),
                };
                tab.streams.push(captured);
                (idx, stream_id, false)
            }
        } else {
            let stream_id = self.next_stream_id;
            self.next_stream_id += 1;
            let captured = CapturedStream {
                stream_id,
                url: payload.url.clone(),
                method: payload.method,
                request_headers: payload.request_headers.clone(),
                server_ip: payload.server_ip,
                format,
                probe_state: ProbeState::Probing,
                manifest_content: payload.manifest_content.clone(),
            };
            let idx = self.tabs.len();
            self.tabs.push(TabSession {
                page_url: payload.page_url,
                page_title: if payload.page_title.is_empty() {
                    "Unknown Page".into()
                } else {
                    payload.page_title
                },
                streams: vec![captured],
                show_noise: false,
                yt_formats: Vec::new(),
            });
            (idx, stream_id, false)
        }
    }

    pub fn selected_stream(&self) -> Option<&CapturedStream> {
        if self.tabs.is_empty() {
            return None;
        }
        let tab = &self.tabs[self.selected_tab_index];
        let fs = tab.filtered_streams();
        if fs.is_empty() {
            return None;
        }
        if self.selected_stream_index >= fs.len() {
            return None;
        }
        Some(fs[self.selected_stream_index])
    }

    pub fn next_tab(&mut self) {
        if self.tabs.len() > 1 {
            self.selected_tab_index = (self.selected_tab_index + 1) % self.tabs.len();
            self.selected_stream_index = 0;
            self.selected_resolution_index = 0;
        }
    }

    pub fn prev_tab(&mut self) {
        if !self.tabs.is_empty() {
            self.selected_tab_index = self
                .selected_tab_index
                .checked_sub(1)
                .unwrap_or(self.tabs.len() - 1);
            self.selected_stream_index = 0;
            self.selected_resolution_index = 0;
        }
    }

    pub fn next_stream(&mut self) {
        if !self.tabs.is_empty() {
            let tab = &self.tabs[self.selected_tab_index];
            let fs_len = tab.filtered_streams().len();
            if fs_len > 1 {
                self.selected_stream_index =
                    (self.selected_stream_index + 1) % fs_len;
                self.selected_resolution_index = 0;
            }
        }
    }

    pub fn prev_stream(&mut self) {
        if !self.tabs.is_empty() {
            let tab = &self.tabs[self.selected_tab_index];
            let fs_len = tab.filtered_streams().len();
            if fs_len > 0 {
                self.selected_stream_index = self
                    .selected_stream_index
                    .checked_sub(1)
                    .unwrap_or(fs_len - 1);
                self.selected_resolution_index = 0;
            }
        }
    }

    pub fn next_resolution(&mut self) {
        if let Some(stream) = self.selected_stream() {
            if let ProbeState::Done(meta) = &stream.probe_state {
                if !meta.resolutions.is_empty() {
                    self.selected_resolution_index =
                        (self.selected_resolution_index + 1) % meta.resolutions.len();
                }
            }
        }
    }

    pub fn prev_resolution(&mut self) {
        if let Some(stream) = self.selected_stream() {
            if let ProbeState::Done(meta) = &stream.probe_state {
                if !meta.resolutions.is_empty() {
                    self.selected_resolution_index = self
                        .selected_resolution_index
                        .checked_sub(1)
                        .unwrap_or(meta.resolutions.len() - 1);
                }
            }
        }
    }

    pub fn set_stream_probe_done(&mut self, stream_id: u64, metadata: StreamMetadata, format: StreamFormat) {
        for tab in &mut self.tabs {
            if let Some(stream) = tab.streams.iter_mut().find(|s| s.stream_id == stream_id) {
                stream.probe_state = ProbeState::Done(metadata);
                stream.format = format;
                return;
            }
        }
    }

    pub fn set_stream_probe_failed(&mut self, stream_id: u64, error: String) {
        for tab in &mut self.tabs {
            if let Some(stream) = tab.streams.iter_mut().find(|s| s.stream_id == stream_id) {
                stream.probe_state = ProbeState::Failed(error);
                return;
            }
        }
    }
}

fn detect_format(url: &str, response_headers: &HashMap<String, String>) -> StreamFormat {
    if let Ok(parsed) = url::Url::parse(url) {
        let path = parsed.path().to_lowercase();
        if path.contains(".m3u8") {
            return StreamFormat::Hls;
        }
        if path.contains(".mpd") {
            return StreamFormat::Dash;
        }
        if path.contains(".mp4") {
            return StreamFormat::Mp4;
        }
        if path.contains(".ts") {
            return StreamFormat::Ts;
        }
    }
    for (k, v) in response_headers {
        if k.to_lowercase() == "content-type" {
            let v_lower = v.to_lowercase();
            if v_lower.contains("mpegurl") {
                return StreamFormat::Hls;
            }
            if v_lower.contains("dash+xml") {
                return StreamFormat::Dash;
            }
            if v_lower.contains("video/mp4") {
                return StreamFormat::Mp4;
            }
            if v_lower.contains("video/mp2t") {
                return StreamFormat::Ts;
            }
        }
    }
    StreamFormat::Unknown
}

/*
pub fn translate_generic_json_manifest(url: &str) -> String {
    if let Ok(mut parsed) = url::Url::parse(url) {
        let path = parsed.path().to_lowercase();
        let is_json_manifest = path.contains("master.json") || path.contains("playlist.json");
        if is_json_manifest {
            let mut new_path = parsed.path().to_string();
            if new_path.contains("master.json") {
                new_path = new_path.replace("master.json", "master.mpd");
            } else if new_path.contains("playlist.json") {
                new_path = new_path.replace("playlist.json", "playlist.mpd");
            }
            parsed.set_path(&new_path);

            let mut params = Vec::new();
            let mut has_ranges = false;
            for (k, v) in parsed.query_pairs() {
                if k == "base64_init" {
                    continue; // Strip base64_init parameter to avoid inline blobs
                }
                if k == "query_string_ranges" {
                    has_ranges = true;
                }
                params.push((k.into_owned(), v.into_owned()));
            }
            if !has_ranges {
                params.push(("query_string_ranges".to_string(), "1".to_string()));
            }

            let mut query_str = String::new();
            for (i, (k, v)) in params.iter().enumerate() {
                if i > 0 { query_str.push('&'); }
                query_str.push_str(&format!("{}={}", k, v));
            }
            parsed.set_query(Some(&query_str));
            return parsed.to_string();
        }
    }
    url.to_string()
}
*/

fn path_dedup(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        let path = parsed.path();
        if let Some(last_slash) = path.rfind('/') {
            let base = &path[..last_slash + 1];
            return format!("{}|{}", parsed.host_str().unwrap_or(""), base);
        }
    }
    url.to_string()
}
