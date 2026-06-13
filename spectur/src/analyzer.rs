use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::types::{AppState, DrmInfo, KeyInfo, ResolutionInfo, StreamMetadata, StreamFormat};

pub async fn analyze_manifest(
    state: Arc<Mutex<AppState>>,
    stream_id: u64,
    url: String,
    headers: HashMap<String, String>,
    manifest_content: Option<String>,
) {
    let result = detect_and_fetch(&url, headers, manifest_content).await;
    let mut app = state.lock().await;
    match result {
        Ok((mut metadata, format)) => {
            let matching_keys: Vec<String> = app.intercepted_keys.iter()
                .filter(|k| {
                    let stream_page = app.tabs.iter()
                        .flat_map(|t| &t.streams)
                        .find(|s| s.stream_id == stream_id)
                        .and_then(|s| app.tabs.iter().find(|t| t.streams.iter().any(|ts| ts.stream_id == s.stream_id)))
                        .map(|t| &t.page_url);
                    stream_page.map_or(false, |p| &k.page_url == p)
                })
                .map(|k| k.key.iter().map(|b| format!("{:02x}", b)).collect())
                .collect();

            for (i, key_info) in metadata.keys.iter_mut().enumerate() {
                if let Some(hex) = matching_keys.get(i).or_else(|| matching_keys.first()) {
                    key_info.key_hex = Some(hex.clone());
                }
            }
            app.set_stream_probe_done(stream_id, metadata, format);
        }
        Err(e) => {
            let err_str = e.to_string();
            app.tui_logs.push(format!("Analyzer error for stream {}: {}", stream_id, err_str));
            app.set_stream_probe_failed(stream_id, err_str);
        }
    }
}

async fn detect_and_fetch(
    url: &str,
    headers: HashMap<String, String>,
    manifest_content: Option<String>,
) -> Result<(StreamMetadata, StreamFormat), Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(tokio::time::Duration::from_secs(15), async {
        if let Some(content) = manifest_content {
            let content_upper = content.to_uppercase();
            if let Some(idx) = content_upper.find("#EXTM3U") {
                let trimmed_content = &content[idx..];
                if let Ok(meta) = parse_hls_content(trimmed_content, url) {
                    return Ok((meta, StreamFormat::Hls));
                }
            } else if content_upper.contains("<MPD") && content_upper.contains("</MPD>") {
                if let Ok(meta) = parse_dash_content(&content, url) {
                    return Ok((meta, StreamFormat::Dash));
                }
            }
        }

        if let Ok(parsed) = url::Url::parse(url) {
            let path = parsed.path().to_lowercase();
            if path.contains(".m3u8") {
                parse_hls(url, headers).await
            } else if path.contains(".mpd") {
                parse_dash(url, headers).await
            } else if path.contains("master.json") || path.contains("playlist.json") {
                // Vimeo serves DASH MPD at the same path with .mpd extension
                let mpd_url = url.replace(".json", ".mpd");
                let mpd_url = if mpd_url.contains('?') {
                    format!("{}&query_string_ranges=1", mpd_url)
                } else {
                    format!("{}?query_string_ranges=1", mpd_url)
                };
                parse_dash(&mpd_url, headers).await
            } else if path.contains(".mp4") {
                parse_mp4(url, headers).await
            } else {
                probe_format_and_parse(url, headers).await
            }
        } else {
            probe_format_and_parse(url, headers).await
        }
    })
    .await
    .map_err(|_| Box::<dyn std::error::Error + Send + Sync>::from("Probing timed out after 15 seconds"))?
}

async fn fetch_with_redirects(
    client: &wreq::Client,
    initial_url: &str,
    headers: &HashMap<String, String>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut current_url = initial_url.to_string();
    let mut redirects_followed = 0;
    const MAX_REDIRECTS: usize = 10;

    loop {
        let mut req = client.get(&current_url);
        for (k, v) in headers {
            let k_lower = k.to_lowercase();
            if k_lower == "host" || k_lower == "accept-encoding" || k_lower == "content-length" || k_lower == "connection" {
                continue;
            }
            if let (Ok(name), Ok(value)) = (wreq::header::HeaderName::from_bytes(k.as_bytes()), wreq::header::HeaderValue::from_str(v)) {
                req = req.header(name, value);
            }
        }

        let resp = req.send().await?;
        let status = resp.status();

        if status.is_redirection() {
            if redirects_followed >= MAX_REDIRECTS {
                return Err("too many redirects".into());
            }
            if let Some(loc_val) = resp.headers().get("location") {
                let loc_str = loc_val.to_str()?;
                let base = url::Url::parse(&current_url)?;
                let next_url = base.join(loc_str)?;
                current_url = next_url.to_string();
                redirects_followed += 1;
                continue;
            }
        }

        if !status.is_success() {
            return Err(format!("HTTP error status: {}", status).into());
        }

        let body = resp.text().await?;
        return Ok(body);
    }
}

fn resolve_segment_base(base_url: &str, segment_uri: &str) -> Option<String> {
    if segment_uri.starts_with("http://") || segment_uri.starts_with("https://") {
        return Some(segment_uri.to_string());
    }
    if let Ok(base) = url::Url::parse(base_url) {
        match base.join(segment_uri) {
            Ok(resolved) => Some(resolved.to_string()),
            Err(_) => None,
        }
    } else {
        None
    }
}

async fn parse_hls(
    url: &str,
    headers: HashMap<String, String>,
) -> Result<(StreamMetadata, StreamFormat), Box<dyn std::error::Error + Send + Sync>> {
    let client = wreq::Client::builder()
        .emulation(wreq_util::Emulation::Firefox136)
        .redirect(wreq::redirect::Policy::none())
        .build()?;

    let body = fetch_with_redirects(&client, url, &headers).await?;
    let base_url = extract_base_url(url);
    let meta = parse_hls_content(&body, &base_url)?;
    let meta = StreamMetadata {
        segment_base_url: Some(base_url),
        ..meta
    };
    Ok((meta, StreamFormat::Hls))
}

fn extract_base_url(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        if let Some(last_slash) = parsed.as_str().rfind('/') {
            let base = &parsed.as_str()[..last_slash + 1];
            return base.to_string();
        }
    }
    url.to_string()
}

fn parse_hls_content(body: &str, base_url: &str) -> Result<StreamMetadata, Box<dyn std::error::Error + Send + Sync>> {
    let (_, playlist) = m3u8_rs::parse_playlist(body.as_bytes())
        .map_err(|e| format!("m3u8 parse error: {:?}", e))?;

    match playlist {
        m3u8_rs::Playlist::MasterPlaylist(master) => {
            let mut resolutions = Vec::new();
            let mut audio_tracks = Vec::new();
            let mut keys = Vec::new();
            let drm = Vec::new();

            for variant in &master.variants {
                if let Some(res) = &variant.resolution {
                    let label = format!("{}x{}", res.width, res.height);
                    let bw = variant.bandwidth;
                    let variant_url = resolve_segment_base(base_url, &variant.uri).unwrap_or(variant.uri.clone());
                    if !resolutions.iter().any(|r: &ResolutionInfo| r.label == label) {
                        resolutions.push(ResolutionInfo {
                            label,
                            bandwidth: bw,
                            codecs: variant.codecs.clone(),
                            frame_rate: variant.frame_rate.clone().map(|f| format!("{:.3}", f)),
                            mime_type: None,
                            url: Some(variant_url),
                        });
                    }
                }
            }

            for alt in &master.alternatives {
                if alt.media_type == m3u8_rs::AlternativeMediaType::Audio {
                    audio_tracks.push(alt.name.clone());
                }
            }

            for sess_key in &master.session_key {
                let absolute_uri = sess_key.0.uri.as_ref().map(|u| {
                    resolve_segment_base(base_url, u).unwrap_or_else(|| u.clone())
                });
                keys.push(KeyInfo {
                    method: format!("{:?}", sess_key.0.method),
                    uri: absolute_uri,
                    iv: sess_key.0.iv.clone(),
                    keyformat: sess_key.0.keyformat.clone(),
                    key_hex: None,
                });
            }

            Ok(StreamMetadata {
                duration_seconds: 0.0,
                total_segments: 0,
                resolutions,
                audio_tracks,
                keys,
                drm,
                segment_base_url: None,
                size_bytes: None,
            })
        }
        m3u8_rs::Playlist::MediaPlaylist(media) => {
            let duration: f32 = media.segments.iter().map(|s| s.duration).sum();
            let total_segments = media.segments.len();
            let resolutions = Vec::new();
            let audio_tracks = Vec::new();
            let drm = Vec::new();

            let mut keys = Vec::new();
            let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
            for seg in &media.segments {
                if let Some(key) = &seg.key {
                    let key_id = format!("{:?}:{}", key.method, key.uri.as_deref().unwrap_or(""));
                    if seen_keys.insert(key_id) {
                        let absolute_uri = key.uri.as_ref().map(|u| {
                            resolve_segment_base(base_url, u).unwrap_or_else(|| u.clone())
                        });
                        keys.push(KeyInfo {
                            method: format!("{:?}", key.method),
                            uri: absolute_uri,
                            iv: key.iv.clone(),
                            keyformat: key.keyformat.clone(),
                            key_hex: None,
                        });
                    }
                }
            }

            Ok(StreamMetadata {
                duration_seconds: duration,
                total_segments,
                resolutions,
                audio_tracks,
                keys,
                drm,
                segment_base_url: None,
                size_bytes: None,
            })
        }
    }
}

async fn parse_dash(
    url: &str,
    headers: HashMap<String, String>,
) -> Result<(StreamMetadata, StreamFormat), Box<dyn std::error::Error + Send + Sync>> {
    let client = wreq::Client::builder()
        .emulation(wreq_util::Emulation::Firefox136)
        .redirect(wreq::redirect::Policy::none())
        .build()?;

    let body = fetch_with_redirects(&client, url, &headers).await?;
    let base_url = extract_base_url(url);
    let meta = parse_dash_content(&body, &base_url)?;
    let meta = StreamMetadata {
        segment_base_url: Some(base_url),
        ..meta
    };
    Ok((meta, StreamFormat::Dash))
}

fn resolve_dash_url(
    manifest_url: &str,
    mpd: &dash_mpd::MPD,
    period: &dash_mpd::Period,
    adaptation: &dash_mpd::AdaptationSet,
    representation: &dash_mpd::Representation,
) -> Option<String> {
    let mut current = if manifest_url.ends_with(".mpd") || manifest_url.contains('?') || manifest_url.split('/').last().map(|s| s.contains('.')).unwrap_or(false) {
        extract_base_url(manifest_url)
    } else {
        manifest_url.to_string()
    };

    let mut join_url = |rel: &str| {
        if rel.starts_with("http://") || rel.starts_with("https://") {
            current = rel.to_string();
        } else if let Ok(base) = url::Url::parse(&current) {
            if let Ok(joined) = base.join(rel) {
                current = joined.to_string();
            }
        }
    };

    if let Some(bu) = mpd.base_url.first() {
        join_url(&bu.base);
    }
    if let Some(bu) = period.BaseURL.first() {
        join_url(&bu.base);
    }
    if let Some(bu) = adaptation.BaseURL.first() {
        join_url(&bu.base);
    }
    if let Some(bu) = representation.BaseURL.first() {
        join_url(&bu.base);
    }

    Some(current)
}

fn parse_dash_content(body: &str, base_url: &str) -> Result<StreamMetadata, Box<dyn std::error::Error + Send + Sync>> {
    let mpd = dash_mpd::parse(body)
        .map_err(|e| format!("MPD parse error: {:?}", e))?;

    let duration_seconds = mpd.mediaPresentationDuration
        .as_ref()
        .map(|d| d.as_secs() as f32)
        .unwrap_or(0.0);

    let mut resolutions = Vec::new();
    let mut audio_tracks = Vec::new();
    let mut drm_infos = Vec::new();

    for period in &mpd.periods {
        for adaptation in &period.adaptations {
            let is_video = adaptation.contentType.as_deref() == Some("video");
            let is_audio = adaptation.contentType.as_deref() == Some("audio");
            let as_codecs = adaptation.codecs.clone();
            let as_fps = adaptation.frameRate.clone();
            let as_mime = adaptation.mimeType.clone();

            for rep in &adaptation.representations {
                let codecs = rep.codecs.clone().or_else(|| as_codecs.clone());
                let frame_rate = rep.frameRate.clone().or_else(|| as_fps.clone());
                let mime_type = rep.mimeType.clone().or_else(|| as_mime.clone());

                if is_video {
                    if let (Some(w), Some(h)) = (rep.width, rep.height) {
                        let label = format!("{}x{}", w, h);
                        let bw = rep.bandwidth.unwrap_or(0);
                        let rep_url = resolve_dash_url(base_url, &mpd, period, adaptation, rep);
                        if !resolutions.iter().any(|r: &ResolutionInfo| r.label == label) {
                            resolutions.push(ResolutionInfo {
                                label,
                                bandwidth: bw,
                                codecs,
                                frame_rate,
                                mime_type,
                                url: rep_url,
                            });
                        }
                    }
                }
                if is_audio {
                    if let Some(id) = &rep.id {
                        audio_tracks.push(id.clone());
                    }
                }
            }
        }
    }

    let mut all_drm = Vec::new();
    all_drm.extend(mpd.ContentProtection.iter().cloned());
    for period in &mpd.periods {
        all_drm.extend(period.ContentProtection.iter().cloned());
        for adaptation in &period.adaptations {
            all_drm.extend(adaptation.ContentProtection.iter().cloned());
            for rep in &adaptation.representations {
                all_drm.extend(rep.ContentProtection.iter().cloned());
            }
        }
    }

    for cp in &all_drm {
        let system = classify_drm(cp);
        let pssh_data = cp.cenc_pssh.first()
            .and_then(|p| p.content.clone());
        let license_url = cp.laurl.as_ref()
            .and_then(|l| l.content.clone())
            .or_else(|| cp.clearkey_laurl.as_ref().and_then(|l| l.content.clone()));

        let unique_dedup = format!("{}-{:?}", cp.schemeIdUri, cp.default_KID);
        if !drm_infos.iter().any(|d: &DrmInfo| {
            let ddedup = format!("{}-{:?}", d.scheme_id_uri, d.default_kid);
            ddedup == unique_dedup
        }) {
            drm_infos.push(DrmInfo {
                system,
                scheme_id_uri: cp.schemeIdUri.clone(),
                pssh_data,
                default_kid: cp.default_KID.clone(),
                license_url,
            });
        }
    }

    let total_segments = mpd.periods.iter()
        .flat_map(|p| &p.adaptations)
        .flat_map(|a| &a.representations)
        .filter_map(|r| r.SegmentTemplate.as_ref())
        .filter_map(|st| st.SegmentTimeline.as_ref())
        .map(|tl| tl.segments.len())
        .sum::<usize>()
        + mpd.periods.iter()
            .flat_map(|p| &p.adaptations)
            .flat_map(|a| &a.representations)
            .filter_map(|r| r.SegmentList.as_ref())
            .map(|sl| sl.segment_urls.len())
            .sum::<usize>();

    Ok(StreamMetadata {
        duration_seconds,
        total_segments,
        resolutions,
        audio_tracks,
        keys: Vec::new(),
        drm: drm_infos,
        segment_base_url: None,
        size_bytes: None,
    })
}

fn classify_drm(cp: &dash_mpd::ContentProtection) -> String {
    let uri = cp.schemeIdUri.to_lowercase();
    if uri.contains("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed") {
        return "Widevine".into();
    }
    if uri.contains("9a04f079-9840-4286-ab92-e65be0885f95") {
        return "PlayReady".into();
    }
    if uri.contains("94ce86fb-07ff-4f43-adb8-93d2fa968ca2") {
        return "FairPlay".into();
    }
    if uri.contains("1077efec-c0b2-4d02-ace3-3c1e52e2fb4b") {
        return "ClearKey".into();
    }
    if uri.contains("mp4protection") {
        return "CENC".into();
    }
    if let Some(v) = &cp.value {
        let v = v.to_lowercase();
        if v.contains("widevine") { return "Widevine".into(); }
        if v.contains("playready") { return "PlayReady".into(); }
        if v.contains("cenc") { return "CENC".into(); }
    }
    "Unknown".into()
}

async fn parse_mp4(
    url: &str,
    headers: HashMap<String, String>,
) -> Result<(StreamMetadata, StreamFormat), Box<dyn std::error::Error + Send + Sync>> {
    let mut header_str = String::new();
    for (k, v) in &headers {
        let kl = k.to_lowercase();
        if kl == "host" || kl == "accept-encoding" || kl == "content-length" || kl == "connection" || kl == "range" {
            continue;
        }
        header_str.push_str(&format!("{}: {}\r\n", k, v));
    }

    let mut cmd = tokio::process::Command::new("ffprobe");
    cmd.kill_on_drop(true)
       .arg("-v").arg("error")
       .arg("-show_entries")
       .arg("format=duration,size,bit_rate:stream=codec_name,codec_type,width,height,r_frame_rate")
       .arg("-of").arg("json");

    if !header_str.is_empty() {
        cmd.arg("-headers").arg(header_str);
    }
    cmd.arg(url);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let output_res = tokio::time::timeout(tokio::time::Duration::from_secs(5), cmd.output()).await;

    match output_res {
        Ok(Ok(output)) if output.status.success() => {
            if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                let duration = json_val["format"]["duration"]
                    .as_str()
                    .and_then(|s| s.parse::<f32>().ok())
                    .unwrap_or(0.0);

                let size_bytes = json_val["format"]["size"]
                    .as_str()
                    .and_then(|s| s.parse::<u64>().ok());

                let bit_rate = json_val["format"]["bit_rate"]
                    .as_str()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let mut resolutions = Vec::new();
                let mut audio_tracks = Vec::new();

                if let Some(streams) = json_val["streams"].as_array() {
                    for stream in streams {
                        let codec_type = stream["codec_type"].as_str().unwrap_or("");
                        if codec_type == "video" {
                            let width = stream["width"].as_i64().unwrap_or(0);
                            let height = stream["height"].as_i64().unwrap_or(0);
                            let codec = stream["codec_name"].as_str().map(|s| s.to_string());
                            let frame_rate_raw = stream["r_frame_rate"].as_str().unwrap_or("");
                            let clean_fr = if !frame_rate_raw.is_empty() {
                                let fr = clean_frame_rate(frame_rate_raw);
                                if fr != "0" { Some(fr) } else { None }
                            } else {
                                None
                            };

                            if width > 0 && height > 0 {
                                resolutions.push(ResolutionInfo {
                                    label: format!("{}x{}", width, height),
                                    bandwidth: bit_rate,
                                    codecs: codec,
                                    frame_rate: clean_fr,
                                    mime_type: Some("video/mp4".to_string()),
                                    url: Some(url.to_string()),
                                });
                            }
                        } else if codec_type == "audio" {
                            let codec = stream["codec_name"].as_str().unwrap_or("unknown");
                            audio_tracks.push(format!("Audio Track ({})", codec));
                        }
                    }
                }

                return Ok((StreamMetadata {
                    duration_seconds: duration,
                    total_segments: 1,
                    resolutions,
                    audio_tracks,
                    keys: Vec::new(),
                    drm: Vec::new(),
                    segment_base_url: None,
                    size_bytes,
                }, StreamFormat::Mp4));
            }
        }
        _ => {}
    }

    Ok((StreamMetadata {
        duration_seconds: 0.0,
        total_segments: 1,
        resolutions: vec![ResolutionInfo {
            label: "Unknown Resolution".to_string(),
            bandwidth: 0,
            codecs: None,
            frame_rate: None,
            mime_type: Some("video/mp4".to_string()),
            url: Some(url.to_string()),
        }],
        audio_tracks: Vec::new(),
        keys: Vec::new(),
        drm: Vec::new(),
        segment_base_url: None,
        size_bytes: None,
    }, StreamFormat::Mp4))
}

fn clean_frame_rate(fr: &str) -> String {
    if let Some(pos) = fr.find('/') {
        if let (Ok(num), Ok(den)) = (fr[..pos].parse::<f32>(), fr[pos+1..].parse::<f32>()) {
            if den > 0.0 {
                let val = num / den;
                if val.fract() == 0.0 {
                    return format!("{:.0}", val);
                } else {
                    return format!("{:.2}", val);
                }
            }
        }
    }
    fr.to_string()
}

async fn probe_format_and_parse(
    url: &str,
    headers: HashMap<String, String>,
) -> Result<(StreamMetadata, StreamFormat), Box<dyn std::error::Error + Send + Sync>> {
    let client = wreq::Client::builder()
        .emulation(wreq_util::Emulation::Firefox136)
        .redirect(wreq::redirect::Policy::none())
        .build()?;

    let mut current_url = url.to_string();
    let mut redirects_followed = 0;
    const MAX_REDIRECTS: usize = 10;

    let resp = loop {
        let mut req = client.get(&current_url);
        for (k, v) in &headers {
            let k_lower = k.to_lowercase();
            if k_lower == "host" || k_lower == "accept-encoding" || k_lower == "content-length" || k_lower == "connection" {
                continue;
            }
            if let (Ok(name), Ok(value)) = (wreq::header::HeaderName::from_bytes(k.as_bytes()), wreq::header::HeaderValue::from_str(v)) {
                req = req.header(name, value);
            }
        }
        let r = req.send().await?;
        let status = r.status();
        if status.is_redirection() {
            if redirects_followed >= MAX_REDIRECTS {
                return Err("too many redirects".into());
            }
            if let Some(loc_val) = r.headers().get("location") {
                let loc_str = loc_val.to_str()?;
                let base = url::Url::parse(&current_url)?;
                let next_url = base.join(loc_str)?;
                current_url = next_url.to_string();
                redirects_followed += 1;
                continue;
            }
        }
        if !status.is_success() {
            return Err(format!("HTTP error status: {}", status).into());
        }
        break r;
    };

    let content_type = resp.headers().get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_lowercase();

    if content_type.contains("mpegurl") {
        let body = resp.text().await?;
        let base_url = extract_base_url(url);
        let meta = parse_hls_content(&body, &base_url)?;
        Ok((meta, StreamFormat::Hls))
    } else if content_type.contains("dash+xml") {
        let body = resp.text().await?;
        let base_url = extract_base_url(url);
        let meta = parse_dash_content(&body, &base_url)?;
        Ok((meta, StreamFormat::Dash))
    } else if content_type.contains("video/mp4") || content_type.contains("video/") || content_type.contains("audio/") {
        parse_mp4(url, headers).await
    } else {
        let body = resp.text().await?;
        let base_url = extract_base_url(url);
        if let Ok(meta) = parse_hls_content(&body, &base_url) {
            Ok((meta, StreamFormat::Hls))
        } else if let Ok(meta) = parse_dash_content(&body, &base_url) {
            Ok((meta, StreamFormat::Dash))
        } else {
            Err("unknown format or unsupported media".into())
        }
    }
}
