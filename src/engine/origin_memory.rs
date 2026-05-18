use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum H2TuningSource {
    Default,
    LearnedOrigin,
    OriginMemoryHint,
}

impl H2TuningSource {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::LearnedOrigin => "learned-origin",
            Self::OriginMemoryHint => "origin-memory-hint",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct ClientTuning {
    pub expected_concurrency: usize,
    pub http2_stream_window_bytes: u32,
    pub http2_connection_window_bytes: u32,
    pub http2_max_send_buffer_bytes: usize,
    pub source: H2TuningSource,
}

#[derive(Debug, Clone, Copy)]
struct OriginH2TuningEntry {
    tuning: ClientTuning,
    last_used_tick: u64,
}

#[derive(Debug, Default)]
pub(crate) struct OriginH2TuningStore {
    entries: HashMap<String, OriginH2TuningEntry>,
    usage_tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedOriginProfile {
    phi_ratio: Option<f64>,
    h2_tuning: Option<ClientTuning>,
    protocol_hint: Option<ProtocolFamily>,
    reuse_rate: Option<f64>,
    handshake_ms: Option<f64>,
    supports_ranges: Option<bool>,
    content_length_reliable: Option<bool>,
    saw_rate_limit: bool,
    cookies_used: Option<bool>,
    auth_used: Option<bool>,
    referer_used: Option<bool>,
    challenge_detected: Option<bool>,
    challenge_kind: Option<String>,
    last_used_tick: u64,
}

#[derive(Debug, Default)]
pub(crate) struct OriginMemoryStore {
    entries: HashMap<String, PersistedOriginProfile>,
    usage_tick: u64,
    enabled: bool,
}

impl OriginMemoryStore {
    pub(super) fn load_enabled(enabled: bool) -> Self {
        if !enabled {
            return Self {
                entries: HashMap::new(),
                usage_tick: 0,
                enabled: false,
            };
        }
        let path = origin_memory_path();
        let Ok(bytes) = fs::read(path) else {
            return Self {
                entries: HashMap::new(),
                usage_tick: 0,
                enabled: true,
            };
        };
        let Ok(entries) = bincode::deserialize::<HashMap<String, PersistedOriginProfile>>(&bytes) else {
            return Self {
                entries: HashMap::new(),
                usage_tick: 0,
                enabled: true,
            };
        };
        let usage_tick = entries
            .values()
            .map(|profile| profile.last_used_tick)
            .max()
            .unwrap_or(0);
        Self {
            entries,
            usage_tick,
            enabled: true,
        }
    }

    fn persist(&self) {
        if !self.enabled {
            return;
        }
        let path = origin_memory_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(bytes) = bincode::serialize(&self.entries) {
            let _ = fs::write(path, bytes);
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.usage_tick = self.usage_tick.saturating_add(1);
        self.usage_tick
    }

    fn touch_profile(&mut self, origin: &str) -> &mut PersistedOriginProfile {
        let tick = self.next_tick();
        let profile = self.entries.entry(origin.to_string()).or_insert(PersistedOriginProfile {
            phi_ratio: None,
            h2_tuning: None,
            protocol_hint: None,
            reuse_rate: None,
            handshake_ms: None,
            supports_ranges: None,
            content_length_reliable: None,
            saw_rate_limit: false,
            cookies_used: None,
            auth_used: None,
            referer_used: None,
            challenge_detected: None,
            challenge_kind: None,
            last_used_tick: tick,
        });
        profile.last_used_tick = tick;
        profile
    }

    pub(super) fn protocol_hint_for_origin(&mut self, origin: &str) -> Option<ProtocolFamily> {
        if !self.enabled {
            return None;
        }
        self.touch_profile(origin).protocol_hint
    }

    pub(super) fn note_protocol(&mut self, origin: &str, protocol: ProtocolFamily) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).protocol_hint = Some(protocol);
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_phi_ratio(&mut self, origin: &str, ratio: f64) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).phi_ratio = Some(ratio);
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_h2_tuning(&mut self, origin: &str, tuning: ClientTuning) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).h2_tuning = Some(tuning);
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_reuse_metrics(&mut self, origin: &str, reuse_rate: f64, handshake_ms: f64) {
        if !self.enabled {
            return;
        }
        let profile = self.touch_profile(origin);
        profile.reuse_rate = Some(reuse_rate);
        profile.handshake_ms = Some(handshake_ms);
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_range_support(&mut self, origin: &str, supports_ranges: bool) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).supports_ranges = Some(supports_ranges);
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_content_length_reliable(&mut self, origin: &str, reliable: bool) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).content_length_reliable = Some(reliable);
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_rate_limit(&mut self, origin: &str) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).saw_rate_limit = true;
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_session_requirement(
        &mut self,
        origin: &str,
        field: SessionRequirementField,
        used: bool,
    ) {
        if !self.enabled {
            return;
        }
        let profile = self.touch_profile(origin);
        match field {
            SessionRequirementField::Cookies => profile.cookies_used = Some(used),
            SessionRequirementField::Auth => profile.auth_used = Some(used),
            SessionRequirementField::Referer => profile.referer_used = Some(used),
        }
        self.prune_lru();
        self.persist();
    }

    pub(super) fn note_challenge_detected(
        &mut self,
        origin: &str,
        kind: Option<super::ChallengeKind>,
    ) {
        if !self.enabled {
            return;
        }
        let profile = self.touch_profile(origin);
        profile.challenge_detected = Some(kind.is_some());
        profile.challenge_kind = kind.map(|k| k.as_str().to_string());
        self.prune_lru();
        self.persist();
    }

    pub(super) fn session_info_for_origin(&self, origin: &str) -> Option<SessionMemoryInfo> {
        if !self.enabled {
            return None;
        }
        self.entries.get(origin).map(|p| SessionMemoryInfo {
            cookies_used: p.cookies_used,
            auth_used: p.auth_used,
            referer_used: p.referer_used,
            challenge_detected: p.challenge_detected,
            challenge_kind: p.challenge_kind.clone(),
            saw_rate_limit: p.saw_rate_limit,
        })
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > ORIGIN_MEMORY_CAPACITY {
            let Some(lru_key) = self
                .entries
                .iter()
                .min_by_key(|(_, profile)| profile.last_used_tick)
                .map(|(origin, _)| origin.clone())
            else {
                break;
            };
            self.entries.remove(&lru_key);
        }
    }

    pub(super) fn hydrate_phi_ratios(&self) -> OriginPhiRatioStore {
        let mut store = OriginPhiRatioStore::default();
        for (origin, profile) in &self.entries {
            if let Some(phi_ratio) = profile.phi_ratio {
                store.update_origin_ratio(origin.clone(), phi_ratio);
            }
        }
        store.usage_tick = self.usage_tick;
        store
    }

    pub(super) fn hydrate_h2_tunings(&self) -> OriginH2TuningStore {
        let mut store = OriginH2TuningStore::default();
        for (origin, profile) in &self.entries {
            if let Some(mut tuning) = profile.h2_tuning {
                tuning.source = H2TuningSource::OriginMemoryHint;
                store.entries.insert(
                    origin.clone(),
                    OriginH2TuningEntry {
                        tuning,
                        last_used_tick: profile.last_used_tick,
                    },
                );
            }
        }
        store.usage_tick = self.usage_tick;
        store
    }

    pub(super) fn memory_hit_for_origin(&self, origin: &str) -> bool {
        self.enabled && self.entries.contains_key(origin)
    }
}

/// A field of session requirement that can be remembered per-origin.
pub(super) enum SessionRequirementField {
    Cookies,
    Auth,
    Referer,
}

/// Summary of session memory for a single origin.
pub(super) struct SessionMemoryInfo {
    pub cookies_used: Option<bool>,
    pub auth_used: Option<bool>,
    pub referer_used: Option<bool>,
    pub challenge_detected: Option<bool>,
    pub challenge_kind: Option<String>,
    pub saw_rate_limit: bool,
}

impl OriginH2TuningStore {
    fn next_tick(&mut self) -> u64 {
        self.usage_tick = self.usage_tick.saturating_add(1);
        self.usage_tick
    }

    pub(super) fn tuning_for_origin(&mut self, origin: &str) -> Option<ClientTuning> {
        let tick = self.next_tick();
        self.entries.get_mut(origin).map(|entry| {
            entry.last_used_tick = tick;
            entry.tuning
        })
    }

    pub(super) fn update_origin_tuning(&mut self, origin: String, tuning: ClientTuning) {
        let tick = self.next_tick();
        self.entries.insert(
            origin,
            OriginH2TuningEntry {
                tuning,
                last_used_tick: tick,
            },
        );
        self.prune_lru();
    }

    #[cfg(test)]
    pub(crate) fn current_tuning(&self, origin: &str) -> Option<ClientTuning> {
        self.entries.get(origin).map(|entry| entry.tuning)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > ORIGIN_PHI_RATIO_CAPACITY {
            let Some(lru_key) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used_tick)
                .map(|(origin, _)| origin.clone())
            else {
                break;
            };
            self.entries.remove(&lru_key);
        }
    }
}

pub(super) fn origin_key(url: &str) -> String {
    if let Ok(parsed) = Url::parse(url) {
        let scheme = parsed.scheme();
        let host = parsed.host_str().unwrap_or(url);
        let port = parsed.port_or_known_default().unwrap_or_default();
        return format!("{scheme}://{host}:{port}");
    }
    url.to_string()
}

fn origin_memory_path() -> PathBuf {
    if let Ok(xdg_cache_home) = std::env::var("XDG_CACHE_HOME") {
        return PathBuf::from(xdg_cache_home).join("tur").join("origin-memory.bin");
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join(".cache")
            .join("tur")
            .join("origin-memory.bin");
    }
    PathBuf::from(".tur-origin-memory.bin")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_memory_persists_across_reload() {
        let base = std::env::temp_dir().join(format!("tur-origin-memory-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&base).unwrap();
        unsafe {
            std::env::set_var("XDG_CACHE_HOME", &base);
        }

        let origin = "https://example.com:443";
        let tuning = ClientTuning {
            expected_concurrency: 4,
            http2_stream_window_bytes: 4 * MB as u32,
            http2_connection_window_bytes: 16 * MB as u32,
            http2_max_send_buffer_bytes: 2 * MB as usize,
            source: H2TuningSource::LearnedOrigin,
        };

        let mut store = OriginMemoryStore::load_enabled(true);
        store.note_protocol(origin, ProtocolFamily::Http2);
        store.note_phi_ratio(origin, 1.7);
        store.note_h2_tuning(origin, tuning);

        let mut reloaded = OriginMemoryStore::load_enabled(true);
        assert_eq!(reloaded.protocol_hint_for_origin(origin), Some(ProtocolFamily::Http2));
        assert_eq!(
            reloaded.hydrate_phi_ratios().current_ratio(origin),
            Some(1.7)
        );
        assert_eq!(
            reloaded
                .hydrate_h2_tunings()
                .current_tuning(origin)
                .unwrap()
                .source,
            H2TuningSource::OriginMemoryHint
        );

        let _ = std::fs::remove_dir_all(base);
        unsafe {
            std::env::remove_var("XDG_CACHE_HOME");
        }
    }
}
