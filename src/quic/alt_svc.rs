use http::HeaderMap;

pub fn parse_alt_svc_h3_port(headers: &HeaderMap) -> Option<u16> {
    let alt_svc = headers.get("alt-svc")?.to_str().ok()?;
    for token in alt_svc.split(',') {
        let token = token.trim();
        if token.starts_with("h3") && token.contains("\":") {
            if let Some(start) = token.find("\":") {
                let rest = &token[start + 2..];
                if let Some(end) = rest.find('"') {
                    return rest[..end].parse::<u16>().ok();
                }
            }
        }
    }
    None
}
