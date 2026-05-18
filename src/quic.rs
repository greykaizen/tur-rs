mod alt_svc;
#[cfg(feature = "http3")]
mod cache;

#[cfg(feature = "http3")]
mod client;
mod stream;

#[cfg(not(feature = "http3"))]
mod stub;

pub use alt_svc::parse_alt_svc_h3_port;

#[cfg(feature = "http3")]
pub use client::*;

#[cfg(not(feature = "http3"))]
pub use stub::*;

pub use stream::H3Response;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_alt_svc_h3_port() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ALT_SVC,
            "h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400"
                .parse()
                .unwrap(),
        );
        assert_eq!(parse_alt_svc_h3_port(&headers), Some(443));

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ALT_SVC,
            "h3=\":8443\"; ma=3600".parse().unwrap(),
        );
        assert_eq!(parse_alt_svc_h3_port(&headers), Some(8443));

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ALT_SVC,
            "h2=\":443\"; ma=86400".parse().unwrap(),
        );
        assert_eq!(parse_alt_svc_h3_port(&headers), None);
    }
}
