use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, IdleTimeout};
use rustls::{ClientConfig as TlsConfig, RootCertStore};

pub fn build_tls_config() -> Result<TlsConfig> {
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut crypto = TlsConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    Ok(crypto)
}

pub fn build_quic_endpoint() -> Result<quinn::Endpoint> {
    let crypto = build_tls_config()?;
    let quic_crypto = QuicClientConfig::try_from(Arc::new(crypto))?;
    let mut client_config = ClientConfig::new(Arc::new(quic_crypto));
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_idle_timeout(Some(
        IdleTimeout::try_from(Duration::from_secs(30))
            .map_err(|_| anyhow!("invalid idle timeout"))?,
    ));
    client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap())?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}
