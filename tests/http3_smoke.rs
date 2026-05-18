#[cfg(feature = "http3")]
#[test]
#[ignore = "requires a reachable HTTP/3 origin via TUR_H3_TEST_URL"]
fn headless_http3_range_download_smoke() {
    let Some(url) = std::env::var_os("TUR_H3_TEST_URL") else {
        return;
    };

    let out_dir = std::env::temp_dir().join(format!("tur-h3-smoke-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&out_dir).unwrap();

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_tur"))
        .arg("--headless")
        .arg("--url")
        .arg(url)
        .arg("--http-mode")
        .arg("http3")
        .arg("--connections")
        .arg("1")
        .arg("--min-connections")
        .arg("1")
        .arg("--max-connections")
        .arg("1")
        .arg("--dir")
        .arg(&out_dir)
        .status()
        .unwrap();

    assert!(status.success());
    let _ = std::fs::remove_dir_all(out_dir);
}
