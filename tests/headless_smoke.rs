use std::process::Command;

#[test]
fn headless_dry_run_supports_runtime_threads_flag() {
    let out_dir = std::env::temp_dir().join(format!("tur-headless-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&out_dir).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_tur"))
        .arg("--headless")
        .arg("--dry-run")
        .arg("--dry-run-size-mb")
        .arg("8")
        .arg("--runtime-threads")
        .arg("2")
        .arg("--url")
        .arg("https://example.com/a")
        .arg("--dir")
        .arg(&out_dir)
        .status()
        .unwrap();

    assert!(status.success());
    let _ = std::fs::remove_dir_all(out_dir);
}

#[test]
fn headless_multi_task_dry_run_completes() {
    let out_dir = std::env::temp_dir().join(format!("tur-headless-multi-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&out_dir).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_tur"))
        .arg("--headless")
        .arg("--dry-run")
        .arg("--dry-run-size-mb")
        .arg("8")
        .arg("--tasks")
        .arg("2")
        .arg("--url")
        .arg("https://example.com/a")
        .arg("--url")
        .arg("https://example.com/b")
        .arg("--dir")
        .arg(&out_dir)
        .status()
        .unwrap();

    assert!(status.success());
    let _ = std::fs::remove_dir_all(out_dir);
}
