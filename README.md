<div align="center">
  <img src="docs/images/tur.png" alt="Tur Logo" width="80" />
  <h1>tur-rs</h1>
  <p>A hyper-fast, highly concurrent download engine built in Rust.</p>
</div>

---

**tur-rs** is a reusable download core that can be embedded as a Rust library or
used via its CLI/TUI binary. It uses adaptive work-stealing, protocol-aware
scheduling, and platform-optimised storage backends to saturate your bandwidth
with a minimal memory footprint (~15 MB).

## Features

| Layer | Capability |
|-------|------------|
| **Protocol** | HTTP/1.1, HTTP/2, HTTP/3 (experimental) with auto-negotiation |
| **Scheduling** | Dynamic range stealing with Fibonacci, equal, or adaptive chunking |
| **Storage** | Aligned I/O, splice (Linux), direct I/O, Windows overlapped |
| **Scaling** | Adaptive connection management from real-time throughput signals |
| **CLI** | Full terminal UI (ratatui) or headless mode for scripting |
| **Library** | Embeddable via `TurService` facade — see `examples/embed.rs` |

## Library embedding

Add `tur-rs` to your `Cargo.toml`:

```toml
[dependencies]
tur-rs = { git = "https://github.com/greykaizen/tur-rs" }
```

Then use the `TurService` facade:

```rust
use tokio::task::LocalSet;
use tur_rs::{TurService, ServiceConfig, DownloadRequest, DownloadUpdate};

let local = LocalSet::new();
local.run_until(async {
    let mut service = TurService::new(ServiceConfig::default()).await?;
    let mut handle = service
        .add_download(DownloadRequest::new("https://example.com/file.zip"))
        .await?;

    while let Some(update) = handle.recv().await {
        match update {
            DownloadUpdate::Progress { downloaded_bytes, speed_bps } =>
                println!("{downloaded_bytes} bytes at {speed_bps:.0} bps"),
            DownloadUpdate::StatusChanged(status) => {
                if matches!(status, tur_rs::DownloadStatus::Completed) { break; }
            }
            _ => {}
        }
    }
    service.shutdown().await;
    Ok::<_, anyhow::Error>(())
}).await;
```

See [`examples/embed.rs`](examples/embed.rs) for a complete runnable example.

## CLI usage

```bash
# Install the binary
cargo install --path .

# Download a file
tur --url https://example.com/file.zip

# With 16 connections, 2 concurrent tasks, in the background
tur -u https://example.com/file.zip -c 16 -t 2 --headless
```

### Core options

| Flag | Description | Default |
|------|-------------|---------|
| `-u, --url <URL>` | Target URL(s) to download | required |
| `-d, --dir <DIR>` | Output directory | `.` |
| `-c, --connections <N>` | Connections per download | `8` |
| `-t, --tasks <N>` | Concurrent downloads | `3` |
| `--headless` | Run without TUI | `false` |
| `--http-mode <MODE>` | Force HTTP mode (`auto`, `http1`, `http2`, `http3`) | `auto` |
| `--schedule-mode <MODE>` | Chunking algorithm (`equal`, `fib`) | `fib` |

Run `tur --help` for the full list.

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tui` | ✓ | Terminal UI (ratatui, crossterm) |
| `http3` | | Experimental HTTP/3 via QUIC |
| `linux-io-uring-experimental` | | Linux io_uring storage |

## Benchmarks

Tur matches the speed of established tools while using significantly less memory.

<div align="center">
  <img src="docs/images/benchmark_speed.png" alt="Download speed benchmark" width="500" />
  <img src="docs/images/benchmark_memory.png" alt="Memory benchmark" width="500" />
</div>

<table>
<tr><th>Metric</th><th>Tur</th><th>aria2c</th><th>Axel</th></tr>
<tr><td>Peak memory</td><td><strong>~10 MB</strong></td><td>~22 MB</td><td>~14 MB</td></tr>
<tr><td>Download time</td><td colspan="3" align="center"><em>parity across real-world WAN tests</em></td></tr>
</table>

[Full benchmark methodology](docs/benchmarks.md)

## Project structure

```
src/
├── lib.rs           # Library crate root (stable API re-exports)
├── main.rs          # Thin binary bootstrap
├── cli.rs           # CLI argument parsing (clap)
├── connector.rs     # Platform-adaptive TCP connector
├── engine.rs        # Core engine: scheduling, scaling, lifecycle
├── engine/          # Engine sub-modules
│   ├── coordinator/ # Range coordination & state tracking
│   ├── http/        # HTTP client construction & protocol helpers
│   ├── metrics/     # Scheduler metrics counters
│   ├── runtime/     # DownloadEngine runtime & event dispatch
│   ├── scaler/      # Adaptive connection scaler
│   ├── types/       # Shared type definitions
│   ├── worker/      # Per-connection worker logic
│   ├── helpers.rs   # Statistical & helper functions
│   ├── origin_memory.rs
│   ├── persistence.rs
│   └── ranges.rs
├── quic.rs          # HTTP/3 client (feature-gated)
├── service.rs       # TurService facade for library embedding
├── storage.rs       # Platform storage backends
└── tui/             # Terminal UI (feature-gated)
    ├── app.rs
    ├── input.rs
    └── render.rs
```

## Stability

The types re-exported from the crate root (`TurService`, `ServiceConfig`,
`DownloadRequest`, `DownloadHandle`, `DownloadUpdate`, `DownloadStatus`,
`HttpMode`, `ScheduleMode`, `StorageConfig`) form the **stable public API**
and follow semantic versioning.

Internal modules (`engine`, `connector`, `quic`, `storage`, `cli`, `tui`) are
exposed for advanced use but may change between minor releases.

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE).
