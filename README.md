<div align="center">
  <img src="docs/images/tur.png" alt="Tur Logo" width="80" />
  <h1>Tur</h1>
  <p><strong>A hyper-fast, highly concurrent download manager built in Rust.</strong></p>
</div>

---

## 🎯 Purpose & Inspiration

**Tur** is heavily inspired by `aria2c`, an incredible and robust tool that has served the community for years as the gold standard for high-speed downloads. We built Tur to explore how those proven concurrent downloading concepts could be implemented using the modern Rust asynchronous ecosystem (`tokio` and `hyper`).

By leveraging Rust, Tur aims to achieve:
- **A Lean Footprint:** Utilizing Rust's zero-cost abstractions to maintain a small codebase and low memory footprint.
- **Relentless Saturation:** An aggressive "range borrowing" scheduler ensures that if one TCP connection finishes its chunk early, it instantly steals work from slower connections, maximizing throughput.
- **Raw Network Speed:** Built directly on `hyper`, giving us fine-grained control over the HTTP/1.1 and HTTP/2 transport layers for optimal performance.

## ✨ Features

- **Concurrent Chunking:** Splits single files into dynamic byte ranges to bypass per-connection speed limits.
- **Advanced Schedulers:** Supports `equal`, `fib`, and the experimental `fib-adaptive` mode for dynamic work stealing.
- **Protocol Flexibility:** Native HTTP/1.1 and HTTP/2 support with auto-negotiation.
- **Beautiful TUI:** A responsive, real-time terminal user interface powered by `ratatui` (headless mode also available).
- **Dry-Run Profiling:** Built-in tools to benchmark scheduler logic and connection establishment without writing to disk.

## 📈 Benchmark Comparison

Tur is designed to be highly competitive with established C/C++ downloaders. In our testing, Tur consistently matches or exceeds the performance of `aria2c` and `axel` while maintaining a significantly lower memory footprint.

### Methodology & Results
The following data represents a snapshot from a **"Full Tournament"** benchmark run.

**Environment:**
- **Date:** 2026-05-09
- **OS:** Linux (x86_64)
- **Network:** ~20-30 Mbps Real-world WAN
- **Artifacts:** Rust 1.86.0 Source (~351 MB), VSCode/VLC Binaries.
- **Configuration:** 4-8 connections, `fib-adaptive` mode, `http1`.

<div align="center">
  <h4>Download Performance (Time)</h4>
  <img src="docs/images/benchmark_speed.png" alt="Download Time Performance" width="600" />
  <p><em>Tur is competitively aligned with Axel and aria2c, often finishing in the same performance bracket or slightly ahead.</em></p>
</div>

<div align="center">
  <h4>Peak Memory Footprint (RSS)</h4>
  <img src="docs/images/benchmark_memory.png" alt="Memory Efficiency" width="600" />
  <p><em>Tur maintains a ~35-65% lower memory footprint than aria2c, even when utilizing large 1MB write buffers.</em></p>
</div>

**Why Tur is Competitive:**
- **Speed-Aware Stealing:** The scheduler dynamically monitors connection health and reallocates ranges from "stragglers" to faster workers.
- **Zero-Copy Buffering:** A 1MB per-worker write buffer reduces system call frequency and context switching overhead.
- **Asynchronous Runtime:** Leveraging `tokio` and `hyper` for non-blocking I/O with minimal thread-management overhead.

## 🚀 Installation

Ensure you have [Rust and Cargo](https://rustup.rs/) installed, then clone the repository and build:

```bash
cargo build --release
```

The optimized executable will be located at `target/release/tur`.

## 🛠️ Usage

```bash
tur [OPTIONS] --url <URL>...
```

### Core Options

- `-u, --url <URL>...`: Target URL(s) to download.
- `-d, --dir <DIR>`: Output directory (default: `.`).
- `-c, --connections <CONNECTIONS>`: Number of concurrent TCP connections per file (default: `8`).
- `-t, --tasks <TASKS>`: Number of files to download simultaneously (default: `3`).
- `--headless`: Run in the background without the graphical TUI.

### Advanced Tuning

- `--schedule-mode <MODE>`: Range chunking algorithm (`equal` or `fib`).
- `--http-mode <MODE>`: Force HTTP transport mode (`auto`, `http1`, or `http2`).
- `--borrow-limit-mb <MB>`: The minimum megabytes left in a chunk before another worker is allowed to steal from it (default: `2`).
- `--threads <THREADS>`: Max OS threads for the asynchronous runtime pool.
- `--dry-run`: Run the entire network handshake and scheduling loop without saving data.

### Examples

**Max speed download (16 connections) to a specific folder:**
```bash
tur -u https://example.com/large-dataset.iso -c 16 -d ~/Downloads
```

**Scripting/Headless mode:**
```bash
tur -u https://example.com/update.tar.gz --headless
```

## 🤝 Contribution

Contributions are more than welcome! Whether it's optimizing the `hyper` pipeline, adding new TUI widgets, or fixing bugs:
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/blazing-fast-io`).
3. Commit your changes (`git commit -m 'Add blazing fast IO'`).
4. Push to the branch (`git push origin feature/blazing-fast-io`).
5. Open a Pull Request.

Please ensure your code passes standard Rust formatting (`cargo fmt`) and linting (`cargo clippy`).

## 📄 License

This project is open-source and available under the [GNU General Public License v3.0](LICENSE).
