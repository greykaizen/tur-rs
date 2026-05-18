use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "tur", author, version, about, long_about = None)]
pub struct Cli {
    /// URL(s) to download
    #[arg(short, long, num_args = 1..)]
    pub url: Vec<String>,

    /// Download directory
    #[arg(short, long, default_value = ".")]
    pub dir: String,

    /// Initial connections per download
    #[arg(short, long)]
    pub connections: Option<usize>,

    /// Per-download minimum connection count for autonomous scaling
    #[arg(long)]
    pub min_connections: Option<usize>,

    /// Per-download maximum connection count for autonomous scaling
    #[arg(long)]
    pub max_connections: Option<usize>,

    /// Concurrent downloads (tasks)
    #[arg(short, long, default_value_t = 3)]
    pub tasks: usize,

    /// Run the scheduler without issuing range GET requests
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Synthetic size for dry runs, in MiB
    #[arg(long)]
    pub dry_run_size_mb: Option<u64>,

    /// Borrow stop threshold, in MiB
    #[arg(long, default_value_t = 2)]
    pub borrow_limit_mb: u64,

    /// Scheduler mode: equal or fib
    #[arg(long, default_value = "equal")]
    pub schedule_mode: String,

    /// HTTP transport mode: auto, http1, or http2
    #[arg(long, default_value = "http1")]
    pub http_mode: String,

    /// Run without the TUI and exit when tasks finish
    #[arg(long, default_value_t = false)]
    pub headless: bool,

    /// Root directory for engine logs and metadata
    #[arg(long)]
    pub log_root: Option<String>,

    /// Disable persisted origin behavior memory
    #[arg(long, default_value_t = false)]
    pub no_origin_memory: bool,

    /// System-wide ceiling for active download connections
    #[arg(long, default_value_t = 32)]
    pub max_total_connections: usize,

    /// Global bandwidth cap in Mbps, 0 disables the cap
    #[arg(long, default_value_t = 0)]
    pub bandwidth_limit: u64,

    /// Per-download bandwidth cap in Mbps, 0 disables the cap
    #[arg(long, default_value_t = 0)]
    pub per_download_limit: u64,

    /// Disable Linux pwrite zero-copy write path
    #[arg(long = "no-pwrite", alias = "no-splice", default_value_t = false)]
    pub no_pwrite: bool,

    /// Disable Linux kernel splice() zero-copy write path (pipe+splice)
    #[arg(long, default_value_t = false)]
    pub no_splice: bool,

    /// Disable Linux io_uring write path (experimental)
    #[arg(long, default_value_t = false)]
    pub no_io_uring: bool,

    /// Disable Direct I/O (FILE_FLAG_NO_BUFFERING) on Windows; has no effect on other platforms
    #[arg(long, default_value_t = false)]
    pub no_direct_io: bool,

    /// Number of worker threads for the async runtime; 1 = single-threaded
    #[arg(long, default_value_t = 1)]
    pub runtime_threads: usize,
}
