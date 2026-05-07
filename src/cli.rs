use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// URL(s) to download
    #[arg(short, long, num_args = 1..)]
    pub url: Vec<String>,

    /// Download directory
    #[arg(short, long, default_value = ".")]
    pub dir: String,

    /// Connections per download
    #[arg(short, long, default_value_t = 8)]
    pub connections: usize,

    /// Concurrent downloads (tasks)
    #[arg(short, long, default_value_t = 3)]
    pub tasks: usize,

    /// Max threads for the engine pool
    #[arg(long)]
    pub threads: Option<usize>,

    /// Run the scheduler without issuing range GET requests
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Synthetic size for dry runs, in MiB
    #[arg(long)]
    pub dry_run_size_mb: Option<u64>,

    /// Borrow stop threshold, in MiB
    #[arg(long, default_value_t = 2)]
    pub borrow_limit_mb: u64,

    /// Run without the TUI and exit when tasks finish
    #[arg(long, default_value_t = false)]
    pub headless: bool,

    /// Root directory for engine logs and metadata
    #[arg(long)]
    pub log_root: Option<String>,
}
