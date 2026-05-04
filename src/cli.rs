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
    #[arg(short, long)]
    pub threads: Option<usize>,
}
