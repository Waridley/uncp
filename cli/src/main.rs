use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;
use tracing::{Level, subscriber::set_global_default};
fn init_tracing(verbosity: u8) {
    // Map -q/-v to tracing levels; default INFO
    let level = match verbosity {
        0 => Level::WARN,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let env_filter = EnvFilter::from_default_env()
        .add_directive(level.into());

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr) // logs to stderr
        .with_target(false)
        .with_level(true)
        .compact()
        .finish();

    // Ignore error if already set in tests or env
    let _ = set_global_default(subscriber);
}

use uncp::{DuplicateDetector, DetectorConfig};

fn main() {
    let opts = Opts::parse();
    init_tracing(opts.verbose.saturating_sub(opts.quiet));
    smol::block_on(async move {
        if let Err(e) = run(opts).await {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    });
}

async fn run(opts: Opts) -> anyhow::Result<()> {
    match opts.command {
        Command::Scan { path, hash } => {
            let mut detector = DuplicateDetector::new(DetectorConfig::default())?;
            if hash {
                detector.scan_and_hash(path).await?;
            } else {
                detector.scan_directory(path).await?;
            }
            print_summary(&detector);
        }
    }
    Ok(())
}

fn print_summary(detector: &DuplicateDetector) {
    println!("Total files: {}", detector.total_files());
    println!("Pending hash: {}", detector.files_pending_hash());
    println!("By type:");
    for (k, v) in detector.files_by_type_counts() { println!("  {k}: {v}"); }
}

#[derive(Parser)]
#[command(version, about = "uncp CLI (proof-of-concept)")]
pub struct Opts {
    /// Increase verbosity (-v, -vv). Default INFO.
    #[arg(short = 'v', action = clap::ArgAction::Count)]
    pub verbose: u8,
    /// Decrease verbosity (-q). Each -q reduces level by one step.
    #[arg(short = 'q', action = clap::ArgAction::Count)]
    pub quiet: u8,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Scan a path; optionally hash contents
    Scan {
        /// Path to scan
        path: PathBuf,
        /// Also run hashing
        #[arg(long)]
        hash: bool,
    },
}
