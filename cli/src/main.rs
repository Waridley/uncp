use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{debug, subscriber::set_global_default, Level};
use tracing_subscriber::EnvFilter;
fn init_tracing(verbosity: u8) {
	// Map -q/-v to tracing levels; default INFO
	let level = match verbosity {
		0 => Level::WARN,
		1 => Level::INFO,
		2 => Level::DEBUG,
		_ => Level::TRACE,
	};

	let env_filter = EnvFilter::from_default_env().add_directive(level.into());

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

use dirs::cache_dir;
use uncp::engine::{BackgroundEngine, EngineCommand, EngineEvent, EngineMode};

use std::fs;
use uncp::{DetectorConfig, DuplicateDetector, PathFilter};

fn default_cache_dir() -> Option<PathBuf> {
	cache_dir().map(|mut p| {
		p.push("uncp");
		p
	})
}
fn ensure_dir(dir: &PathBuf) -> anyhow::Result<()> {
	fs::create_dir_all(dir)?;
	Ok(())
}

fn main() {
	let opts = Opts::parse();
	init_tracing(opts.verbose.saturating_sub(opts.quiet));

	// Initialize rayon thread pool for CPU-bound work
	let nthreads = std::thread::available_parallelism()
		.map(|n| n.get())
		.unwrap_or(4);

	rayon::ThreadPoolBuilder::new()
		.num_threads(nthreads)
		.thread_name(|i| format!("uncp-cli-rayon-{}", i))
		.build_global()
		.expect("Failed to initialize rayon thread pool");

	tracing::info!("Initialized rayon thread pool with {} threads", nthreads);

	smol::block_on(async move {
		if let Err(e) = run(opts).await {
			eprintln!("error: {e}");
			std::process::exit(1);
		}
	});
}

async fn run(opts: Opts) -> anyhow::Result<()> {
	match opts.command {
		Command::Scan { path, hash: _, include, exclude } => {
			debug!(?include, ?exclude, "Scan requested for {}", path.display());
			
			// Create detector config with path filtering
			let mut config = DetectorConfig::default();
			if !include.is_empty() || !exclude.is_empty() {
				config.path_filter = PathFilter::new(include, exclude)?;
			}
			let mut detector = DuplicateDetector::new(config.clone())?;
			let cache_path = default_cache_dir();
			if let Some(dir) = &cache_path {
				ensure_dir(dir)?;
			}
			if let Some(dir) = &cache_path {
				if detector.load_cache_all(dir.clone())? {
					tracing::info!("Loaded cache from {}", dir.display());
				}
			}

			// Foreground CLI: run engine for parallel speedup, print progress to stderr, exit on completion
			let (_engine, events, cmds) = BackgroundEngine::start_with_mode(detector, EngineMode::Cli);
			let _ = cmds.send(EngineCommand::SetPath(path.clone())).await;
			// Clear any cached state to ensure we only scan the specified path
			let _ = cmds.send(EngineCommand::ClearState).await;
			let _ = cmds.send(EngineCommand::Start).await;

			let pref = path.to_string_lossy().to_string();
			while let Ok(evt) = events.recv().await {
				match evt {
					EngineEvent::SnapshotReady(snap) => {
						// Print a compact progress line to stderr (respects global tracing level)
						eprintln!(
							"progress: total={} pending_hash={}",
							snap.total_files, snap.pending_hash
						);
						// Exit early if this requested directory is fully processed
						if snap.pending_hash_under_prefix(&pref) == 0 {
							break;
						}
					}
					EngineEvent::Completed => break,
					_ => {}
				}
			}

			// Save final state to cache and print summary
			if let Some(dir) = default_cache_dir() {
				// Re-load so we have a detector for printing (use same config)
				let mut det = DuplicateDetector::new(config.clone())?;
				det.load_cache_all(dir.clone())?;
				print_summary(&det);
			}
		}

		Command::ClearCache => {
			if let Some(dir) = default_cache_dir() {
				if dir.exists() {
					tracing::warn!("Deleting cache at {}", dir.display());
					fs::remove_dir_all(&dir)?;
					println!("Deleted cache: {}", dir.display());
				} else {
					println!("Cache not found: {}", dir.display());
				}
			} else {
				println!("No cache directory for this platform");
			}
		}
	}
	Ok(())
}

fn print_summary(detector: &DuplicateDetector) {
	println!("Total files: {}", detector.total_files());
	println!("Pending hash: {}", detector.files_pending_hash());
	println!("By type:");
	for (k, v) in detector.files_by_type_counts() {
		println!("  {k}: {v}");
	}
}

#[derive(Parser, Debug)]
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

#[derive(Subcommand, Debug)]
pub enum Command {
	/// Scan a path; optionally hash contents
	Scan {
		/// Path to scan
		path: PathBuf,
		/// Also run hashing
		#[arg(long)]
		hash: bool,
		/// Include only files matching these glob patterns (can be specified multiple times)
		#[arg(long = "include", action = clap::ArgAction::Append)]
		include: Vec<String>,
		/// Exclude files matching these glob patterns (can be specified multiple times)
		#[arg(long = "exclude", action = clap::ArgAction::Append)]
		exclude: Vec<String>,
	},
	/// Delete the local cache directory
	ClearCache,
}
