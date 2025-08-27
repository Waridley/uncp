use tiny_bail::prelude::c;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{debug, subscriber::set_global_default, Level};
use tracing_subscriber::EnvFilter;
fn init_tracing(verbosity: i8) {
	// Map -q/-v to tracing levels; default INFO
	let level = match verbosity {
		..=-2 => Level::ERROR,
		-1 => Level::WARN,
		0 => Level::INFO,
		1 => Level::DEBUG,
		2.. => Level::TRACE,
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

fn main() -> anyhow::Result<()> {
	let opts = Opts::parse();
	init_tracing(opts.verbose as i8 - opts.quiet as i8);

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
		run(opts).await
	})
}

async fn run(opts: Opts) -> anyhow::Result<()> {
	match opts.command {
		Command::Scan {
			path,
			hash: _,
			include,
			exclude,
		} => {
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
			let (_engine, events, cmds) =
				BackgroundEngine::start_with_mode(detector, EngineMode::Cli);
			let _ = cmds.send(EngineCommand::SetPath(path.clone())).await;
			// Clear any cached state to ensure we only scan the specified path
			let _ = cmds.send(EngineCommand::ClearState).await;
			let _ = cmds.send(EngineCommand::Start).await;

			let pref = path.to_string_lossy().to_string();
			while let Ok(evt) = events.recv().await {
				match evt {
					EngineEvent::SnapshotReady(snap) => {
						// Print a compact progress line using tracing
						tracing::info!(
							"progress: total={} pending_hash={}",
							snap.total_files,
							snap.pending_hash
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

		Command::Interactive {
			path,
			include,
			exclude,
		} => {
			debug!(?include, ?exclude, "Interactive mode requested for {}", path.display());

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

			// Interactive mode: keep engine running until user quits
			let (_engine, events, cmds) =
				BackgroundEngine::start_with_mode(detector, EngineMode::Interactive);
			let _ = cmds.send(EngineCommand::SetPath(path.clone())).await;
			let _ = cmds.send(EngineCommand::ClearState).await;
			let _ = cmds.send(EngineCommand::Start).await;

			run_interactive_mode(events, cmds, path).await?;
		}

		Command::ClearCache => {
			if let Some(dir) = default_cache_dir() {
				if dir.exists() {
					tracing::warn!("Deleting cache at {}", dir.display());
					fs::remove_dir_all(&dir)?;
					tracing::info!("Deleted cache: {}", dir.display());
				} else {
					tracing::info!("Cache not found: {}", dir.display());
				}
			} else {
				tracing::info!("No cache directory for this platform");
			}
		}
	}
	Ok(())
}

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use crossterm::event::{KeyCode, KeyModifiers};

async fn run_interactive_mode(
	events: smol::channel::Receiver<EngineEvent>,
	cmds: smol::channel::Sender<EngineCommand>,
	path: PathBuf,
) -> anyhow::Result<()> {
	
	tracing::info!("Starting interactive mode for path: {}", path.display());
	tracing::info!("Engine will keep running until Ctrl+C is pressed");
	tracing::info!("This mode is useful for engine testing without a UI");
	
	let should_quit = Arc::new(AtomicBool::new(false));
	
	{
		// Set up Ctrl+C handler
		let should_quit = should_quit.clone();
		ctrlc::set_handler(move || {
			tracing::info!("Received termination signal, shutting down gracefully...");
			should_quit.store(true, Ordering::Relaxed);
			
			// TODO: Remove this when we have proper Ctrl+C handling
			tracing::warn!("Actually, exiting abruptly for now until Q/Esc quitting is at least working, so I don't have to keep closing the terminal.");
			std::process::exit(1);
		})?;
	}
	
	crossterm::terminal::enable_raw_mode()?;
	interactive_loop(events, cmds, should_quit).await?;
	crossterm::terminal::disable_raw_mode()?;
	Ok(())
}

async fn interactive_loop(
	events: smol::channel::Receiver<EngineEvent>,
	cmds: smol::channel::Sender<EngineCommand>,
	should_quit: Arc<AtomicBool>,
) -> anyhow::Result<()> {
	// Main event processing loop
	let mut last_progress_time = std::time::Instant::now();

	loop {
		if crossterm::event::poll(Duration::ZERO)? {
			match crossterm::event::read()? {
				crossterm::event::Event::Key(key) => {
					if key.is_press() {
						match key.code {
							KeyCode::Char('q') | KeyCode::Esc => {
								tracing::info ! ("Quit requested");
								should_quit.store(true, Ordering::Relaxed);
							}
							KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
								tracing::info!("Ctrl+C requested");
								should_quit.store(true, Ordering::Relaxed);
								
								// TODO: Remove this when we have proper Ctrl+C handling
								tracing::warn!("Actually, exiting abruptly for now until Q/Esc quitting is at least working, so I don't have to keep closing the terminal.");
								std::process::exit(1);
							}
							_ => {}
						}
					}
				}
				other => tracing::trace!("Other event: {other:?}"),
			}
		}
		
		if should_quit.load(Ordering::Relaxed) {
			tracing::info!("Stopping engine...");
			c!(cmds.send(EngineCommand::Stop).await);
			// Don't keep spamming events
			should_quit.store(false, Ordering::Relaxed);
			continue;
		}

		match events.try_recv() {
			Ok(evt) => {
				match evt {
					EngineEvent::SnapshotReady(snap) => {
						// Print progress periodically (every 2 seconds)
						let now = std::time::Instant::now();
						if now.duration_since(last_progress_time) >= std::time::Duration::from_secs(2) {
							tracing::info!(
								"progress: total={} pending_hash={}",
								snap.total_files,
								snap.pending_hash
							);
							last_progress_time = now;
						}
					}
					EngineEvent::Completed => {
						tracing::info!("Engine completed processing");
					}
					EngineEvent::Error(err) => {
						tracing::error!("Engine error: {}", err);
					}
					EngineEvent::Stopped => {
						tracing::info!("Engine stopped");
						break;
					}
					other => tracing::trace!("Other engine event: {other:?}"),
				}
			}
			Err(smol::channel::TryRecvError::Empty) => {
				tracing::trace!("No events available");
			}
			Err(smol::channel::TryRecvError::Closed) => {
				// Channel closed, engine stopped
				tracing::info!("Engine channel closed");
				break;
			}
		}

		// Small delay to prevent busy waiting
		smol::Timer::after(std::time::Duration::from_millis(100)).await;
	}

	tracing::info!("Interactive mode shutdown complete");
	Ok(())
}

fn print_summary(detector: &DuplicateDetector) {
	tracing::info!("Total files: {}", detector.total_files());
	tracing::info!("Pending hash: {}", detector.files_pending_hash());
	tracing::info!("By type:");
	for (k, v) in detector.files_by_type_counts() {
		tracing::info!("  {k}: {v}");
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
	/// Keep the engine running interactively for testing the engine separately from any UI
	Interactive {
		/// Path to scan
		path: PathBuf,
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
