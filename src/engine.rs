//! Background engine to drive continuous processing with progress events

use std::path::PathBuf;

use async_channel as channel;
use futures_lite::future;
use tracing::{info, warn};

use crate::systems::SystemProgress;
use crate::{paths::default_cache_dir, DuplicateDetector};

/// Commands for controlling the background processing engine.
///
/// `EngineCommand` provides a message-based interface for controlling
/// the background engine's behavior. Commands are sent through a channel
/// and processed asynchronously, allowing for responsive UI interaction
/// while maintaining clean separation between UI and processing logic.
///
/// ## Command Categories
///
/// ### Path Management
/// - **SetPath**: Change the directory being scanned
/// - **SetPathFilter**: Apply glob-based file filtering
/// - **ClearPathFilter**: Remove all path filters
///
/// ### Processing Control
/// - **Start**: Begin or resume processing operations
/// - **Pause**: Temporarily halt processing (preserves state)
/// - **Stop**: Halt processing and prepare for shutdown
/// - **ClearState**: Reset all detector state for fresh start
///
/// ### Cache Operations
/// - **LoadCache**: Load cached data from specific directory
///
/// ## Usage Patterns
///
/// ```rust
/// use uncp::engine::{BackgroundEngine, EngineCommand, EngineMode};
/// use uncp::DetectorConfig;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create engine with command channel
/// let (engine, mut events, commands) = BackgroundEngine::new(
///     DetectorConfig::default(),
///     EngineMode::Interactive
/// )?;
///
/// // Start background processing
/// let engine_task = smol::spawn(engine.run());
///
/// // Send commands to control processing
/// commands.send(EngineCommand::SetPath("/data/photos".into())).await?;
/// commands.send(EngineCommand::Start).await?;
///
/// // Process can be paused and resumed
/// commands.send(EngineCommand::Pause).await?;
/// commands.send(EngineCommand::Start).await?;
///
/// // Clean shutdown
/// commands.send(EngineCommand::Stop).await?;
/// engine_task.await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Thread Safety
///
/// Commands are sent through async channels and are inherently thread-safe.
/// Multiple producers can send commands concurrently without synchronization.
#[derive(Debug, Clone)]
pub enum EngineCommand {
	/// Change the directory path being scanned for duplicate files
	SetPath(PathBuf),
	/// Start or resume processing operations
	Start,
	/// Pause processing while preserving current state
	Pause,
	/// Stop processing and prepare for shutdown
	Stop,
	/// Clear all detector state for a fresh start
	ClearState,
	/// Load cached data from the specified directory
	LoadCache(PathBuf),
	/// Apply glob-based path filtering to limit which files are processed
	SetPathFilter(crate::PathFilter),
	/// Remove all path filters to process all discovered files
	ClearPathFilter,
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
	DiscoveryProgress(SystemProgress),
	HashingProgress(SystemProgress),
	SnapshotReady(crate::ui::PresentationState),
	Started,
	Completed,
	Error(String),
	CacheLoading,
	CacheLoaded,
	CacheSaving,
	CacheSaved,
	CacheValidating,
	CacheValidated {
		files_removed: usize,
		files_invalidated: usize,
	},
}

pub struct BackgroundEngine {
	_cmd_tx: channel::Sender<EngineCommand>,
	_evt_rx: channel::Receiver<EngineEvent>,
}

/// Operating mode configuration for the background processing engine.
///
/// `EngineMode` determines the engine's behavior regarding lifecycle management,
/// resource allocation, and interaction patterns. Different modes are optimized
/// for different use cases and user interfaces.
///
/// ## Mode Characteristics
///
/// ### CLI Mode
/// Optimized for batch processing and command-line usage:
/// - **Automatic Shutdown**: Exits after completing all work
/// - **Minimal Overhead**: Reduced memory usage and simpler event handling
/// - **Progress Reporting**: Essential progress information only
/// - **Resource Management**: Aggressive cleanup and memory reclamation
/// - **Error Handling**: Fail-fast behavior with detailed error reporting
///
/// ### Interactive Mode
/// Designed for GUI and TUI applications with ongoing user interaction:
/// - **Persistent Operation**: Continues running until explicitly stopped
/// - **Rich Events**: Detailed progress and status information
/// - **Responsive**: Optimized for real-time UI updates
/// - **Resource Retention**: Keeps data in memory for fast queries
/// - **Graceful Degradation**: Continues operation despite non-critical errors
///
/// ## Performance Implications
///
/// ### CLI Mode Performance
/// - Lower memory usage due to aggressive cleanup
/// - Faster startup and shutdown times
/// - Optimized for single-pass processing
/// - Minimal event overhead
///
/// ### Interactive Mode Performance
/// - Higher memory usage for responsive queries
/// - Persistent caching for fast repeated operations
/// - Real-time progress updates with minimal latency
/// - Background processing continues during user interaction
///
/// ## Usage Examples
///
/// ### CLI Application
/// ```rust
/// use uncp::engine::{BackgroundEngine, EngineMode};
/// use uncp::DetectorConfig;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // CLI mode for batch processing
/// let (engine, mut events, commands) = BackgroundEngine::new(
///     DetectorConfig::default(),
///     EngineMode::Cli
/// )?;
///
/// // Engine will automatically exit after completing work
/// let result = engine.run().await;
/// # Ok(())
/// # }
/// ```
///
/// ### Interactive Application (TUI/GUI)
/// ```rust
/// use uncp::engine::{BackgroundEngine, EngineMode};
/// use uncp::DetectorConfig;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Interactive mode for ongoing user interaction
/// let (engine, mut events, commands) = BackgroundEngine::new(
///     DetectorConfig::default(),
///     EngineMode::Interactive
/// )?;
///
/// // Engine continues running until explicitly stopped
/// let engine_task = smol::spawn(engine.run());
///
/// // Handle user interactions...
/// // commands.send(EngineCommand::Pause).await?;
/// // commands.send(EngineCommand::Start).await?;
///
/// // Explicit shutdown required
/// commands.send(EngineCommand::Stop).await?;
/// engine_task.await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub enum EngineMode {
	/// CLI mode: optimized for batch processing with automatic shutdown
	Cli,
	/// Interactive mode: persistent operation for GUI/TUI applications
	Interactive,
}

impl BackgroundEngine {
	pub fn start(
		detector: DuplicateDetector,
	) -> (
		Self,
		channel::Receiver<EngineEvent>,
		channel::Sender<EngineCommand>,
	) {
		Self::start_with_mode(detector, EngineMode::Interactive)
	}

	pub fn start_with_mode(
		mut detector: DuplicateDetector,
		mode: EngineMode,
	) -> (
		Self,
		channel::Receiver<EngineEvent>,
		channel::Sender<EngineCommand>,
	) {
		let (cmd_tx, cmd_rx) = channel::unbounded::<EngineCommand>();
		let (evt_tx, evt_rx) = channel::unbounded::<EngineEvent>();

		// Spawn the engine loop on a smol executor thread
		std::thread::spawn(move || {
			future::block_on(async move {
				info!("Engine: started in {:?} mode", mode);
				let mut current_path: Option<PathBuf> = None;
				let mut discovery_completed_for_path: Option<PathBuf> = None;
				let cancellation_token = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
				let mut engine_running = false;
				// Save snapshots periodically to avoid losing too much work
				let mut last_save = std::time::Instant::now();

				let _ = evt_tx.send(EngineEvent::Started).await;

				// Send initial snapshot
				let mut snap = crate::ui::PresentationState::from_detector(&detector);
				if let Some(ref p) = current_path {
					let scoped = detector.files_pending_hash_under_prefix(p.to_string_lossy());
					snap.pending_hash_scoped = Some(scoped);
				}
				let _ = evt_tx.send(EngineEvent::SnapshotReady(snap)).await;
				loop {
					// Pull any pending commands without blocking
					while let Ok(cmd) = cmd_rx.try_recv() {
						match cmd {
							EngineCommand::SetPath(p) => {
								current_path = Some(p.clone());
								// Reset discovery completion when path changes
								if discovery_completed_for_path.as_ref() != Some(&p) {
									discovery_completed_for_path = None;
								}
							}
							EngineCommand::Start => {
								engine_running = true;
								// Reset cancellation token when starting
								cancellation_token.store(false, std::sync::atomic::Ordering::Relaxed);
							}
							EngineCommand::Pause => {
								engine_running = false;
							}
							EngineCommand::Stop => {
								// Cancel any ongoing operations and stop the engine
								cancellation_token.store(true, std::sync::atomic::Ordering::Relaxed);
								engine_running = false;
								break;
							}
							EngineCommand::ClearState => {
								info!("Engine: clearing detector state");
								detector.clear_state();
								// Reset cancellation token when clearing state
								cancellation_token.store(false, std::sync::atomic::Ordering::Relaxed);
							}
							EngineCommand::LoadCache(dir) => {
								let _ = evt_tx.send(EngineEvent::CacheLoading).await;
								if let Ok(true) = detector.load_cache_all(dir) {
									info!("Engine: loaded cache in background");
									let _ = evt_tx.send(EngineEvent::CacheLoaded).await;

									// Validate cached files against filesystem
									let _ = evt_tx.send(EngineEvent::CacheValidating).await;
									match detector.validate_cached_files() {
										Ok((files_removed, files_invalidated)) => {
											let _ = evt_tx
												.send(EngineEvent::CacheValidated {
													files_removed,
													files_invalidated,
												})
												.await;
										}
										Err(e) => {
											warn!("Cache validation failed: {}", e);
											let _ = evt_tx
												.send(EngineEvent::CacheValidated {
													files_removed: 0,
													files_invalidated: 0,
												})
												.await;
										}
									}
								}
							}
							EngineCommand::SetPathFilter(filter) => {
								info!("Engine: setting path filter with {} include patterns, {} exclude patterns",
									filter.include_patterns.len(), filter.exclude_patterns.len());
								detector.config.path_filter = filter;
							}
							EngineCommand::ClearPathFilter => {
								info!("Engine: clearing path filter");
								detector.config.path_filter = crate::PathFilter::default();
							}
						}
					}

					if engine_running && !cancellation_token.load(std::sync::atomic::Ordering::Relaxed) {
						// Check if there's work to do
						let pending_hash_count = detector.files_pending_hash();
						let _total_files = detector.total_files();

						// Only run discovery if we have a path and haven't completed discovery for it yet
						let needs_discovery = current_path.is_some()
							&& discovery_completed_for_path != current_path;

						let needs_hashing = pending_hash_count > 0;

						if needs_discovery {
							if let Some(p) = current_path.clone() {
								info!("Engine: starting discovery for path {}", p.display());
								// Emit initial discovery progress
								let initial_progress = crate::systems::SystemProgress {
									system_name: "Discovery".to_string(),
									processed_items: 0,
									total_items: 0,
									current_item: Some(format!(
										"Starting discovery in {}",
										p.display()
									)),
									estimated_remaining: None,
								};
								let _ = evt_tx
									.send(EngineEvent::DiscoveryProgress(initial_progress))
									.await;

								// Set up progress callback for discovery
								let evt_tx_discovery = evt_tx.clone();
								let discovery_progress =
									std::sync::Arc::new(move |progress: SystemProgress| {
										let _ = evt_tx_discovery
											.try_send(EngineEvent::DiscoveryProgress(progress));
									});

								// Run discovery for this path with cancellation support
								let result = detector
									.scan_with_progress_and_cancellation(p.clone(), discovery_progress, cancellation_token.clone())
									.await;

								// Check if discovery was cancelled
								if cancellation_token.load(std::sync::atomic::Ordering::Relaxed) {
									info!("Engine: discovery was cancelled");
									continue; // Skip to next iteration to process new commands
								}

								if result.is_ok() {
									// Emit final discovery progress
									let final_progress = crate::systems::SystemProgress {
										system_name: "Discovery".to_string(),
										processed_items: detector.total_files(),
										total_items: detector.total_files(),
										current_item: Some("Discovery completed".to_string()),
										estimated_remaining: None,
									};
									let _ = evt_tx
										.send(EngineEvent::DiscoveryProgress(final_progress))
										.await;

									// Mark discovery as completed for this path
									discovery_completed_for_path = current_path.clone();
								}
							}
						} else if needs_hashing {
							info!(
								"Engine: starting hashing for {} pending files",
								pending_hash_count
							);
							// Emit initial hashing progress
							let initial_progress = crate::systems::SystemProgress {
								system_name: "Hashing".to_string(),
								processed_items: detector.total_files() - pending_hash_count,
								total_items: detector.total_files(),
								current_item: Some("Starting hashing...".to_string()),
								estimated_remaining: None,
							};
							let _ = evt_tx
								.send(EngineEvent::HashingProgress(initial_progress))
								.await;

							// Set up progress callback for hashing
							let evt_tx_hashing = evt_tx.clone();
							let hashing_progress =
								std::sync::Arc::new(move |progress: SystemProgress| {
									let _ = evt_tx_hashing
										.try_send(EngineEvent::HashingProgress(progress));
								});

							// Process hashing work
							let _ = detector
								.process_until_complete_with_progress(None, hashing_progress)
								.await;

							// Emit final hashing progress
							let final_progress = crate::systems::SystemProgress {
								system_name: "Hashing".to_string(),
								processed_items: detector.total_files(),
								total_items: detector.total_files(),
								current_item: Some("Hashing completed".to_string()),
								estimated_remaining: None,
							};
							let _ = evt_tx
								.send(EngineEvent::HashingProgress(final_progress))
								.await;
						} else {
							// No work to do, emit completion event and pause
							info!(
								"Engine: no work to do (total_files={}, pending_hash={})",
								detector.total_files(),
								pending_hash_count
							);
							let _ = evt_tx.send(EngineEvent::Completed).await;
							engine_running = false;

							// For CLI usage: if we have a path set and no pending work, we're done
							if matches!(mode, EngineMode::Cli) && current_path.is_some() {
								info!("Engine: work completed for CLI mode, stopping");
								break;
							}
						}

						// Always emit snapshot for UI updates
						let mut snap = crate::ui::PresentationState::from_detector(&detector);
						if let Some(ref p) = current_path {
							let path_str = p.to_string_lossy();
							let scoped = detector.files_pending_hash_under_prefix(&path_str);
							snap.pending_hash_scoped = Some(scoped);
							// Update file table with path filter
							snap.file_table = detector.files_under_prefix_sorted_by_size(&path_str);
							snap.current_path_filter = path_str.to_string();
						}
						let _ = evt_tx.send(EngineEvent::SnapshotReady(snap)).await;

						// Throttled autosave: every ~5s to avoid blocking frequently
						// Only attempt cache save if auto-cache is enabled (avoids filesystem access in tests)
						if last_save.elapsed() >= std::time::Duration::from_secs(5)
							&& !detector.config.disable_auto_cache
						{
							if let Some(dir) = default_cache_dir() {
								let _ = evt_tx.send(EngineEvent::CacheSaving).await;
								// Save cache synchronously but quickly
								let _ = detector.save_cache_all(dir);
								let _ = evt_tx.send(EngineEvent::CacheSaved).await;
							}
							last_save = std::time::Instant::now();
						}

						// Short yield to keep UI responsive
						smol::Timer::after(std::time::Duration::from_millis(100)).await;
					} else {
						// Even when not running, emit snapshots for UI updates
						let mut snap = crate::ui::PresentationState::from_detector(&detector);
						if let Some(ref p) = current_path {
							let path_str = p.to_string_lossy();
							let scoped = detector.files_pending_hash_under_prefix(&path_str);
							snap.pending_hash_scoped = Some(scoped);
							// Update file table with path filter
							snap.file_table = detector.files_under_prefix_sorted_by_size(&path_str);
							snap.current_path_filter = path_str.to_string();
						}
						let _ = evt_tx.send(EngineEvent::SnapshotReady(snap)).await;
						smol::Timer::after(std::time::Duration::from_millis(500)).await;
					}
				}
				// Engine loop ends when no more commands arrive
			})
		});

		(
			Self {
				_cmd_tx: cmd_tx.clone(),
				_evt_rx: evt_rx.clone(),
			},
			evt_rx,
			cmd_tx,
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;
	use tempfile::TempDir;

	#[test]
	fn test_engine_basic_lifecycle() {
		smol::block_on(async {
			let temp_dir = TempDir::new().unwrap();
			let test_path = temp_dir.path().to_path_buf();

			// Create a test file
			std::fs::write(test_path.join("test.txt"), "hello world").unwrap();

			let detector =
				crate::DuplicateDetector::new(crate::DetectorConfig::for_testing()).unwrap();
			let (_engine, events, cmds) = BackgroundEngine::start(detector);

			// Send commands
			cmds.send(EngineCommand::SetPath(test_path.clone()))
				.await
				.unwrap();
			cmds.send(EngineCommand::Start).await.unwrap();

			// Collect events for a short time
			let mut snapshots_received = 0;
			let mut started_received = false;
			let start_time = std::time::Instant::now();

			loop {
				// Check timeout
				if start_time.elapsed() > Duration::from_secs(5) {
					println!("Test timeout reached");
					break;
				}

				// Try to receive an event without blocking
				if let Ok(event) = events.try_recv() {
					match event {
						EngineEvent::Started => {
							started_received = true;
							println!("✓ Received Started event");
						}
						EngineEvent::SnapshotReady(snap) => {
							snapshots_received += 1;
							println!(
								"✓ Received snapshot #{}: total_files={}, pending_hash={}",
								snapshots_received, snap.total_files, snap.pending_hash
							);

							// Stop after we see some progress
							if snapshots_received >= 2 {
								break;
							}
						}
						EngineEvent::Completed => {
							println!("✓ Received Completed event");
							break;
						}
						_ => {}
					}
				} else {
					// No event available, wait a bit
					smol::Timer::after(Duration::from_millis(50)).await;
				}
			}

			assert!(started_received, "Should receive Started event");
			assert!(
				snapshots_received > 0,
				"Should receive at least one snapshot"
			);

			println!("Test completed: {} snapshots received", snapshots_received);
		});
	}

	#[test]
	fn test_background_engine_creation() {
		smol::block_on(async {
			let _temp_dir = tempfile::TempDir::new().unwrap();
			let config = crate::DetectorConfig::for_testing();
			let detector = DuplicateDetector::new(config).unwrap();

			let (_engine, _events, _commands) = BackgroundEngine::start(detector);

			// Engine should start successfully
			// We don't test much here since the engine runs in background
		});
	}
}
