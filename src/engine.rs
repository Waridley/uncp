//! Background engine to drive continuous processing with progress events

use std::path::PathBuf;

use async_channel as channel;
use futures_lite::future;
use tracing::info;

use crate::systems::SystemProgress;
use crate::{paths::default_cache_dir, DuplicateDetector};

#[derive(Debug, Clone)]
pub enum EngineCommand {
	SetPath(PathBuf),
	Start,
	Pause,
	Stop,
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
	DiscoveryProgress(SystemProgress),
	HashingProgress(SystemProgress),
	SnapshotReady(crate::ui::PresentationState),
	Started,
	Completed,
	Error(String),
}

pub struct BackgroundEngine {
	_cmd_tx: channel::Sender<EngineCommand>,
	_evt_rx: channel::Receiver<EngineEvent>,
}

impl BackgroundEngine {
	pub fn start(
		mut detector: DuplicateDetector,
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
				info!("Engine: started");
				let mut current_path: Option<PathBuf> = None;
				let mut running = false;
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
								current_path = Some(p);
							}
							EngineCommand::Start => {
								running = true;
							}
							EngineCommand::Pause => {
								running = false;
							}
							EngineCommand::Stop => break,
						}
					}

					if running {
						// Small step: run discovery and hashing for current path then yield
						if let Some(p) = current_path.clone() {
							let _ = detector.scan_directory(p.clone()).await;
							let _ = detector.scan_and_hash(p.clone()).await;
						}
						// Emit snapshot for UI/CLI refresh, with scoped pending if path set
						let mut snap = crate::ui::PresentationState::from_detector(&detector);
						if let Some(ref p) = current_path {
							let scoped =
								detector.files_pending_hash_under_prefix(p.to_string_lossy());
							snap.pending_hash_scoped = Some(scoped);
						}
						let _ = evt_tx.send(EngineEvent::SnapshotReady(snap)).await;
						// Throttled autosave: every ~1s or when many files processed, to balance durability vs. throughput
						if last_save.elapsed() >= std::time::Duration::from_secs(1) {
							if let Some(dir) = default_cache_dir() {
								let _ = detector.save_cache_all(dir);
							}
							last_save = std::time::Instant::now();
						}
						smol::Timer::after(std::time::Duration::from_millis(75)).await;
					} else {
						// Even when not running, emit snapshots for UI updates
						let mut snap = crate::ui::PresentationState::from_detector(&detector);
						if let Some(ref p) = current_path {
							let scoped = detector.files_pending_hash_under_prefix(p.to_string_lossy());
							snap.pending_hash_scoped = Some(scoped);
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

            let detector = crate::DuplicateDetector::new(crate::DetectorConfig::default()).unwrap();
            let (_engine, events, cmds) = BackgroundEngine::start(detector);

            // Send commands
            cmds.send(EngineCommand::SetPath(test_path.clone())).await.unwrap();
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
                            println!("✓ Received snapshot #{}: total_files={}, pending_hash={}",
                                snapshots_received, snap.total_files, snap.pending_hash);

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
            assert!(snapshots_received > 0, "Should receive at least one snapshot");

            println!("Test completed: {} snapshots received", snapshots_received);
        });
    }
}