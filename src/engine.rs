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
    cmd_tx: channel::Sender<EngineCommand>,
    evt_rx: channel::Receiver<EngineEvent>,
}

impl BackgroundEngine {
    pub fn start(mut detector: DuplicateDetector) -> (Self, channel::Receiver<EngineEvent>, channel::Sender<EngineCommand>) {
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
                loop {
                    // Pull any pending commands without blocking
                    while let Ok(cmd) = cmd_rx.try_recv() {
                        match cmd {
                            EngineCommand::SetPath(p) => { current_path = Some(p); }
                            EngineCommand::Start => { running = true; }
                            EngineCommand::Pause => { running = false; }
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
                            let scoped = detector.files_pending_hash_under_prefix(p.to_string_lossy());
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
                        smol::Timer::after(std::time::Duration::from_millis(100)).await;
                    }
                }
                let _ = evt_tx.send(EngineEvent::Completed).await;
            });
        });

        (Self { cmd_tx: cmd_tx.clone(), evt_rx: evt_rx.clone() }, evt_rx, cmd_tx)
    }
}

