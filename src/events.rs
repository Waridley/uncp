//! Central event bus for system-to-system communication

use async_channel as channel;
use std::sync::{Arc, Mutex};
use tracing::error;

/// Identify which DataFrame/store the metadata belongs to.
#[derive(Debug, Clone)]
pub enum DataFrameId {
	/// Main per-file information DataFrame
	Files,
	/// Relations/edges DataFrame(s)
	Relations,
}

/// Events emitted by systems and core components.
#[derive(Debug, Clone)]
pub enum SystemEvent {
	/// File discovery has produced new files (count only, data is in shared DataFrames)
	FilesDiscovered { count: usize },
	/// New metadata columns are available in the shared data store
	MetadataAvailable {
		columns: Vec<String>,
		frame: DataFrameId,
	},
	/// A system requests a content load for a specific file id (can be recent or old)
	RequestContentLoad { path: crate::paths::DirEntryId },
	/// File contents were loaded into memory for the given path id
	ContentLoaded { path: crate::paths::DirEntryId },
	/// File contents were unloaded (all users released)
	ContentUnloaded { path: crate::paths::DirEntryId },
}

/// Simple broadcast-style event bus using fan-out to per-subscriber channels.
#[derive(Default)]
pub struct EventBus {
	subscribers: Mutex<Vec<channel::Sender<SystemEvent>>>,
}

impl EventBus {
	pub fn new() -> Arc<Self> {
		Arc::new(Self {
			subscribers: Mutex::new(Vec::new()),
		})
	}

	/// Subscribe to events. Returns a Receiver that will get future events.
	pub fn subscribe(self: &Arc<Self>) -> channel::Receiver<SystemEvent> {
		let (tx, rx) = channel::unbounded();
		match self.subscribers.lock() {
			Ok(mut subs) => subs.push(tx),
			Err(_) => error!("EventBus: subscribers lock poisoned; subscriber not registered"),
		}
		rx
	}

	/// Broadcast an event to all subscribers. Best-effort, drops if a channel is full/closed.
	pub fn emit(&self, event: SystemEvent) {
		if let Ok(subs) = self.subscribers.lock() {
			for sub in subs.iter() {
				let _ = sub.try_send(event.clone());
			}
		} else {
			error!("EventBus: subscribers lock poisoned; dropping event");
		}
	}
}
