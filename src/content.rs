//! Content store with reference counting and bus notifications.

use std::collections::HashMap;
use std::sync::Arc;

use crate::DetectorResult;
use crate::events::{EventBus, SystemEvent};
use crate::memory::{FileBytes, MemoryManager};
use crate::paths::DirEntryId;

/// Manages loading/unloading of file contents with simple ref-count semantics.
pub struct ContentStore {
	refs: HashMap<DirEntryId, usize>,
	bus: Arc<EventBus>,
}

impl ContentStore {
	pub fn new(bus: Arc<EventBus>) -> Self {
		Self {
			refs: HashMap::new(),
			bus,
		}
	}

	/// Ensure contents for this id are loaded and increment reference count.
	pub async fn acquire(
		&mut self,
		id: DirEntryId,
		memory: &mut MemoryManager,
	) -> DetectorResult<()> {
		let count = self.refs.entry(id).or_insert(0);
		*count += 1;
		// Load into memory (no-op if already cached)
		let _ = memory.load_file(id.resolve()).await?;
		// Notify subscribers that content is available
		self.bus.emit(SystemEvent::ContentLoaded { path: id });
		Ok(())
	}

	/// Decrement reference count; unload when it reaches zero.
	pub fn release(&mut self, id: DirEntryId, memory: &mut MemoryManager) {
		if let Some(count) = self.refs.get_mut(&id) {
			if *count > 0 {
				*count -= 1;
			}
			if *count == 0 {
				self.refs.remove(&id);
				// Remove from memory and emit event
				let _ = memory.remove_file(&id.resolve());
				self.bus.emit(SystemEvent::ContentUnloaded { path: id });
			}
		}
	}

	/// Check if content is currently loaded.
	pub fn is_loaded(&mut self, id: DirEntryId, memory: &mut MemoryManager) -> bool {
		memory.get_file(&id.resolve()).is_some()
	}

	/// Get a temporary view into cached bytes (if present). Consumers should use events to
	/// know when data is available; this API does not bump ref-count by itself.
	pub fn get_cached<'a>(
		&'a mut self,
		id: DirEntryId,
		memory: &'a mut MemoryManager,
	) -> Option<&'a FileBytes> {
		memory.get_file(&id.resolve())
	}
}
