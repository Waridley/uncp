//! Content loading system: drains request queue, prioritizes large files, respects memory.

use async_trait::async_trait;
use tracing::{debug, info, warn};

use crate::content_queue::ContentLoadQueue;
use crate::data::ScanState;
use crate::error::{SystemError, SystemResult};
use crate::events::{EventBus, SystemEvent};
use crate::memory::MemoryManager;
use crate::paths::DirEntryId;
use crate::systems::{System, SystemRunner};

pub struct ContentLoaderSystem {
	pub queue: std::sync::Arc<std::sync::Mutex<ContentLoadQueue>>,
	pub event_bus: Option<std::sync::Arc<EventBus>>,
	/// Max items to attempt per run cycle (avoid monopolizing scheduler)
	pub max_per_run: usize,
}

impl ContentLoaderSystem {
	pub fn new(queue: std::sync::Arc<std::sync::Mutex<ContentLoadQueue>>) -> Self {
		Self {
			queue,
			max_per_run: 512,
			event_bus: None,
		}
	}

	pub fn with_event_bus(mut self, bus: std::sync::Arc<EventBus>) -> Self {
		self.event_bus = Some(bus);
		self
	}

	pub fn with_max_per_run(mut self, v: usize) -> Self {
		self.max_per_run = v;
		self
	}
}

#[async_trait]
impl SystemRunner for ContentLoaderSystem {
	async fn run(&self, state: &mut ScanState, memory_mgr: &mut MemoryManager) -> SystemResult<()> {
		// Drain bus requests first (non-blocking)
		if let Some(ref bus) = self.event_bus {
			// We can't drain directly from EventBus; instead, consumers should enqueue into the queue when requesting loads.
			// Keep this placeholder in case we later extend EventBus with a shared stream.
			let _ = bus; // suppress unused warning for now
		}

		// Snapshot current queue
		let snapshot: Vec<DirEntryId> = {
			let q = self.queue.lock().unwrap();
			q.snapshot()
		};
		if snapshot.is_empty() {
			return Ok(());
		}

		// Prioritize large files first: sort by size desc using DataFrame lookup
		// Build a map DirEntryId -> size from state.data
		let mut items = snapshot;
		let sizes = match state.data.column("size") {
			Ok(col) => col.u64().map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?,
			Err(e) => {
				return Err(SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				});
			}
		};
		let path_struct = state
			.data
			.column("path")
			.and_then(|c| c.struct_())
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?;
		let idx_series =
			path_struct
				.field_by_name("idx")
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?;
		let idx_ca = idx_series
			.u64()
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.clone();
		let gen_series =
			path_struct
				.field_by_name("gen")
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?;
		let gen_ca = gen_series
			.u64()
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.clone();

		let mut size_by_key = std::collections::HashMap::with_capacity(state.data.height());
		for i in 0..state.data.height() {
			if let (Some(idx), Some(r#gen), Some(sz)) = (idx_ca.get(i), gen_ca.get(i), sizes.get(i))
			{
				size_by_key.insert((idx as usize, r#gen as usize), sz);
			}
		}

		items.sort_by_key(|id| {
			std::cmp::Reverse(size_by_key.get(&id.raw_parts()).copied().unwrap_or(0))
		});

		// Attempt loads up to max_per_run
		let mut loaded = 0usize;
		for id in items.into_iter().take(self.max_per_run) {
			// Try to allocate the expected bytes based on size column to short-circuit if necessary
			let expected = size_by_key.get(&id.raw_parts()).copied().unwrap_or(0) as usize;
			if expected > 0 && !memory_mgr.can_allocate(expected) {
				// Try evicting/allocating through try_allocate; if fails, stop loading further
				if memory_mgr.try_allocate(expected).is_err() {
					debug!("ContentLoader: memory pressure, deferring further loads");
					break;
				}
				// If succeeded via try_allocate, immediately deallocate to not double count; load_file will count real size
				memory_mgr.deallocate(expected);
			}

			// Load the file into memory
			match memory_mgr.load_file(id.resolve()).await {
				Ok(_) => {
					if let Some(ref bus) = self.event_bus {
						bus.emit(SystemEvent::ContentLoaded { path: id });
					}
					// Remove from queue after success
					let mut q = self.queue.lock().unwrap();
					q.remove(&id);
					loaded += 1;
				}
				Err(e) => {
					warn!(?e, "ContentLoader: failed to load content");
				}
			}
		}

		if loaded > 0 {
			info!("ContentLoader: loaded {} items", loaded);
		}
		Ok(())
	}

	fn can_run(&self, _state: &ScanState) -> bool {
		true
	}
	fn priority(&self) -> u8 {
		0
	}
	fn name(&self) -> &'static str {
		"ContentLoader"
	}
}

impl System for ContentLoaderSystem {
	fn required_columns(&self) -> &[&'static str] {
		&["path", "size"]
	}
	fn optional_columns(&self) -> &[&'static str] {
		&[]
	}
	fn description(&self) -> &'static str {
		"Loads file contents into memory based on queue and memory availability"
	}
}
