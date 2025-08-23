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
	async fn run(&self, pool: std::sync::Arc<crate::pool::DataPool>) -> SystemResult<()> {
		loop {
			if !pool.is_running() { break; }
			// Snapshot current queue
			let snapshot: Vec<DirEntryId> = { let q = self.queue.lock().unwrap(); q.snapshot() };
			if snapshot.is_empty() { smol::future::yield_now().await; continue; }
			// Build size map under read lock
			let (mut items, size_by_key): (Vec<DirEntryId>, std::collections::HashMap<(usize,usize), u64>) = {
				let state = pool.state.read().unwrap();
				let mut items = snapshot;
				let sizes = state.data.column("size").and_then(|c| c.u64()).map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?.clone();
				let path_struct = state.data.column("path").and_then(|c| c.struct_()).map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;
				let idx_series = path_struct.field_by_name("idx").map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?.clone();
				let gen_series = path_struct.field_by_name("gen").map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?.clone();
				let idx_ca = idx_series.u64().map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?.clone();
				let gen_ca = gen_series.u64().map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?.clone();
				let mut size_by_key = std::collections::HashMap::with_capacity(state.data.height());
				for i in 0..state.data.height() { if let (Some(idx), Some(r#gen), Some(sz)) = (idx_ca.get(i), gen_ca.get(i), sizes.get(i)) { size_by_key.insert((idx as usize, r#gen as usize), sz); } }
				(items, size_by_key)
			};
			items.sort_by_key(|id| std::cmp::Reverse(size_by_key.get(&id.raw_parts()).copied().unwrap_or(0)));
			// Attempt loads up to max_per_run
			let mut loaded = 0usize;
			for id in items.into_iter().take(self.max_per_run) {
				let expected = size_by_key.get(&id.raw_parts()).copied().unwrap_or(0) as usize;
				// Read file bytes without holding the memory lock
				let bytes = match smol::fs::read(id.resolve()).await {
					Ok(b) => b,
					Err(e) => { warn!(?e, "ContentLoader: failed to read content"); continue; }
				};
				// Now put into cache under a short write lock
				let mut mem = pool.memory.write().unwrap();
				let load_ok = match mem.put_file(id.resolve(), crate::memory::FileBytes::new(bytes)) {
					Ok(_) => true,
					Err(e) => { warn!(?e, "ContentLoader: cache put failed"); false }
				};
				if load_ok { if let Some(ref bus) = self.event_bus { bus.emit(SystemEvent::ContentLoaded { path: id }); } let mut q = self.queue.lock().unwrap(); q.remove(&id); loaded += 1; }
			}
			if loaded > 0 { info!("ContentLoader: loaded {} items", loaded); }
			smol::future::yield_now().await;
		}
		Ok(())
	}
	fn name(&self) -> &'static str { "ContentLoader" }
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
