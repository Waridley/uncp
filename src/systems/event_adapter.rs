//! Event-to-queue adapter system: consumes RequestContentLoad events and enqueues them.

use async_trait::async_trait;
use tracing::info;

use crate::content_queue::ContentLoadQueue;
use crate::data::ScanState;
use crate::error::SystemResult;
use crate::events::{EventBus, SystemEvent};
use crate::memory::MemoryManager;
use crate::systems::{System, SystemRunner};

pub struct RequestToQueueAdapter {
	pub bus: std::sync::Arc<EventBus>,
	pub queue: std::sync::Arc<std::sync::Mutex<ContentLoadQueue>>,
}

impl RequestToQueueAdapter {
	pub fn new(
		bus: std::sync::Arc<EventBus>,
		queue: std::sync::Arc<std::sync::Mutex<ContentLoadQueue>>,
	) -> Self {
		Self { bus, queue }
	}
}

#[async_trait]
impl SystemRunner for RequestToQueueAdapter {
	async fn run(
		&self,
		_state: &mut ScanState,
		_memory_mgr: &mut MemoryManager,
	) -> SystemResult<()> {
		let rx = self.bus.subscribe();
		// Drain any currently buffered RequestContentLoad events without blocking
		let mut count = 0usize;
		while let Ok(ev) = rx.try_recv() {
			if let SystemEvent::RequestContentLoad { path } = ev {
				let mut q = self.queue.lock().unwrap();
				q.enqueue(path);
				count += 1;
			}
		}
		if count > 0 {
			info!("RequestToQueueAdapter enqueued {} items", count);
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
		"RequestToQueueAdapter"
	}
}

impl System for RequestToQueueAdapter {
	fn required_columns(&self) -> &[&'static str] {
		&[]
	}
	fn optional_columns(&self) -> &[&'static str] {
		&[]
	}
	fn description(&self) -> &'static str {
		"Converts RequestContentLoad events into queue entries"
	}
}
