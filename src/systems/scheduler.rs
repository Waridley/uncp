//! System scheduler for orchestrating system execution

use crate::data::ScanState;
use crate::error::SystemResult;
use crate::memory::MemoryManager;
use crate::systems::SystemRunner;
use tracing::{debug, info};

pub struct SystemScheduler {
	pub systems: Vec<Box<dyn SystemRunner>>,
}

impl Default for SystemScheduler {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemScheduler {
	pub fn new() -> Self {
		Self {
			systems: Vec::new(),
		}
	}

	pub fn add_system<S: SystemRunner + 'static>(&mut self, system: S) {
		debug!("Scheduler: added system");
		self.systems.push(Box::new(system));
	}

	/// Add a pre-boxed system (useful for dynamic registration from UIs/plugins)
	pub fn add_boxed_system(&mut self, system: Box<dyn SystemRunner>) {
		debug!("Scheduler: added boxed system");
		self.systems.push(system);
	}

	pub async fn run_all(
		&self,
		state: &mut ScanState,
		memory_mgr: &mut MemoryManager,
	) -> SystemResult<()> {
		// Sequential single pass without priority to keep behavior deterministic for now
		let runnable: Vec<&Box<dyn SystemRunner>> =
			self.systems.iter().filter(|s| s.can_run(state)).collect();
		info!("Scheduler: running {} systems", runnable.len());
		for system in runnable.into_iter() {
			debug!("Scheduler: running system {}", system.name());
			system.run(state, memory_mgr).await?;
			debug!("Scheduler: completed system {}", system.name());
		}
		Ok(())
	}

	pub async fn run_all_with_cancellation(
		&self,
		state: &mut ScanState,
		memory_mgr: &mut MemoryManager,
		cancellation_token: std::sync::Arc<std::sync::atomic::AtomicBool>,
	) -> SystemResult<()> {
		let runnable: Vec<&Box<dyn SystemRunner>> =
			self.systems.iter().filter(|s| s.can_run(state)).collect();
		info!("Scheduler: running {} systems", runnable.len());
		for system in runnable.into_iter() {
			if cancellation_token.load(std::sync::atomic::Ordering::Relaxed) {
				info!("Scheduler: operation cancelled");
				return Err(crate::error::SystemError::ExecutionFailed {
					system: "Scheduler".to_string(),
					reason: "Operation was cancelled".to_string(),
				});
			}
			debug!("Scheduler: running system {}", system.name());
			system
				.run_with_cancellation(state, memory_mgr, cancellation_token.clone())
				.await?;
			debug!("Scheduler: completed system {}", system.name());
		}
		Ok(())
	}
}
