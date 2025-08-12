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

	pub async fn run_all(
		&self,
		state: &mut ScanState,
		memory_mgr: &mut MemoryManager,
	) -> SystemResult<()> {
		// Filter systems that can run
		let mut runnable: Vec<&Box<dyn SystemRunner>> =
			self.systems.iter().filter(|s| s.can_run(state)).collect();

		// Sort by priority descending
		runnable.sort_by_key(|s| std::cmp::Reverse(s.priority()));
		info!("Scheduler: running {} systems", runnable.len());

		// Execute sequentially for now; parallelism can be added when dependencies are managed
		for system in runnable.into_iter() {
			debug!(
				"Scheduler: running system {} (priority {})",
				system.name(),
				system.priority()
			);
			system.run(state, memory_mgr).await?;
			debug!("Scheduler: completed system {}", system.name());
		}

		Ok(())
	}
}
