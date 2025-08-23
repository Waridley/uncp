//! System scheduler for orchestrating system execution

use std::sync::Arc;

use crate::error::SystemResult;
use crate::pool::DataPool;
use crate::systems::SystemRunner;
use tracing::info;

pub struct SystemScheduler {
	pub systems: Vec<Arc<dyn SystemRunner>>,
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
		self.systems.push(Arc::new(system));
	}

	pub fn add_boxed_system(&mut self, system: Arc<dyn SystemRunner>) {
		self.systems.push(system);
	}

	pub async fn run_all(&self, pool: Arc<DataPool>) -> SystemResult<()> {
		info!("Scheduler: spawning {} systems", self.systems.len());
		for sys in &self.systems {
			let p = pool.clone();
			let s = sys.clone();
			smol::spawn(async move {
				let _ = s.run(p).await;
			})
			.detach();
		}
		Ok(())
	}
}
