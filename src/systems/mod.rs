//! Processing systems for the duplicate detection pipeline

use async_trait::async_trait;

use crate::data::ScanState;
use crate::error::SystemResult;
use crate::memory::MemoryManager;

pub mod discovery;
pub mod event_adapter;
pub mod hashing;
pub mod loader;
pub mod scheduler;

pub use discovery::FileDiscoverySystem;
pub use hashing::ContentHashSystem;
pub use scheduler::SystemScheduler;

/// Common interface for all processing systems
#[async_trait]
pub trait SystemRunner: Send + Sync {
	/// Run the system on the current state
	async fn run(&self, state: &mut ScanState, memory_mgr: &mut MemoryManager) -> SystemResult<()>;

	/// Run the system with cancellation support
	async fn run_with_cancellation(
		&self,
		state: &mut ScanState,
		memory_mgr: &mut MemoryManager,
		_cancellation_token: std::sync::Arc<std::sync::atomic::AtomicBool>,
	) -> SystemResult<()> {
		// Default implementation just calls run() - systems can override for cancellation support
		self.run(state, memory_mgr).await
	}

	/// Check if this system can run (dependencies met)
	fn can_run(&self, state: &ScanState) -> bool;

	/// Hint if there is work to do; default defers to can_run
	fn has_work(&self, state: &ScanState) -> bool {
		self.can_run(state)
	}

	/// System priority (higher number = higher priority)
	fn priority(&self) -> u8;

	/// System name for logging and identification
	fn name(&self) -> &'static str;
}

/// System metadata interface
pub trait System {
	/// Columns required by this system
	fn required_columns(&self) -> &[&'static str];

	/// Columns optionally used by this system
	fn optional_columns(&self) -> &[&'static str];

	/// System description
	fn description(&self) -> &'static str;
}

/// Progress information for system execution
#[derive(Debug, Clone)]
pub struct SystemProgress {
	pub system_name: String,
	pub total_items: usize,
	pub processed_items: usize,
	pub current_item: Option<String>,
	pub estimated_remaining: Option<std::time::Duration>,
}

impl SystemProgress {
	pub fn new(system_name: String, total_items: usize) -> Self {
		Self {
			system_name,
			total_items,
			processed_items: 0,
			current_item: None,
			estimated_remaining: None,
		}
	}

	pub fn update(&mut self, processed: usize, current_item: Option<String>) {
		self.processed_items = processed;
		self.current_item = current_item;
	}

	pub fn progress_ratio(&self) -> f64 {
		if self.total_items == 0 {
			1.0
		} else {
			self.processed_items as f64 / self.total_items as f64
		}
	}

	pub fn is_complete(&self) -> bool {
		self.processed_items >= self.total_items
	}
}

/// System execution context
pub struct SystemContext {
	pub max_concurrent_files: usize,
	pub yield_interval: std::time::Duration,
	pub progress_callback: Option<Box<dyn Fn(SystemProgress) + Send + Sync>>,
	pub cancellation_token: std::sync::Arc<std::sync::atomic::AtomicBool>,
	/// Optional event bus for broadcast events between systems/UI
	pub event_bus: Option<std::sync::Arc<crate::events::EventBus>>,
}

impl Default for SystemContext {
	fn default() -> Self {
		Self {
			max_concurrent_files: num_cpus::get(),
			yield_interval: std::time::Duration::from_millis(100),
			progress_callback: None,
			cancellation_token: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
			event_bus: None,
		}
	}
}

impl SystemContext {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_max_concurrent_files(mut self, max: usize) -> Self {
		self.max_concurrent_files = max;
		self
	}

	pub fn with_yield_interval(mut self, interval: std::time::Duration) -> Self {
		self.yield_interval = interval;
		self
	}

	pub fn with_progress_callback<F>(mut self, callback: F) -> Self
	where
		F: Fn(SystemProgress) + Send + Sync + 'static,
	{
		self.progress_callback = Some(Box::new(callback));
		self
	}

	pub fn with_cancellation_token(
		mut self,
		token: std::sync::Arc<std::sync::atomic::AtomicBool>,
	) -> Self {
		self.cancellation_token = token;
		self
	}

	pub fn with_event_bus(mut self, bus: std::sync::Arc<crate::events::EventBus>) -> Self {
		self.event_bus = Some(bus);
		self
	}

	pub fn report_progress(&self, progress: SystemProgress) {
		if let Some(ref callback) = self.progress_callback {
			callback(progress);
		}
	}

	pub fn is_cancelled(&self) -> bool {
		self.cancellation_token
			.load(std::sync::atomic::Ordering::Relaxed)
	}

	pub fn cancel(&self) {
		self.cancellation_token
			.store(true, std::sync::atomic::Ordering::Relaxed);
	}
}

/// Helper function to yield control periodically and check for cancellation
pub async fn yield_periodically(
	last_yield: &mut std::time::Instant,
	interval: std::time::Duration,
) {
	if last_yield.elapsed() >= interval {
		smol::future::yield_now().await;
		*last_yield = std::time::Instant::now();
	}
}

/// Helper function to yield control periodically and check for cancellation
pub async fn yield_periodically_with_cancellation(
	last_yield: &mut std::time::Instant,
	interval: std::time::Duration,
	context: &SystemContext,
) -> Result<(), crate::error::SystemError> {
	if last_yield.elapsed() >= interval {
		smol::future::yield_now().await;
		*last_yield = std::time::Instant::now();
	}

	if context.is_cancelled() {
		return Err(crate::error::SystemError::ExecutionFailed {
			system: "System".to_string(),
			reason: "Operation was cancelled".to_string(),
		});
	}

	Ok(())
}
