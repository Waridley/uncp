//! Main API for duplicate detection

use std::path::PathBuf;

use crate::cache::CacheManager;
use crate::data::{RelationStore, ScanState};
use crate::error::{DetectorError, DetectorResult};
use crate::memory::{MemoryManager, Settings};
use crate::query::Query;

use crate::paths::default_cache_dir;

use crate::systems::{ContentHashSystem, FileDiscoverySystem, SystemProgress, SystemScheduler};
use tracing::info;

#[derive(Debug)]
pub struct DetectorConfig {
	pub memory_settings: Settings,
}

impl Default for DetectorConfig {
	fn default() -> Self {
		Self {
			memory_settings: Settings::from_sysinfo().expect("failed to read sysinfo"),
		}
	}
}

pub struct DuplicateDetector {
	pub state: ScanState,
	pub relations: RelationStore,
	pub scheduler: SystemScheduler,
	pub memory_mgr: MemoryManager,
}

impl DuplicateDetector {
	pub fn new(config: DetectorConfig) -> DetectorResult<Self> {
		let mut this = Self {
			state: ScanState::new()?,
			relations: RelationStore::new()?,
			scheduler: SystemScheduler::new(),
			memory_mgr: MemoryManager::with_settings(config.memory_settings)?,
		};
		// Try loading cache on creation (best-effort)
		if let Some(dir) = default_cache_dir() {
			if let Ok(Some((state, relations))) = CacheManager::new(dir).load_all() {
				this.state = state;
				this.relations = relations;
				info!("Detector: loaded cache at init");
			}
		}
		Ok(this)
	}

	pub async fn scan_directory(&mut self, path: PathBuf) -> DetectorResult<()> {
		info!("Detector: scan_directory {}", path.display());
		let discovery = FileDiscoverySystem::new(vec![path]);
		self.scheduler.add_system(discovery);
		let res = self.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok() {
			if let Some(dir) = default_cache_dir() {
				let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
			}
		}
		res
	}

	pub async fn scan_with_progress(
		&mut self,
		path: PathBuf,
		progress: std::sync::Arc<dyn Fn(crate::systems::SystemProgress) + Send + Sync>,
	) -> DetectorResult<()> {
		info!("Detector: scan_directory {} (with progress)", path.display());
		let discovery = FileDiscoverySystem::new(vec![path]).with_progress_callback(progress);
		self.scheduler.add_system(discovery);
		let res = self
			.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok() {
			if let Some(dir) = default_cache_dir() {
				let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
			}
		}
		res
	}

	pub async fn scan_and_hash(&mut self, path: PathBuf) -> DetectorResult<()> {
		info!("Detector: scan_and_hash {}", path.display());
		// Run discovery first
		let discovery = FileDiscoverySystem::new(vec![path.clone()]);
		self.scheduler.add_system(discovery);
		// Then hashing (scoped to requested path)
		self.scheduler.add_system(ContentHashSystem::new().with_scope_prefix(path.to_string_lossy()));
		let res = self.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok() {
			if let Some(dir) = default_cache_dir() {
				let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
			}
		}
		res
	}
	pub async fn process_until_complete_with_progress(
		&mut self,
		path: Option<PathBuf>,
		progress: std::sync::Arc<dyn Fn(SystemProgress) + Send + Sync>,
	) -> DetectorResult<()> {
		if let Some(p) = path.clone() {
			let discovery = FileDiscoverySystem::new(vec![p.clone()]).with_progress_callback(progress.clone());
			self.scheduler.add_system(discovery);
		}
		self.scheduler.add_system(ContentHashSystem::new());
		let res = self
			.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok() {
			if let Some(dir) = default_cache_dir() {
				let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
			}
		}
		res
	}


	pub async fn process_until_complete(&mut self) -> DetectorResult<()> {
		let res = self.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok() {
			if let Some(dir) = default_cache_dir() {
				let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
			}
		}
		res
	}

	pub fn save_cache_all(&self, dir: PathBuf) -> DetectorResult<()> {
		CacheManager::new(dir).save_all(&self.state, &self.relations)?;
		Ok(())
	}

	pub fn load_cache_all(&mut self, dir: PathBuf) -> DetectorResult<bool> {
		if let Some((state, relations)) = CacheManager::new(dir).load_all()? {
			self.state = state;
			self.relations = relations;
			return Ok(true);
		}
		Ok(false)
	}

	pub fn query(&self) -> Query<'_> {
		Query::new(&self.state, &self.relations)
	}

	// Disk caching stubs for now

	pub fn total_files(&self) -> usize {
		self.state.data.height()
	}
	pub fn files_pending_hash(&self) -> usize {
		self
			.state
			.data
			.column("hashed")
			.ok()
			.and_then(|s| s.bool().ok())
			.map(|b| b.into_iter().filter(|v| matches!(v, Some(false))).count())
			.unwrap_or(0)
	}

	pub fn files_pending_hash_under_prefix<S: AsRef<str>>(&self, prefix: S) -> usize {
		use polars::prelude::*;
		let pref = prefix.as_ref();
		self
			.state
			.data
			.clone()
			.lazy()
			.filter(col("hashed").eq(lit(false)))
			.filter(col("path").str().starts_with(lit(pref)))
			.collect()
			.map(|df| df.height())
			.unwrap_or(0)
	}
	pub fn files_by_type_counts(&self) -> std::collections::HashMap<String, usize> {
		let mut map = std::collections::HashMap::new();
		if let Ok(s) = self.state.data.column("file_type") {
			if let Ok(utf8) = s.str() {
				for v in utf8.into_iter().flatten() {
					*map.entry(v.to_string()).or_insert(0) += 1;
				}
			}
		}
		map
	}
}
