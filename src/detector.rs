//! Main API for duplicate detection

use std::path::{Path, PathBuf};

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
		Ok(Self {
			state: ScanState::new()?,
			relations: RelationStore::new()?,
			scheduler: SystemScheduler::new(),
			memory_mgr: MemoryManager::with_settings(config.memory_settings)?,
		})
	}

	pub fn new_with_cache(config: DetectorConfig) -> DetectorResult<Self> {
		let mut this = Self::new(config)?;
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

	pub async fn scan_with_progress(
		&mut self,
		path: PathBuf,
		progress: std::sync::Arc<dyn Fn(crate::systems::SystemProgress) + Send + Sync>,
	) -> DetectorResult<()> {
		info!(
			"Detector: scan_directory {} (with progress)",
			path.display()
		);
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
		self.scheduler
			.add_system(ContentHashSystem::new().with_scope_prefix(path.to_string_lossy()));
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
	pub async fn process_until_complete_with_progress(
		&mut self,
		path: Option<PathBuf>,
		progress: std::sync::Arc<dyn Fn(SystemProgress) + Send + Sync>,
	) -> DetectorResult<()> {
		if let Some(p) = path.clone() {
			let discovery =
				FileDiscoverySystem::new(vec![p.clone()]).with_progress_callback(progress.clone());
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
		self.state
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
		self.state
			.data
			.clone()
			.lazy()
			.filter(col("hashed").eq(lit(false)))
			.filter(col("path").str().starts_with(lit(pref)))
			.collect()
			.map(|df| df.height())
			.unwrap_or(0)
	}

	pub fn files_under_prefix<S: AsRef<str>>(&self, prefix: S) -> usize {
		use polars::prelude::*;
		let pref = prefix.as_ref();
		self.state
			.data
			.clone()
			.lazy()
			.filter(col("path").str().starts_with(lit(pref)))
			.collect()
			.map(|df| df.height())
			.unwrap_or(0)
	}

	/// Clean up stale cache entries by removing deleted files and marking modified files for re-processing
	pub fn validate_cached_files(&mut self) -> DetectorResult<(usize, usize)> {
		use polars::prelude::*;
		use std::fs;

		let mut files_removed = 0;
		let mut files_invalidated = 0;

		// Get all cached files with their metadata
		let df = self.state.data.clone();
		if df.height() == 0 {
			return Ok((0, 0));
		}

		// Build a mask for files that should be kept
		let mut keep_mask = Vec::new();
		let mut invalidate_mask = Vec::new();

		// Extract columns
		let paths = df.column("path")?.str()?;
		let cached_sizes = df.column("size")?.u64()?;  // size is u64 in schema
		let cached_mtimes = df.column("modified")?.i64()?;  // column name is "modified", not "modified_time"
		let hashed_flags = df.column("hashed")?.bool()?;

		for (((path_opt, cached_size_opt), cached_mtime_opt), hashed_opt) in paths
			.into_iter()
			.zip(cached_sizes.into_iter())
			.zip(cached_mtimes.into_iter())
			.zip(hashed_flags.into_iter())
		{
			if let (Some(path_str), Some(cached_size), Some(cached_mtime), Some(is_hashed)) =
				(path_opt, cached_size_opt, cached_mtime_opt, hashed_opt) {

				let path = Path::new(path_str);

				// Check if file still exists
				match fs::metadata(path) {
					Ok(metadata) => {
						let current_size = metadata.len(); // Keep as u64 to match schema
						let current_mtime = metadata
							.modified()
							.unwrap_or(std::time::UNIX_EPOCH)
							.duration_since(std::time::UNIX_EPOCH)
							.unwrap_or_default()
							.as_nanos() as i64; // Convert to nanoseconds as per schema comment

						// Keep the file
						keep_mask.push(true);

						// Check if file has been modified
						if current_size != cached_size || current_mtime != cached_mtime {
							// File modified, mark for re-processing
							invalidate_mask.push(true);
							if is_hashed {
								files_invalidated += 1;
							}
						} else {
							// File unchanged
							invalidate_mask.push(false);
						}
					}
					Err(_) => {
						// File no longer exists, remove it
						keep_mask.push(false);
						invalidate_mask.push(false);
						files_removed += 1;
					}
				}
			} else {
				// Invalid row, remove it
				keep_mask.push(false);
				invalidate_mask.push(false);
			}
		}

		// Apply the keep mask to filter out deleted files
		if keep_mask.iter().any(|&x| !x) {
			let keep_series = Series::new("keep", keep_mask.clone());
			self.state.data = self.state.data
				.clone()
				.lazy()
				.filter(lit(keep_series))
				.collect()?;
		}

		// Apply the invalidate mask to mark modified files for re-hashing
		if invalidate_mask.iter().any(|&x| x) {
			let invalidate_series = Series::new("invalidate", invalidate_mask);
			self.state.data = self.state.data
				.clone()
				.lazy()
				.with_columns([
					when(lit(invalidate_series))
						.then(lit(false))
						.otherwise(col("hashed"))
						.alias("hashed")
				])
				.collect()?;
		}

		info!("Cache validation: {} files removed, {} files marked for re-processing", files_removed, files_invalidated);
		Ok((files_removed, files_invalidated))
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
