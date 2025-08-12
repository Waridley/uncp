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
	/// Disable automatic cache saving (useful for tests)
	pub disable_auto_cache: bool,
}

impl Default for DetectorConfig {
	fn default() -> Self {
		Self {
			memory_settings: Settings::from_sysinfo().expect("failed to read sysinfo"),
			disable_auto_cache: false,
		}
	}
}

impl DetectorConfig {
	/// Create a test configuration that disables automatic cache saving
	#[cfg(test)]
	pub fn for_testing() -> Self {
		Self {
			memory_settings: Settings::from_sysinfo().expect("failed to read sysinfo"),
			disable_auto_cache: true,
		}
	}
}

pub struct DuplicateDetector {
	pub state: ScanState,
	pub relations: RelationStore,
	pub scheduler: SystemScheduler,
	pub memory_mgr: MemoryManager,
	pub config: DetectorConfig,
}

impl DuplicateDetector {
	pub fn new(config: DetectorConfig) -> DetectorResult<Self> {
		let memory_settings = config.memory_settings.clone();
		Ok(Self {
			state: ScanState::new()?,
			relations: RelationStore::new()?,
			scheduler: SystemScheduler::new(),
			memory_mgr: MemoryManager::with_settings(memory_settings)?,
			config,
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

	/// Create a detector with cache loading from a specific directory (for testing)
	#[cfg(test)]
	pub fn new_with_cache_dir(config: DetectorConfig, cache_dir: PathBuf) -> DetectorResult<Self> {
		let mut this = Self::new(config)?;
		// Try loading cache from specified directory (best-effort)
		if let Ok(Some((state, relations))) = CacheManager::new(cache_dir).load_all() {
			this.state = state;
			this.relations = relations;
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
		if res.is_ok() && !self.config.disable_auto_cache {
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
		if res.is_ok() && !self.config.disable_auto_cache {
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
		if res.is_ok() && !self.config.disable_auto_cache {
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
		if res.is_ok() && !self.config.disable_auto_cache {
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
		if res.is_ok() && !self.config.disable_auto_cache {
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

	/// Save cache only if auto-cache is enabled (respects disable_auto_cache flag)
	pub fn save_cache_if_enabled(&self, dir: PathBuf) -> DetectorResult<()> {
		if self.config.disable_auto_cache {
			return Ok(()); // Silently skip cache save for test configurations
		}
		self.save_cache_all(dir)
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

	/// Clear all detector state (files, relations, etc.) for a fresh start
	pub fn clear_state(&mut self) {
		info!("Detector: clearing all state");
		self.state = ScanState::new().expect("Failed to create new ScanState");
		self.relations = RelationStore::new().expect("Failed to create new RelationStore");
		// Clear any pending systems in the scheduler
		self.scheduler = SystemScheduler::new();
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

	/// Get file information filtered by path prefix, sorted by size (descending)
	/// Returns a vector of (path, size, file_type, hashed) tuples
	pub fn files_under_prefix_sorted_by_size<S: AsRef<str>>(&self, prefix: S) -> Vec<(String, u64, String, bool)> {
		use polars::prelude::*;
		let pref = prefix.as_ref();

		let result = self.state
			.data
			.clone()
			.lazy()
			.filter(col("path").str().starts_with(lit(pref)))
			.select([
				col("path"),
				col("size"),
				col("file_type"),
				col("hashed")
			])
			.collect();

		match result {
			Ok(df) => {
				let mut files = Vec::new();
				if df.height() > 0 {
					let paths = df.column("path").unwrap().str().unwrap();
					let sizes = df.column("size").unwrap().u64().unwrap();
					let file_types = df.column("file_type").unwrap().str().unwrap();
					let hashed_flags = df.column("hashed").unwrap().bool().unwrap();

					for (((path_opt, size_opt), file_type_opt), hashed_opt) in paths
						.into_iter()
						.zip(sizes.into_iter())
						.zip(file_types.into_iter())
						.zip(hashed_flags.into_iter())
					{
						if let (Some(path), Some(size), Some(file_type), Some(hashed)) =
							(path_opt, size_opt, file_type_opt, hashed_opt) {
							files.push((path.to_string(), size, file_type.to_string(), hashed));
						}
					}
				}
				// Sort by size in descending order
				files.sort_by(|a, b| b.1.cmp(&a.1));
				files
			}
			Err(_) => Vec::new(),
		}
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
		let cached_sizes = df.column("size")?.u64()?; // size is u64 in schema
		let cached_mtimes = df.column("modified")?.i64()?; // column name is "modified", not "modified_time"
		let hashed_flags = df.column("hashed")?.bool()?;

		for (((path_opt, cached_size_opt), cached_mtime_opt), hashed_opt) in paths
			.into_iter()
			.zip(cached_sizes.into_iter())
			.zip(cached_mtimes.into_iter())
			.zip(hashed_flags.into_iter())
		{
			if let (Some(path_str), Some(cached_size), Some(cached_mtime), Some(is_hashed)) =
				(path_opt, cached_size_opt, cached_mtime_opt, hashed_opt)
			{
				let path = Path::new(path_str);

				// Check if file still exists
				match fs::metadata(path) {
					Ok(metadata) => {
						let current_size = metadata.len(); // Keep as u64 to match schema

						// Use the same timestamp conversion as discovery system for consistency
						let current_mtime = match metadata.modified() {
							Ok(system_time) => {
								let datetime = chrono::DateTime::<chrono::Utc>::from(system_time);
								datetime.timestamp_nanos_opt().unwrap_or(0)
							}
							Err(_) => 0, // Default to epoch if timestamp unavailable
						};

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

		// Apply both masks in a single operation to avoid length mismatches
		if keep_mask.iter().any(|&x| !x) || invalidate_mask.iter().any(|&x| x) {
			let keep_series = Series::new("keep", keep_mask.clone());
			let invalidate_series = Series::new("invalidate", invalidate_mask.clone());

			self.state.data = self
				.state
				.data
				.clone()
				.lazy()
				// First apply invalidation to mark files for re-hashing
				.with_columns([when(lit(invalidate_series))
					.then(lit(false))
					.otherwise(col("hashed"))
					.alias("hashed")])
				// Then filter out deleted files
				.filter(lit(keep_series))
				.collect()?;
		}

		info!(
			"Cache validation: {} files removed, {} files marked for re-processing",
			files_removed, files_invalidated
		);
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

#[cfg(test)]
mod tests {
	use super::*;
	use tempfile::TempDir;

	#[test]
	fn test_detector_config_default() {
		let config = DetectorConfig::default();
		assert!(config.memory_settings.max_total_loaded_bytes > 0);
		assert!(config.memory_settings.num_max_loaded_files > 0);
		assert!(!config.disable_auto_cache); // Default should allow auto cache
	}

	#[test]
	fn test_detector_config_for_testing() {
		let config = DetectorConfig::for_testing();
		assert!(config.memory_settings.max_total_loaded_bytes > 0);
		assert!(config.memory_settings.num_max_loaded_files > 0);
		assert!(config.disable_auto_cache); // Test config should disable auto cache
	}

	#[test]
	fn test_detector_creation() {
		let config = DetectorConfig::for_testing();
		let detector = DuplicateDetector::new(config);
		assert!(detector.is_ok());

		let detector = detector.unwrap();
		assert_eq!(detector.state.data.height(), 0);
	}

	#[test]
	fn test_detector_with_cache() {
		// Test the basic detector creation without touching real cache
		// We avoid calling new_with_cache() in tests since it tries to load from real cache
		let config = DetectorConfig::for_testing();

		// Create a detector normally and verify it works
		let detector = DuplicateDetector::new(config);
		assert!(detector.is_ok());

		let detector = detector.unwrap();
		// Note: detector might have default scan_id of 1, so we just check it's created
		assert!(detector.state.scan_id >= 1);

		// Test that the config is properly set to disable auto cache
		assert!(detector.config.disable_auto_cache);
	}

	#[test]
	fn test_detector_with_safe_cache_dir() {
		// Test cache functionality using a temporary directory
		let temp_dir = TempDir::new().unwrap();
		let cache_dir = temp_dir.path().join("cache");

		let config = DetectorConfig::for_testing();

		// Test creating detector with non-existent cache directory
		let detector = DuplicateDetector::new_with_cache_dir(config, cache_dir);
		assert!(detector.is_ok());

		let detector = detector.unwrap();
		assert!(detector.config.disable_auto_cache);
		assert_eq!(detector.state.data.height(), 0);
	}

	#[test]
	fn test_cache_operations() {
		let temp_dir = TempDir::new().unwrap();
		let cache_dir = temp_dir.path().join("cache");

		let config = DetectorConfig::for_testing();
		let mut detector = DuplicateDetector::new(config).unwrap();

		// Initially no cache
		let loaded = detector.load_cache_all(cache_dir.clone()).unwrap();
		assert!(!loaded);

		// Save empty cache
		let result = detector.save_cache_all(cache_dir.clone());
		assert!(result.is_ok());

		// Load cache back
		let loaded = detector.load_cache_all(cache_dir).unwrap();
		assert!(loaded);
	}

	#[test]
	fn test_query_interface() {
		let config = DetectorConfig::for_testing();
		let detector = DuplicateDetector::new(config).unwrap();

		let _query = detector.query();
		// Query should be created successfully
		// We don't test query functionality here as it's tested separately
	}

	#[test]
	fn test_clear_state() {
		let config = DetectorConfig::for_testing();
		let mut detector = DuplicateDetector::new(config).unwrap();

		// Add some dummy data to state
		detector.state.scan_id = 42;

		detector.clear_state();

		// State should be reset
		assert_eq!(detector.state.scan_id, 1); // New state starts with scan_id 1
		assert_eq!(detector.state.data.height(), 0);
	}
}
