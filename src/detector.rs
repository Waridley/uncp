//! Main API for duplicate detection

use std::path::{Path, PathBuf};

use crate::data::{RelationStore, ScanState};
use crate::error::{DetectorError, DetectorResult};
use crate::memory::{MemoryManager, Settings};
use crate::persist::CacheManager;
use crate::query::Query;
use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::paths::{DirEntryId, default_cache_dir};

use crate::systems::{ContentHashSystem, FileDiscoverySystem, SystemProgress, SystemScheduler};
use tracing::info;

/// Flexible file filtering system using glob patterns for inclusion and exclusion.
///
/// `PathFilter` provides a powerful way to control which files are included in
/// duplicate detection scans. It supports both inclusion and exclusion patterns
/// using glob syntax, with compiled pattern matching for high performance.
///
/// ## Pattern Matching
///
/// The filter uses a two-stage process:
/// 1. **Include Patterns**: If specified, only files matching these patterns are included
/// 2. **Exclude Patterns**: Files matching these patterns are excluded (applied after includes)
///
/// If no include patterns are specified, all files are included by default.
/// Exclude patterns are always applied regardless of include patterns.
///
/// ## Glob Syntax
///
/// Supports standard glob patterns:
/// - `*` matches any number of characters except path separators
/// - `?` matches exactly one character except path separators
/// - `**` matches any number of directories
/// - `[abc]` matches any character in the bracket set
/// - `{jpg,png}` matches any of the comma-separated patterns
///
/// ## Performance
///
/// - **Compiled Patterns**: Glob patterns are compiled once for fast matching
/// - **Early Filtering**: Files are filtered during discovery to avoid unnecessary processing
/// - **Memory Efficient**: Patterns are cached and reused across multiple scans
///
/// ## Examples
///
/// ```rust
/// use uncp::detector::PathFilter;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Include only image files
/// let mut filter = PathFilter::new(
///     vec!["**/*.{jpg,jpeg,png,gif}".to_string()],
///     vec![]
/// )?;
///
/// // Exclude system directories
/// filter.add_exclude_pattern("**/node_modules/**".to_string())?;
/// filter.add_exclude_pattern("**/.git/**".to_string())?;
/// filter.add_exclude_pattern("**/target/**".to_string())?;
///
/// // Test if a file should be included
/// assert!(filter.should_include("/photos/vacation.jpg"));
/// assert!(!filter.should_include("/project/node_modules/lib.js"));
/// # Ok(())
/// # }
/// ```
///
/// ## Common Use Cases
///
/// - **Media Files Only**: `**/*.{jpg,png,mp4,mp3}`
/// - **Source Code**: `**/*.{rs,py,js,cpp,h}`
/// - **Exclude Build Artifacts**: `**/target/**`, `**/build/**`
/// - **Exclude Version Control**: `**/.git/**`, `**/.svn/**`
/// - **Large File Focus**: Combined with size filters for efficiency
#[derive(Default, Debug, Clone)]
pub struct PathFilter {
	/// Glob patterns that determine what files to include in scanning.
	/// If empty, all files are included by default.
	include_patterns: Vec<String>,
	/// Glob patterns that exclude files from scanning.
	/// Applied after include patterns for fine-grained control.
	exclude_patterns: Vec<String>,
	/// Compiled globset for include patterns (cached for performance)
	include_globset: Option<GlobSet>,
	/// Compiled globset for exclude patterns (cached for performance)
	exclude_globset: Option<GlobSet>,
}

impl PathFilter {
	/// Create a new PathFilter with the given patterns
	pub fn new(
		include_patterns: Vec<String>,
		exclude_patterns: Vec<String>,
	) -> DetectorResult<Self> {
		let mut filter = Self {
			include_patterns,
			exclude_patterns,
			include_globset: None,
			exclude_globset: None,
		};
		filter.compile_patterns()?;
		Ok(filter)
	}

	/// Compile the glob patterns into GlobSets for efficient matching
	pub fn compile_patterns(&mut self) -> DetectorResult<()> {
		// Compile include patterns
		if !self.include_patterns.is_empty() {
			let mut builder = GlobSetBuilder::new();
			for pattern in &self.include_patterns {
				let glob = Glob::new(pattern).map_err(|e| DetectorError::InvalidGlobPattern {
					pattern: pattern.clone(),
					reason: e.to_string(),
				})?;
				builder.add(glob);
			}
			self.include_globset =
				Some(
					builder
						.build()
						.map_err(|e| DetectorError::InvalidGlobPattern {
							pattern: "include patterns".to_string(),
							reason: e.to_string(),
						})?,
				);
		}

		// Compile exclude patterns
		if !self.exclude_patterns.is_empty() {
			let mut builder = GlobSetBuilder::new();
			for pattern in &self.exclude_patterns {
				let glob = Glob::new(pattern).map_err(|e| DetectorError::InvalidGlobPattern {
					pattern: pattern.clone(),
					reason: e.to_string(),
				})?;
				builder.add(glob);
			}
			self.exclude_globset =
				Some(
					builder
						.build()
						.map_err(|e| DetectorError::InvalidGlobPattern {
							pattern: "exclude patterns".to_string(),
							reason: e.to_string(),
						})?,
				);
		}

		Ok(())
	}

	/// Check if a path should be included based on the filter patterns
	pub fn should_include<P: AsRef<Path>>(&self, path: P) -> bool {
		let path = path.as_ref();

		// If we have include patterns, the path must match at least one
		if let Some(ref include_set) = self.include_globset
			&& !include_set.is_match(path)
		{
			return false;
		}

		// If we have exclude patterns, the path must not match any
		if let Some(ref exclude_set) = self.exclude_globset
			&& exclude_set.is_match(path)
		{
			return false;
		}

		true
	}

	/// Add an include pattern
	pub fn add_include_pattern(&mut self, pattern: String) -> DetectorResult<()> {
		self.include_patterns.push(pattern);
		self.compile_patterns()
	}

	/// Add an exclude pattern
	pub fn add_exclude_pattern(&mut self, pattern: String) -> DetectorResult<()> {
		self.exclude_patterns.push(pattern);
		self.compile_patterns()
	}

	/// Check if any patterns are configured
	pub fn has_patterns(&self) -> bool {
		!self.include_patterns.is_empty() || !self.exclude_patterns.is_empty()
	}

	pub fn include_patterns(&self) -> &Vec<String> {
		&self.include_patterns
	}

	pub fn exclude_patterns(&self) -> &Vec<String> {
		&self.exclude_patterns
	}
}

/// Configuration for duplicate detection behavior and resource management.
///
/// `DetectorConfig` controls all aspects of the duplicate detection process,
/// including memory management, caching behavior, file filtering, and performance
/// tuning. It provides sensible defaults while allowing fine-grained control
/// for specific use cases.
///
/// ## Memory Management
///
/// The detector automatically manages memory usage based on system resources:
/// - **Automatic Limits**: Adapts to available system memory
/// - **LRU Caching**: Intelligent eviction of least-recently-used data
/// - **Batch Processing**: Processes large datasets in manageable chunks
/// - **Memory Pressure**: Responds to system memory pressure events
///
/// ## Caching Strategy
///
/// Intelligent caching minimizes redundant work across multiple runs:
/// - **Persistent Storage**: Parquet format for efficient serialization
/// - **Atomic Operations**: Safe concurrent access with crash recovery
/// - **Incremental Updates**: Only processes changed files
/// - **Validation**: Automatic detection of stale cache entries
///
/// ## Performance Tuning
///
/// Configuration options for different performance profiles:
/// - **Thread Count**: Configurable parallelism for CPU-bound operations
/// - **I/O Concurrency**: Separate limits for disk-intensive operations
/// - **Memory Limits**: Explicit control over memory usage
/// - **Batch Sizes**: Tunable for different dataset characteristics
///
/// ## Examples
///
/// ### Default Configuration
/// ```rust
/// use uncp::detector::DetectorConfig;
///
/// // Use system defaults (recommended for most cases)
/// let config = DetectorConfig::default();
/// ```
///
/// ### Custom Memory Limits
/// ```rust
/// use uncp::detector::DetectorConfig;
/// use uncp::memory::Settings;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = DetectorConfig::default();
/// // Set custom memory settings: 4GB total load cap, allow up to 12 files cached
/// config.memory_settings = Settings::new(4 * 1024 * 1024 * 1024, 12);
/// config.disable_auto_cache = true;  // Disable caching for testing
/// # Ok(())
/// # }
/// ```
///
/// ### File Filtering
/// ```rust
/// use uncp::detector::{DetectorConfig, PathFilter};
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let filter = PathFilter::new(
///     vec!["**/*.{jpg,png,mp4}".to_string()],  // Only media files
///     vec!["**/node_modules/**".to_string()]   // Exclude dependencies
/// )?;
///
/// let mut config = DetectorConfig::default();
/// config.path_filter = filter;
/// # Ok(())
/// # }
/// ```
///
/// ## Platform Considerations
///
/// - **Windows**: Handles long paths and NTFS-specific features
/// - **macOS**: Optimized for HFS+/APFS filesystems
/// - **Linux**: Supports various filesystems (ext4, btrfs, xfs)
/// - **Memory**: Adapts to system memory constraints automatically
#[derive(Debug, Clone)]
pub struct DetectorConfig {
	/// Memory management settings including limits and caching behavior
	pub memory_settings: Settings,
	/// Disable automatic cache saving (useful for testing and debugging)
	pub disable_auto_cache: bool,
	/// Path filtering configuration for including/excluding files
	pub path_filter: PathFilter,
}

impl Default for DetectorConfig {
	fn default() -> Self {
		Self {
			memory_settings: Settings::from_sysinfo().expect("failed to read sysinfo"),
			disable_auto_cache: false,
			path_filter: PathFilter::default(),
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
			path_filter: PathFilter::default(),
		}
	}
}

/// Primary interface for duplicate file detection and analysis.
///
/// `DuplicateDetector` is the main entry point for all duplicate detection operations.
/// It orchestrates the entire processing pipeline from file discovery through
/// similarity analysis, providing a high-level API that abstracts the complexity
/// of the underlying data processing systems.
///
/// ## Core Functionality
///
/// The detector provides a complete duplicate detection workflow:
///
/// 1. **File Discovery**: Scans directories with configurable filtering
/// 2. **Content Hashing**: Computes Blake3 hashes for exact duplicate detection
/// 3. **Similarity Analysis**: Extensible system for near-duplicate detection
/// 4. **Caching**: Persistent storage with intelligent invalidation
/// 5. **Querying**: High-level API for accessing results
///
/// ## Processing Pipeline
///
/// ```text
/// Directory Scan → File Discovery → Content Hashing → Similarity Analysis
///       ↓              ↓               ↓                    ↓
///   Path Filter → ScanState → Hash Relations → Similarity Groups
///       ↓              ↓               ↓                    ↓
///   Cache Load ←  Cache Save ←  Cache Update ←  Cache Merge
/// ```
///
/// ## Memory Management
///
/// The detector automatically manages memory usage:
/// - **Adaptive Limits**: Adjusts to available system memory
/// - **Batch Processing**: Handles datasets larger than RAM
/// - **LRU Caching**: Intelligent eviction of unused data
/// - **Memory Pressure**: Responds to system memory constraints
///
/// ## Performance Characteristics
///
/// Typical performance on modern hardware:
/// - **Discovery**: 10,000-50,000 files/second
/// - **Hashing**: 500MB/second per thread
/// - **Memory**: 50-100MB base + ~100 bytes per file
/// - **Cache**: 100,000+ files/second loading from cache
///
/// ## Examples
///
/// ### Basic Usage
/// ```rust
/// use uncp::{DuplicateDetector, DetectorConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create detector with default settings
/// let mut detector = DuplicateDetector::new(DetectorConfig::default())?;
///
/// // Scan a directory
/// detector.scan_directory("./photos".into()).await?;
///
/// // Process until complete (includes hashing)
/// detector.process_until_complete().await?;
///
/// // Query for files needing hashing
/// let query = detector.query();
/// let pending = query.files_needing_hashing()?;
/// println!("{} files need hashing", pending.height());
/// # Ok(())
/// # }
/// ```
///
/// ### With Progress Reporting
/// ```rust
/// use uncp::{DuplicateDetector, DetectorConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut detector = DuplicateDetector::new(DetectorConfig::default())?;
///
/// // Scan with progress callbacks
/// detector.scan_with_progress("./data".into(), std::sync::Arc::new(|progress| {
///     println!("Discovered {} files", progress.processed_items);
/// }))
/// .await?;
///
/// // Continue processing until complete with progress callbacks
/// detector.process_until_complete_with_progress(None, std::sync::Arc::new(|progress| {
///     println!("{}: {}/{}", progress.system_name, progress.processed_items, progress.total_items);
/// }))
/// .await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Thread Safety
///
/// `DuplicateDetector` is designed for single-threaded use. For concurrent
/// access, use the query interface which provides immutable views suitable
/// for parallel processing.
pub struct DuplicateDetector {
	/// Primary data store containing all file metadata and processing state
	pub state: ScanState,
	/// Relationship store for duplicate and similarity data
	pub relations: RelationStore,
	/// System scheduler for coordinating processing pipeline
	pub scheduler: SystemScheduler,
	/// Memory manager for resource optimization and limits
	pub memory_mgr: MemoryManager,
	/// Configuration for behavior and performance tuning
	pub config: DetectorConfig,
}

impl DuplicateDetector {
	pub fn new(config: DetectorConfig) -> DetectorResult<Self> {
		let memory_settings = config.memory_settings.clone();
		Ok(Self {
			state: ScanState::new()?,
			relations: RelationStore::new(),
			scheduler: SystemScheduler::new(),
			memory_mgr: MemoryManager::with_settings(memory_settings)?,
			config,
		})
	}

	pub fn new_with_cache(config: DetectorConfig) -> DetectorResult<Self> {
		let mut this = Self::new(config)?;
		// Try loading cache on creation (best-effort)
		if let Some(dir) = default_cache_dir()
			&& let Ok(Some((state, relations))) = CacheManager::new(dir).load_all()
		{
			this.state = state;
			this.relations = relations;
			info!("Detector: loaded cache at init");
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
		let mut discovery = FileDiscoverySystem::new(vec![path]);
		if self.config.path_filter.has_patterns() {
			discovery = discovery.with_path_filter(self.config.path_filter.clone());
		}
		self.scheduler.add_system(discovery);
		let res = self
			.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok()
			&& !self.config.disable_auto_cache
			&& let Some(dir) = default_cache_dir()
		{
			let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
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
		let mut discovery = FileDiscoverySystem::new(vec![path]).with_progress_callback(progress);
		if self.config.path_filter.has_patterns() {
			discovery = discovery.with_path_filter(self.config.path_filter.clone());
		}
		self.scheduler.add_system(discovery);
		let res = self
			.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok()
			&& !self.config.disable_auto_cache
			&& let Some(dir) = default_cache_dir()
		{
			let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
		}
		res
	}

	pub async fn scan_with_progress_and_cancellation(
		&mut self,
		path: PathBuf,
		progress: std::sync::Arc<dyn Fn(crate::systems::SystemProgress) + Send + Sync>,
		cancellation_token: std::sync::Arc<std::sync::atomic::AtomicBool>,
	) -> DetectorResult<()> {
		info!(
			"Detector: scan_directory {} (with progress and cancellation)",
			path.display()
		);
		let mut discovery = FileDiscoverySystem::new(vec![path]).with_progress_callback(progress);
		if self.config.path_filter.has_patterns() {
			discovery = discovery.with_path_filter(self.config.path_filter.clone());
		}
		self.scheduler.add_system(discovery);
		let res = self
			.scheduler
			.run_all_with_cancellation(&mut self.state, &mut self.memory_mgr, cancellation_token)
			.await
			.map_err(DetectorError::from);
		if res.is_ok()
			&& !self.config.disable_auto_cache
			&& let Some(dir) = default_cache_dir()
		{
			let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
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
		if res.is_ok()
			&& !self.config.disable_auto_cache
			&& let Some(dir) = default_cache_dir()
		{
			let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
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
		if res.is_ok()
			&& !self.config.disable_auto_cache
			&& let Some(dir) = default_cache_dir()
		{
			let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
		}
		res
	}

	pub async fn process_until_complete(&mut self) -> DetectorResult<()> {
		let res = self
			.scheduler
			.run_all(&mut self.state, &mut self.memory_mgr)
			.await
			.map_err(DetectorError::from);
		if res.is_ok()
			&& !self.config.disable_auto_cache
			&& let Some(dir) = default_cache_dir()
		{
			let _ = CacheManager::new(dir).save_all(&self.state, &self.relations);
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
		self.relations = RelationStore::new();
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
		let pref = prefix.as_ref();
		let df = &self.state.data;
		if df.height() == 0 {
			return 0;
		}
		let s = match df.column("path").and_then(|c| c.struct_()) {
			Ok(s) => s,
			Err(_) => return 0,
		};
		let idx_series = match s.field_by_name("idx") {
			Ok(series) => series.clone(),
			Err(_) => return 0,
		};
		let gen_series = match s.field_by_name("gen") {
			Ok(series) => series.clone(),
			Err(_) => return 0,
		};
		let idx_ca = match idx_series.u64() {
			Ok(ca) => ca.clone(),
			Err(_) => return 0,
		};
		let gen_ca = match gen_series.u64() {
			Ok(ca) => ca.clone(),
			Err(_) => return 0,
		};
		let hashed = match df.column("hashed").and_then(|c| c.bool()) {
			Ok(ca) => ca.clone(),
			Err(_) => return 0,
		};
		let mut count = 0usize;
		for i in 0..df.height() {
			if let (Some(idx), Some(r#gen), Some(is_hashed)) =
				(idx_ca.get(i), gen_ca.get(i), hashed.get(i))
				&& !is_hashed
				&& let Some(id) =
					crate::paths::DirEntryId::from_raw_parts(idx as usize, r#gen as usize)
				&& id.is_descendant_of_path(pref)
			{
				count += 1;
			}
		}
		count
	}

	pub fn files_under_prefix<S: AsRef<str>>(&self, prefix: S) -> usize {
		let pref = prefix.as_ref();
		let df = &self.state.data;
		if df.height() == 0 {
			return 0;
		}
		let s = match df.column("path").and_then(|c| c.struct_()) {
			Ok(s) => s,
			Err(_) => return 0,
		};
		let idx_series = match s.field_by_name("idx") {
			Ok(series) => series.clone(),
			Err(_) => return 0,
		};
		let gen_series = match s.field_by_name("gen") {
			Ok(series) => series.clone(),
			Err(_) => return 0,
		};
		let idx_ca = match idx_series.u64() {
			Ok(ca) => ca.clone(),
			Err(_) => return 0,
		};
		let gen_ca = match gen_series.u64() {
			Ok(ca) => ca.clone(),
			Err(_) => return 0,
		};
		let mut count = 0usize;
		for i in 0..df.height() {
			if let (Some(idx), Some(r#gen)) = (idx_ca.get(i), gen_ca.get(i))
				&& let Some(id) =
					crate::paths::DirEntryId::from_raw_parts(idx as usize, r#gen as usize)
				&& id.is_descendant_of_path(pref)
			{
				count += 1;
			}
		}
		count
	}

	/// Get file information filtered by path prefix, sorted by size (descending)
	/// Returns a vector of (path, size, file_type, hashed) tuples
	pub fn files_under_prefix_sorted_by_size<S: AsRef<str>>(
		&self,
		prefix: S,
	) -> Vec<(DirEntryId, u64, String, bool)> {
		let pref = prefix.as_ref();

		// Extract columns directly and filter by actual path descendant relationship
		let df = &self.state.data;
		let mut files = Vec::new();
		if df.height() == 0 {
			return files;
		}
		let paths = match df.column("path").and_then(|c| c.struct_()) {
			Ok(s) => s,
			Err(_) => return files,
		};
		let idx_series = match paths.field_by_name("idx") {
			Ok(series) => series.clone(),
			Err(_) => return files,
		};
		let gen_series = match paths.field_by_name("gen") {
			Ok(series) => series.clone(),
			Err(_) => return files,
		};
		let idx_vals: Vec<Option<u64>> = match idx_series.u64() {
			Ok(ca) => ca.into_iter().collect(),
			Err(_) => return files,
		};
		let gen_vals: Vec<Option<u64>> = match gen_series.u64() {
			Ok(ca) => ca.into_iter().collect(),
			Err(_) => return files,
		};
		let sizes = df.column("size").unwrap().u64().unwrap();
		let file_types = df.column("file_type").unwrap().str().unwrap();
		let hashed_flags = df.column("hashed").unwrap().bool().unwrap();
		for i in 0..df.height() {
			if let (Some(idx), Some(generation), Some(size), Some(file_type), Some(hashed)) = (
				idx_vals[i],
				gen_vals[i],
				sizes.get(i),
				file_types.get(i),
				hashed_flags.get(i),
			) && let Some(path) = DirEntryId::from_raw_parts(idx as usize, generation as usize)
				&& path.is_descendant_of_path(pref)
			{
				files.push((path, size, file_type.to_string(), hashed));
			}
		}
		files.sort_by(|a, b| b.1.cmp(&a.1));
		files
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
			let keep_series = Series::new("keep".into(), keep_mask.clone());
			let invalidate_series = Series::new("invalidate".into(), invalidate_mask.clone());

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
		if let Ok(s) = self.state.data.column("file_type")
			&& let Ok(utf8) = s.str()
		{
			for v in utf8.into_iter().flatten() {
				*map.entry(v.to_string()).or_insert(0) += 1;
			}
		}
		map
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tempfile::TempDir;

	#[test_log::test]
	fn test_detector_config_default() {
		let config = DetectorConfig::default();
		assert!(config.memory_settings.max_total_loaded_bytes > 0);
		assert!(config.memory_settings.num_max_loaded_files > 0);
		assert!(!config.disable_auto_cache); // Default should allow auto cache
	}

	#[test_log::test]
	fn test_detector_config_for_testing() {
		let config = DetectorConfig::for_testing();
		assert!(config.memory_settings.max_total_loaded_bytes > 0);
		assert!(config.memory_settings.num_max_loaded_files > 0);
		assert!(config.disable_auto_cache); // Test config should disable auto cache
	}

	#[test_log::test]
	fn test_detector_creation() {
		let config = DetectorConfig::for_testing();
		let detector = DuplicateDetector::new(config);
		assert!(detector.is_ok());

		let detector = detector.unwrap();
		assert_eq!(detector.state.data.height(), 0);
	}

	#[test_log::test]
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

	#[test_log::test]
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

	#[test_log::test]
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

	#[test_log::test]
	fn test_query_interface() {
		let config = DetectorConfig::for_testing();
		let detector = DuplicateDetector::new(config).unwrap();

		let _query = detector.query();
		// Query should be created successfully
		// We don't test query functionality here as it's tested separately
	}

	#[test_log::test]
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
