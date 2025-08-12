//! Memory management with LRU caching and configurable limits

use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::System;

use crate::error::{DetectorError, DetectorResult};
use tracing::{debug, trace};

/// File contents stored in memory
#[derive(Debug, Clone)]
pub struct FileContents {
	pub data: Vec<u8>,
	pub size: usize,
}

impl FileContents {
	pub fn new(data: Vec<u8>) -> Self {
		let size = data.len();
		Self { data, size }
	}
}

/// Memory manager with LRU caching and configurable limits
#[derive(Debug)]
pub struct MemoryManager {
	/// Maximum bytes allowed in memory
	max_bytes: usize,
	/// Current memory usage in bytes
	current_bytes: AtomicUsize,
	/// LRU cache for file contents (lock-free optimizations welcome)
	file_cache: LruCache<PathBuf, FileContents>,
	/// Maximum number of files to keep in cache
	max_files: usize,
}

impl MemoryManager {
	/// Create a new MemoryManager with system-based defaults
	pub fn new() -> DetectorResult<Self> {
		let settings = Settings::from_sysinfo()?;
		Self::with_settings(settings)
	}

	/// Create a MemoryManager with custom settings
	pub fn with_settings(settings: Settings) -> DetectorResult<Self> {
		let cache_capacity = NonZeroUsize::new(settings.num_max_loaded_files)
			.ok_or_else(|| DetectorError::Config("max_files must be greater than 0".to_string()))?;

		debug!(
			"Memory: init with max_bytes={} max_files={}",
			settings.max_total_loaded_bytes, settings.num_max_loaded_files
		);

		Ok(MemoryManager {
			max_bytes: settings.max_total_loaded_bytes,
			current_bytes: AtomicUsize::new(0),
			file_cache: LruCache::new(cache_capacity),
			max_files: settings.num_max_loaded_files,
		})
	}

	/// Check if we can allocate the requested bytes
	pub fn can_allocate(&self, bytes: usize) -> bool {
		let current = self.current_bytes.load(Ordering::Relaxed);
		current + bytes <= self.max_bytes
	}

	/// Try to allocate memory, evicting LRU items if necessary
	pub fn try_allocate(&mut self, bytes: usize) -> DetectorResult<()> {
		// First check if we can allocate without eviction
		if self.can_allocate(bytes) {
			self.current_bytes.fetch_add(bytes, Ordering::Relaxed);
			return Ok(());
		}
		trace!("Memory: attempting to free for {} bytes", bytes);

		// Try to free memory by evicting LRU items
		let mut freed = 0;
		while !self.can_allocate(bytes - freed) && !self.file_cache.is_empty() {
			if let Some((_, contents)) = self.file_cache.pop_lru() {
				freed += contents.size;
				self.current_bytes
					.fetch_sub(contents.size, Ordering::Relaxed);
			}
		}

		// Check if we have enough memory now
		if self.can_allocate(bytes - freed) {
			self.current_bytes
				.fetch_add(bytes - freed, Ordering::Relaxed);
			Ok(())
		} else {
			Err(DetectorError::MemoryExhausted {
				requested: bytes,
				limit: self.max_bytes,
			})
		}
	}

	/// Deallocate memory
	pub fn deallocate(&self, bytes: usize) {
		self.current_bytes.fetch_sub(bytes, Ordering::Relaxed);
	}

	/// Get file contents from cache
	pub fn get_file(&mut self, path: &PathBuf) -> Option<&FileContents> {
		self.file_cache.get(path)
	}

	/// Put file contents in cache
	pub fn put_file(&mut self, path: PathBuf, contents: FileContents) -> DetectorResult<()> {
		// Try to allocate memory for the new file
		self.try_allocate(contents.size)?;

		// If cache is at capacity, remove LRU item
		if self.file_cache.len() >= self.max_files {
			if let Some((_, old_contents)) = self.file_cache.pop_lru() {
				self.deallocate(old_contents.size);
			}
		}

		self.file_cache.put(path, contents);
		Ok(())
	}

	/// Load file contents into cache
	pub async fn load_file(&mut self, path: PathBuf) -> DetectorResult<&FileContents> {
		// Check if already in cache
		if self.file_cache.contains(&path) {
			return Ok(self.file_cache.get(&path).unwrap());
		}

		// Load file from disk
		let data = smol::fs::read(&path).await?;
		let contents = FileContents::new(data);

		// Put in cache
		self.put_file(path.clone(), contents)?;

		// Return reference to cached contents
		Ok(self.file_cache.get(&path).unwrap())
	}

	/// Get current memory usage
	pub fn current_usage(&self) -> usize {
		self.current_bytes.load(Ordering::Relaxed)
	}

	/// Get memory limit
	pub fn memory_limit(&self) -> usize {
		self.max_bytes
	}

	/// Get cache statistics
	pub fn cache_stats(&self) -> CacheStats {
		CacheStats {
			current_files: self.file_cache.len(),
			max_files: self.max_files,
			current_bytes: self.current_usage(),
			max_bytes: self.max_bytes,
			hit_ratio: 0.0, // Would need to track hits/misses for real implementation
		}
	}

	/// Clear all cached files
	pub fn clear_cache(&mut self) {
		self.file_cache.clear();
		self.current_bytes.store(0, Ordering::Relaxed);
	}
}

/// Memory management settings
#[derive(Debug, Clone)]
pub struct Settings {
	/// Maximum total bytes to load into memory
	pub max_total_loaded_bytes: usize,
	/// Maximum number of files to keep in cache
	pub num_max_loaded_files: usize,
}

impl Settings {
	/// Create settings based on system information
	pub fn from_sysinfo() -> DetectorResult<Self> {
		let mut sys = System::new_all();
		sys.refresh_memory();

		let available_memory = sys.available_memory() as usize;

		// Use half of available memory, but at least 100MB and at most 4GB
		let max_total_loaded_bytes = (available_memory / 2)
			.max(100 * 1024 * 1024) // 100MB minimum
			.min(4 * 1024 * 1024 * 1024); // 4GB maximum

		// One file per CPU core, but at least 4 and at most 1000
		let num_cpus = sys.cpus().len();
		let num_max_loaded_files = num_cpus.max(4).min(1000);

		Ok(Settings {
			max_total_loaded_bytes,
			num_max_loaded_files,
		})
	}

	/// Create custom settings
	pub fn new(max_total_loaded_bytes: usize, num_max_loaded_files: usize) -> Self {
		Settings {
			max_total_loaded_bytes,
			num_max_loaded_files,
		}
	}
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
	pub current_files: usize,
	pub max_files: usize,
	pub current_bytes: usize,
	pub max_bytes: usize,
	pub hit_ratio: f64,
}

impl std::fmt::Display for CacheStats {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"Cache: {}/{} files, {:.1}MB/{:.1}MB, {:.1}% hit ratio",
			self.current_files,
			self.max_files,
			self.current_bytes as f64 / (1024.0 * 1024.0),
			self.max_bytes as f64 / (1024.0 * 1024.0),
			self.hit_ratio * 100.0
		)
	}
}
