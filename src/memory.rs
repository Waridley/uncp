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
pub struct FileBytes {
	pub data: Box<[u8]>,
}

impl FileBytes {
	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}
}

impl FileBytes {
	pub fn new(data: impl Into<Box<[u8]>>) -> Self {
		Self { data: data.into() }
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
	file_cache: LruCache<PathBuf, FileBytes>,
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
				freed += contents.len();
				self.current_bytes
					.fetch_sub(contents.len(), Ordering::Relaxed);
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
	pub fn get_file(&mut self, path: &PathBuf) -> Option<&FileBytes> {
		self.file_cache.get(path)
	}

	/// Put file contents in cache
	pub fn put_file(&mut self, path: PathBuf, contents: FileBytes) -> DetectorResult<()> {
		// Try to allocate memory for the new file
		self.try_allocate(contents.len())?;

		self.file_cache.put(path, contents);
		Ok(())
	}

	/// Load file contents into cache
	pub async fn load_file(&mut self, path: PathBuf) -> DetectorResult<&FileBytes> {
		// Check if already in cache
		if self.file_cache.contains(&path) {
			return Ok(self.file_cache.get(&path).unwrap());
		}

		// Load file from disk
		let data = smol::fs::read(&path).await?;
		let contents = FileBytes::new(data);

		// Put in cache
		self.put_file(path.clone(), contents)?;

		// Return reference to cached contents
		Ok(self.file_cache.get(&path).unwrap())
	}
}

impl MemoryManager {
	/// Remove a file from cache if present. Returns true if removed.
	pub fn remove_file(&mut self, path: &PathBuf) -> bool {
		if let Some(bytes) = self.file_cache.pop(path) {
			self.deallocate(bytes.len());
			true
		} else {
			false
		}
	}
}

impl MemoryManager {
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
			max_files: self.file_cache.cap(),
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
		let max_total_loaded_bytes =
			(available_memory / 2).clamp(100 * 1024 * 1024, 4 * 1024 * 1024 * 1024); // 100MB minimum, 4GB maximum

		// One file per CPU core, but at least 4 and at most 1000
		let num_cpus = sys.cpus().len();
		let num_max_loaded_files = num_cpus.clamp(4, 1000);

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
	pub max_files: NonZeroUsize,
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test_log::test]
	fn test_memory_manager_creation() {
		let manager = MemoryManager::new();
		assert!(manager.is_ok());
	}

	#[test_log::test]
	fn test_settings_from_sysinfo() {
		let settings = Settings::from_sysinfo();
		assert!(settings.is_ok());

		let settings = settings.unwrap();
		assert!(settings.max_total_loaded_bytes > 0);
		assert!(settings.num_max_loaded_files > 0);

		// Should have reasonable defaults
		assert!(settings.max_total_loaded_bytes >= 100 * 1024 * 1024); // At least 100MB
		assert!(settings.num_max_loaded_files >= 4); // At least 4 files
	}

	#[test_log::test]
	fn test_settings_custom() {
		let settings = Settings::new(1024 * 1024, 10); // 1MB, 10 files
		assert_eq!(settings.max_total_loaded_bytes, 1024 * 1024);
		assert_eq!(settings.num_max_loaded_files, 10);
	}

	#[test_log::test]
	fn test_cache_stats_display() {
		let stats = CacheStats {
			current_files: 5,
			max_files: NonZeroUsize::new(10).unwrap(),
			current_bytes: 1024 * 1024, // 1MB
			max_bytes: 2 * 1024 * 1024, // 2MB
			hit_ratio: 0.75,
		};

		let display = stats.to_string();
		assert!(display.contains("5/10 files"));
		assert!(display.contains("1.0MB/2.0MB"));
		assert!(display.contains("75.0% hit ratio"));
	}
}
