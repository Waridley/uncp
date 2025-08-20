//! File discovery system for scanning filesystem

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use tracing::{debug, info, trace, warn};
use walkdir::WalkDir;

use crate::data::{FileKind, FileRecord, ScanState};
use crate::detector::PathFilter;
use crate::error::{SystemError, SystemResult};
use crate::memory::MemoryManager;
use crate::systems::{
	System, SystemContext, SystemProgress, SystemRunner, yield_periodically_with_cancellation,
};

/// System for discovering files in the filesystem
pub struct FileDiscoverySystem {
	/// Optional progress callback (shared for TUI/GUI)
	pub progress_callback: Option<std::sync::Arc<dyn Fn(SystemProgress) + Send + Sync>>,
	/// Paths to scan
	pub scan_paths: Vec<PathBuf>,
	/// Whether to follow symbolic links
	pub follow_links: bool,
	/// Maximum depth to scan (None for unlimited)
	pub max_depth: Option<usize>,
	/// File extensions to include (None for all)
	pub include_extensions: Option<Vec<String>>,
	/// File extensions to exclude
	pub exclude_extensions: Vec<String>,
	/// Minimum file size to include (in bytes)
	pub min_file_size: u64,
	/// Maximum file size to include (in bytes, None for unlimited)
	pub max_file_size: Option<u64>,
	/// Glob-based path filtering
	pub path_filter: Option<PathFilter>,
}

impl FileDiscoverySystem {
	/// Create a new file discovery system
	pub fn new(scan_paths: Vec<PathBuf>) -> Self {
		Self {
			scan_paths,
			follow_links: false,
			max_depth: None,
			include_extensions: None,
			exclude_extensions: Vec::new(),
			min_file_size: 0,
			max_file_size: None,
			progress_callback: None,
			path_filter: None,
		}
	}

	/// Configure whether to follow symbolic links
	pub fn follow_links(mut self, follow: bool) -> Self {
		self.follow_links = follow;
		self
	}

	/// Set maximum scan depth
	pub fn max_depth(mut self, depth: usize) -> Self {
		self.max_depth = Some(depth);
		self
	}

	/// Set file extensions to include
	pub fn include_extensions(mut self, extensions: Vec<String>) -> Self {
		self.include_extensions = Some(extensions);
		self
	}

	/// Set file extensions to exclude
	pub fn exclude_extensions(mut self, extensions: Vec<String>) -> Self {
		self.exclude_extensions = extensions;
		self
	}

	/// Set file size limits
	pub fn file_size_range(mut self, min: u64, max: Option<u64>) -> Self {
		self.min_file_size = min;
		self.max_file_size = max;
		self
	}

	/// Attach a progress callback (TUI/GUI)
	pub fn with_progress_callback(
		mut self,
		cb: std::sync::Arc<dyn Fn(SystemProgress) + Send + Sync>,
	) -> Self {
		self.progress_callback = Some(cb);
		self
	}

	/// Set path filter for glob-based filtering
	pub fn with_path_filter(mut self, filter: PathFilter) -> Self {
		self.path_filter = Some(filter);
		self
	}

	/// Discover files in the configured paths
	pub async fn discover_files(&self, context: &SystemContext) -> SystemResult<Vec<FileRecord>> {
		info!(
			"Discovery: starting scan of {} paths",
			self.scan_paths.len()
		);
		let mut all_files = Vec::new();
		let mut progress = SystemProgress::new("FileDiscovery".to_string(), 0);
		let mut last_yield = std::time::Instant::now();

		for scan_path in &self.scan_paths {
			debug!("Discovery: scanning {}", scan_path.display());
			let files = self
				.discover_files_in_path(scan_path, context, &mut progress, &mut last_yield)
				.await?;
			all_files.extend(files);
		}

		Ok(all_files)
	}

	async fn discover_files_in_path(
		&self,
		path: &Path,
		context: &SystemContext,
		progress: &mut SystemProgress,
		last_yield: &mut std::time::Instant,
	) -> SystemResult<Vec<FileRecord>> {
		let mut files = Vec::new();

		let mut walker = WalkDir::new(path);

		if let Some(max_depth) = self.max_depth {
			walker = walker.max_depth(max_depth);
		}

		if self.follow_links {
			walker = walker.follow_links(true);
		}

		for entry in walker.into_iter() {
			// Yield control periodically and check for cancellation
			yield_periodically_with_cancellation(last_yield, context.yield_interval, context)
				.await?;

			let entry = match entry {
				Ok(e) => e,
				Err(e) => {
					warn!("FileDiscovery walk error: {}", e);
					continue;
				}
			};

			// Skip directories
			if entry.file_type().is_dir() {
				continue;
			}

			let path = entry.path();

			// Apply filters
			if !self.should_include_file(path) {
				continue;
			}

			// Get file metadata
			let metadata = match entry.metadata() {
				Ok(m) => m,
				Err(e) => {
					warn!("Skipping {} (metadata error: {})", path.display(), e);
					continue;
				}
			};

			let size = metadata.len();

			// Apply size filters
			if size < self.min_file_size {
				continue;
			}

			if let Some(max_size) = self.max_file_size
				&& size > max_size
			{
				continue;
			}

			// Convert modification time
			let modified = match metadata.modified() {
				Ok(ts) => ts,
				Err(e) => {
					warn!("Skipping {} (mtime error: {})", path.display(), e);
					continue;
				}
			};

			let modified = DateTime::<Utc>::from(modified);

			// Determine file type
			let file_type = self.determine_file_type(path);

			let file_record = FileRecord {
				path: path.to_path_buf(),
				size,
				modified,
				file_type,
			};

			files.push(file_record);
			trace!("Discovery: found {} ({} bytes)", path.display(), size);

			// Update progress
			progress.update(files.len(), Some(path.to_string_lossy().to_string()));
			// Report progress via context or attached callback
			context.report_progress(progress.clone());
			if let Some(ref cb) = self.progress_callback {
				cb(progress.clone());
			}
		}

		Ok(files)
	}

	fn should_include_file(&self, path: &Path) -> bool {
		// First check glob-based path filter if configured
		if let Some(ref filter) = self.path_filter
			&& !filter.should_include(path)
		{
			return false;
		}

		// Then check legacy extension-based filtering
		let extension = path
			.extension()
			.and_then(|ext| ext.to_str())
			.map(|s| s.to_lowercase());

		// Check exclude list first
		if let Some(ref ext) = extension
			&& self
				.exclude_extensions
				.iter()
				.any(|e| e.eq_ignore_ascii_case(ext))
		{
			return false;
		}

		// Check include list if specified
		if let Some(ref include_list) = self.include_extensions {
			if let Some(ref ext) = extension {
				return include_list.iter().any(|e| e.eq_ignore_ascii_case(ext));
			} else {
				return false; // No extension, but include list specified
			}
		}

		true
	}

	fn determine_file_type(&self, path: &Path) -> FileKind {
		// Use tree_magic_mini to determine MIME type
		let mime = tree_magic_mini::from_filepath(path);
		let mime_str: &str = mime.unwrap_or("");

		match mime_str {
			mime if mime.starts_with("text/") => FileKind::Text,
			mime if mime.starts_with("image/") => FileKind::Image,
			mime if mime.starts_with("audio/") => FileKind::Audio,
			mime if mime.starts_with("video/") => FileKind::Video,
			"application/zip"
			| "application/x-tar"
			| "application/gzip"
			| "application/x-rar-compressed" => FileKind::Archive,
			_ => {
				// Fallback to extension-based detection
				if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
					match ext.to_lowercase().as_str() {
						"txt" | "md" | "rs" | "py" | "js" | "html" | "css" | "json" | "xml"
						| "yaml" | "toml" => FileKind::Text,
						"jpg" | "jpeg" | "png" | "gif" | "bmp" | "tiff" | "webp" | "svg" => {
							FileKind::Image
						}
						"mp3" | "wav" | "flac" | "ogg" | "m4a" | "aac" => FileKind::Audio,
						"mp4" | "avi" | "mkv" | "mov" | "wmv" | "flv" | "webm" => FileKind::Video,
						"zip" | "tar" | "gz" | "bz2" | "xz" | "7z" | "rar" => FileKind::Archive,
						_ => FileKind::Binary,
					}
				} else {
					FileKind::Unknown
				}
			}
		}
	}
}

#[async_trait]
impl SystemRunner for FileDiscoverySystem {
	async fn run(
		&self,
		state: &mut ScanState,
		_memory_mgr: &mut MemoryManager,
	) -> SystemResult<()> {
		let context = SystemContext::new();
		let files = self.discover_files(&context).await?;

		state
			.add_files(files)
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().to_string(),
				reason: format!("Failed to add files to state: {}", e),
			})?;

		Ok(())
	}

	async fn run_with_cancellation(
		&self,
		state: &mut ScanState,
		_memory_mgr: &mut MemoryManager,
		cancellation_token: std::sync::Arc<std::sync::atomic::AtomicBool>,
	) -> SystemResult<()> {
		let context = SystemContext::new().with_cancellation_token(cancellation_token);
		let files = self.discover_files(&context).await?;

		state
			.add_files(files)
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().to_string(),
				reason: format!("Failed to add files to state: {}", e),
			})?;

		Ok(())
	}

	fn can_run(&self, _state: &ScanState) -> bool {
		// File discovery can always run as it creates initial data
		true
	}

	fn priority(&self) -> u8 {
		255 // Highest priority - must run first
	}

	fn name(&self) -> &'static str {
		"FileDiscovery"
	}
}

impl System for FileDiscoverySystem {
	fn required_columns(&self) -> &[&'static str] {
		&[] // Creates initial data, no requirements
	}

	fn optional_columns(&self) -> &[&'static str] {
		&[]
	}

	fn description(&self) -> &'static str {
		"Discovers files in the filesystem and populates initial metadata"
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::data::ScanState;
	use crate::memory::MemoryManager;
	use std::fs;
	use tempfile::TempDir;

	fn create_test_directory() -> TempDir {
		let temp_dir = TempDir::new().unwrap();
		let base_path = temp_dir.path();

		// Create test files
		fs::write(base_path.join("test.txt"), "Hello, world!").unwrap();
		fs::write(base_path.join("image.jpg"), b"fake image data").unwrap();
		fs::write(base_path.join("data.bin"), b"\x00\x01\x02\x03").unwrap();

		// Create subdirectory with files
		let sub_dir = base_path.join("subdir");
		fs::create_dir(&sub_dir).unwrap();
		fs::write(sub_dir.join("nested.txt"), "nested content").unwrap();

		temp_dir
	}

	#[test]
	fn test_file_discovery_system_creation() {
		let paths = vec![PathBuf::from("/test/path")];
		let discovery = FileDiscoverySystem::new(paths.clone());

		assert_eq!(discovery.scan_paths, paths);
		assert_eq!(discovery.name(), "FileDiscovery");
		assert_eq!(discovery.priority(), 255);
		assert!(discovery.can_run(&ScanState::new().unwrap()));
	}

	#[smol_potat::test]
	async fn test_file_discovery_run() {
		let temp_dir = create_test_directory();
		let discovery = FileDiscoverySystem::new(vec![temp_dir.path().to_path_buf()]);

		let mut state = ScanState::new().unwrap();
		let mut memory_mgr = MemoryManager::new().unwrap();

		let result = discovery.run(&mut state, &mut memory_mgr).await;
		assert!(result.is_ok());

		// Should have discovered files
		assert!(state.data.height() > 0);

		// Check that files were added with correct metadata
		let df = &state.data;
		assert!(df.column("path").is_ok());
		assert!(df.column("size").is_ok());
		assert!(df.column("modified").is_ok());
		assert!(df.column("file_type").is_ok());
	}

	#[test]
	fn test_system_interface() {
		let discovery = FileDiscoverySystem::new(vec![PathBuf::from("/test")]);

		assert_eq!(discovery.required_columns(), &[] as &[&str]);
		assert_eq!(discovery.optional_columns(), &[] as &[&str]);
		assert_eq!(
			discovery.description(),
			"Discovers files in the filesystem and populates initial metadata"
		);
	}
}
