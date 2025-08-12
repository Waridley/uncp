//! Common path helpers for cache locations

use dirs::cache_dir;
use std::path::PathBuf;

/// Get the default cache directory for uncp, e.g.:
/// - Linux: ~/.cache/uncp
/// - macOS: ~/Library/Caches/uncp
/// - Windows: %LOCALAPPDATA%\uncp
pub fn default_cache_dir() -> Option<PathBuf> {
	cache_dir().map(|mut p| {
		p.push("uncp");
		p
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_default_cache_dir() {
		let cache_dir = default_cache_dir();

		// Should return Some path on most systems
		if let Some(path) = cache_dir {
			// Path should end with "uncp"
			assert_eq!(path.file_name().unwrap(), "uncp");

			// Path should be absolute
			assert!(path.is_absolute());

			// We don't check if directories exist to avoid any filesystem access
			// that might create directories or modify timestamps
		}
		// Note: On some systems (like CI environments), cache_dir() might return None
		// which is acceptable behavior
	}

	#[test]
	fn test_cache_dir_structure() {
		if let Some(cache_dir) = default_cache_dir() {
			// The cache directory should be a subdirectory of the system cache dir
			if let Some(system_cache) = dirs::cache_dir() {
				assert!(cache_dir.starts_with(system_cache));
			}
		}
	}
}
