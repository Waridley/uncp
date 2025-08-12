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
