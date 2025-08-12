//! Common path helpers for cache locations

use std::path::PathBuf;
use dirs::cache_dir;

/// Get the default cache directory for uncp, e.g.:
/// - Linux: ~/.cache/uncp
/// - macOS: ~/Library/Caches/uncp
/// - Windows: %LOCALAPPDATA%\uncp
pub fn default_cache_dir() -> Option<PathBuf> {
    cache_dir().map(|mut p| { p.push("uncp"); p })
}
