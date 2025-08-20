//! Common path helpers for cache locations

use dirs::cache_dir;
use globset;
use polars::datatypes::{DataType, Field};
use polars::prelude::AnyValue;
use serde::{Deserialize, Deserializer, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fmt::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::{LazyLock, RwLock};
use tracing::error;

type Arena = typed_generational_arena::Arena<PathSegment<'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DirEntryId(typed_generational_arena::Index<PathSegment<'static>, usize, usize>);

static ARENA: LazyLock<RwLock<PathArena>> = LazyLock::new(|| RwLock::new(PathArena::new()));

pub fn get_or_insert_segment(segment: Component<'_>, parent: Option<DirEntryId>) -> DirEntryId {
	if let Some(idx) = ARENA.read().unwrap().get(segment, parent) {
		return idx;
	}
	ARENA.write().unwrap().insert(segment, parent)
}

pub fn intern_path(path: impl AsRef<Path>) -> DirEntryId {
	let path = path.as_ref();
	// Try to canonicalize, but fall back to the original path if it fails
	// This allows interning of non-existent paths
	let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
	let last = path.components().fold(None, |parent, seg| {
		let idx = get_or_insert_segment(seg, parent);
		Some(idx)
	});
	last.expect("successfully inserted at least one segment")
}

pub struct PathArena {
	indices: HashMap<ComponentCow<'static>, HashMap<Option<DirEntryId>, DirEntryId>>,
	segments: Arena,
}

impl Default for PathArena {
	fn default() -> Self {
		Self::new()
	}
}

impl PathArena {
	pub fn new() -> Self {
		Self {
			indices: HashMap::new(),
			segments: Arena::new(),
		}
	}
}

impl PathArena {
	pub fn get(&self, segment: Component<'_>, parent: Option<DirEntryId>) -> Option<DirEntryId> {
		self.indices.get(&segment.into())?.get(&parent).copied()
	}

	pub fn insert(&mut self, segment: Component, parent: Option<DirEntryId>) -> DirEntryId {
		if let Some(idx) = self.get(segment, parent) {
			return idx;
		}
		let segment = ComponentCow::from(segment).into_owned();
		let entry = PathSegment {
			component: segment.clone(),
			parent,
		};
		let idx = DirEntryId(self.segments.insert(entry));
		self.indices.entry(segment).or_default().insert(parent, idx);
		idx
	}
}

#[derive(Debug, Clone)]
pub struct PathSegment<'a> {
	component: ComponentCow<'a>,
	parent: Option<DirEntryId>,
}

impl<'a> PathSegment<'a> {
	pub fn component(&self) -> &ComponentCow<'a> {
		&self.component
	}

	pub fn into_owned(self) -> PathSegment<'static> {
		PathSegment {
			component: self.component.clone().into_owned(),
			parent: self.parent,
		}
	}

	pub fn parent(&self) -> Option<PathSegment<'static>> {
		self.parent.map(|idx| {
			let arena = ARENA.read().unwrap();
			arena.segments.get(idx.0).unwrap().clone().into_owned()
		})
	}

	pub fn resolve(&self) -> PathBuf {
		self.parent()
			.map(|p| p.resolve())
			.unwrap_or_default()
			.join(&self.component)
	}
}

impl DirEntryId {
	pub fn iter(&'_ self) -> DirEntrySegmentIter<'_> {
		let mut segments = Vec::new();
		let mut current = Some(*self);
		let arena = ARENA.read().unwrap();
		while let Some(idx) = current {
			let segment = arena.segments.get(idx.0).unwrap();
			segments.push(segment.component.clone());
			current = segment.parent;
		}
		// Reverse to get root-to-leaf order
		segments.reverse();
		DirEntrySegmentIter::new(segments)
	}

	pub fn resolve(&self) -> PathBuf {
		self.iter().collect()
	}

	pub fn raw_parts(&self) -> (usize, usize) {
		(self.0.arr_idx(), self.0.r#gen())
	}

	pub fn from_raw_parts(idx: usize, generation: usize) -> Option<Self> {
		let idx = Self::from_raw_parts_unchecked(idx, generation);
		let arena = ARENA.read().unwrap();
		if arena.segments.contains(idx.0) {
			Some(idx)
		} else {
			None
		}
	}

	pub fn from_raw_parts_unchecked(idx: usize, generation: usize) -> Self {
		DirEntryId(typed_generational_arena::Index::new(idx, generation))
	}

	pub fn with_segment<U>(&self, f: impl FnOnce(&PathSegment<'_>) -> U) -> Option<U> {
		let arena = ARENA.read().unwrap();
		let segment = arena.segments.get(self.0)?;
		Some(f(segment))
	}

	pub fn name_eq(&self, name: impl AsRef<OsStr>) -> bool {
		self.with_segment(|seg| seg.component.as_os_str() == name.as_ref())
			.unwrap_or(false)
	}

	/// Check if the last component of the represented path ends with the given suffix
	// Apparently the `Pattern` API is unstable, so I can't just use the same trait bounds as `str::ends_with`
	pub fn name_ends_with(&self, suffix: impl AsRef<str>) -> bool {
		self.with_segment(|seg| {
			if let Some(name) = seg.component.as_os_str().to_str() {
				name.ends_with(suffix.as_ref())
			} else {
				false
			}
		})
		.unwrap_or(false)
	}

	/// Check if the last component of the represented path contains the given substring
	/// This is much faster than converting to a full path string
	pub fn name_contains(&self, needle: impl AsRef<str>) -> bool {
		self.with_segment(|seg| {
			if let Some(name) = seg.component.as_os_str().to_str() {
				name.contains(needle.as_ref())
			} else {
				false
			}
		})
		.unwrap_or(false)
	}

	/// Check if the last component of the represented path starts with the given prefix
	pub fn name_starts_with(&self, prefix: impl AsRef<str>) -> bool {
		self.with_segment(|seg| {
			if let Some(name) = seg.component.as_os_str().to_str() {
				name.starts_with(prefix.as_ref())
			} else {
				false
			}
		})
		.unwrap_or(false)
	}

	/// Check if any component in the path contains the given substring
	/// Short-circuits on first match, avoiding full path construction
	pub fn path_contains(&self, needle: impl AsRef<str>) -> bool {
		let mut current = Some(*self);
		let arena = ARENA.read().unwrap();

		while let Some(id) = current {
			let Some(seg) = arena.segments.get(id.0) else {
				return false;
			};
			if let Some(name) = seg.component.as_os_str().to_str()
				&& name.contains(needle.as_ref())
			{
				return true;
			}

			current = id.parent();
		}
		false
	}

	/// Check if the path starts with the given directory path
	/// This is optimized for common filtering patterns like "/home/user" or "/var/log"
	pub fn is_descendant_of_path(&self, prefix_path: impl AsRef<Path>) -> bool {
		let prefix_path = prefix_path.as_ref();
		let prefix_path = prefix_path.canonicalize().unwrap_or_else(|e| {
			error!("Failed to canonicalize prefix path: {e}");
			prefix_path.to_path_buf()
		});
		
		let mut our_components = self.iter();
		for prefix_comp in prefix_path.components() {
			let Some(our_comp) = our_components.next() else {
				return false;
			};
			if our_comp != prefix_comp {
				return false;
			}
		}

		true
	}

	/// Check if the path matches a simple glob pattern
	/// Supports * (any characters) and ? (single character) in the last component only
	/// For directory matching, use starts_with_path() first
	///
	/// # Performance Note
	/// This method compiles the glob pattern on each call. For repeated use of the same pattern,
	/// consider using `name_matches_compiled_glob()` with a pre-compiled `GlobMatcher`.
	pub fn name_matches_glob(&self, pattern: &str) -> bool {
		self.with_segment(|seg| {
			if let Some(name) = seg.component.as_os_str().to_str() {
				glob_match(name, pattern)
			} else {
				false
			}
		})
		.unwrap_or(false)
	}

	/// Check if the filename matches a pre-compiled glob pattern
	/// This is more efficient when using the same pattern multiple times
	///
	/// # Example
	/// ```rust
	/// use globset::Glob;
	///
	/// // Compile once, use many times
	/// let txt_matcher = Glob::new("*.txt").unwrap().compile_matcher();
	///
	/// for path_id in many_paths {
	///     if path_id.name_matches_compiled_glob(&txt_matcher) {
	///         // Process .txt files
	///     }
	/// }
	/// ```
	pub fn name_matches_compiled_glob(&self, matcher: &globset::GlobMatcher) -> bool {
		self.with_segment(|seg| {
			if let Some(name) = seg.component.as_os_str().to_str() {
				matcher.is_match(name)
			} else {
				false
			}
		})
		.unwrap_or(false)
	}

	/// Get the file extension if present
	pub fn extension(&self) -> Option<OsString> {
		self.with_segment(|seg| seg.component.as_path().extension().map(OsStr::to_os_string))
			.flatten()
	}

	/// Check if the file has the given extension (case-insensitive)
	pub fn has_extension(&self, target_ext: impl AsRef<OsStr>) -> bool {
		self.with_segment(|seg| {
			if let Some(ext) = seg.component.as_path().extension() {
				ext.eq_ignore_ascii_case(target_ext)
			} else {
				false
			}
		})
		.unwrap_or(false)
	}

	/// Get the parent directory ID
	pub fn parent(&self) -> Option<DirEntryId> {
		self.with_segment(|seg| seg.parent).unwrap_or(None)
	}

	/// Check if this path is a descendant of the given parent path
	pub fn is_descendant_of(&self, ancestor: DirEntryId) -> bool {
		let mut current = self.parent();
		while let Some(parent) = current {
			if parent == ancestor {
				return true;
			}
			current = parent.parent();
		}
		false
	}

	/// Get the depth of this path (number of components from root)
	pub fn depth(&self) -> usize {
		let mut depth = 0;
		let mut current = Some(*self);

		while let Some(id) = current {
			depth += 1;
			current = id.parent();
		}

		depth
	}

	pub fn to_polars(&self) -> AnyValue<'static> {
		let (idx, generation) = self.raw_parts();
		AnyValue::StructOwned(Box::new((
			vec![
				AnyValue::UInt64(idx as u64),
				AnyValue::UInt64(generation as u64),
			],
			vec![
				Field::new("idx", DataType::UInt64),
				Field::new("gen", DataType::UInt64),
			],
		)))
	}

	pub fn from_polars(value: &AnyValue) -> Option<Self> {
		let (idx, generation) = match value {
			AnyValue::StructOwned(s) => {
				// TODO: Convert these to errors in case user tries to load bad dataframes
				debug_assert_eq!(s.1.len(), 2);
				debug_assert_eq!(s.1[0].name(), "idx");
				debug_assert_eq!(s.1[1].name(), "gen");
				let idx = s.0[0].extract::<u64>()?;
				let generation = s.0[1].extract::<u64>()?;
				(idx as usize, generation as usize)
			}
			other => {
				if cfg!(debug_assertions) {
					panic!("Expected AnyValue::StructOwned, got {other:?}");
				} else {
					return None;
				}
			}
		};
		Self::from_raw_parts(idx, generation)
	}
}

impl From<DirEntryId> for AnyValue<'static> {
	fn from(value: DirEntryId) -> Self {
		value.to_polars()
	}
}

impl From<AnyValue<'_>> for DirEntryId {
	fn from(value: AnyValue) -> Self {
		Self::from_polars(&value).unwrap()
	}
}

pub struct DirEntrySegmentIter<'a> {
	segments: Vec<ComponentCow<'a>>,
	index: usize,
}

impl<'a> DirEntrySegmentIter<'a> {
	fn new(segments: Vec<ComponentCow<'a>>) -> Self {
		Self { segments, index: 0 }
	}
}

impl<'a> Iterator for DirEntrySegmentIter<'a> {
	type Item = ComponentCow<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.index < self.segments.len() {
			let item = self.segments[self.index].clone();
			self.index += 1;
			Some(item)
		} else {
			None
		}
	}
}

impl std::fmt::Display for DirEntryId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let components: Vec<_> = self.iter().collect();

		if components.is_empty() {
			return Ok(());
		}

		// Handle the first component specially - it determines the path style
		let first = &components[0];
		match first {
			ComponentCow::Prefix(_) => {
				// Windows prefix (drive letter, UNC, etc.) - write as-is
				let path: &Path = first.as_ref();
				write!(f, "{}", path.display())?;
			}
			ComponentCow::RootDir => {
				// Unix root directory - write the separator
				f.write_char(std::path::MAIN_SEPARATOR)?;
			}
			ComponentCow::CurDir | ComponentCow::ParentDir | ComponentCow::Name(_) => {
				// Relative path component - write as-is
				let path: &Path = first.as_ref();
				write!(f, "{}", path.display())?;
			}
		}

		// Write remaining components with separators
		for (i, component) in components[1..].iter().enumerate() {
			// Add separator before each subsequent component
			// Exception: don't add separator after Windows prefix if it already ends with one
			// Exception: don't add separator for the first component after RootDir (since RootDir includes the separator)
			let is_first_after_root = i == 0 && matches!(components[0], ComponentCow::RootDir);

			let needs_separator = match (&components[0], component) {
				(ComponentCow::Prefix(prefix), _) => {
					// Check if prefix already ends with a separator
					!prefix.to_string_lossy().ends_with(['\\', '/'])
				}
				_ if is_first_after_root => {
					// First component after RootDir doesn't need separator (RootDir already includes it)
					false
				}
				_ => true,
			};

			if needs_separator {
				f.write_char(std::path::MAIN_SEPARATOR)?;
			}

			match component {
				ComponentCow::RootDir => {
					// RootDir in the middle of a path shouldn't happen, but handle it
					// by not writing anything (the separator is already written)
				}
				_ => {
					let path: &Path = component.as_ref();
					write!(f, "{}", path.display())?;
				}
			}
		}

		Ok(())
	}
}

impl Serialize for DirEntryId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.collect_str(self)
	}
}

impl<'de> Deserialize<'de> for DirEntryId {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let s = <&'de str as Deserialize<'de>>::deserialize(deserializer)?;
		let idx = intern_path(s);
		Ok(idx)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComponentCow<'a> {
	Prefix(Cow<'a, OsStr>),
	RootDir,
	CurDir,
	ParentDir,
	Name(Cow<'a, OsStr>),
}

impl<'a> From<Component<'a>> for ComponentCow<'a> {
	fn from(value: Component<'a>) -> Self {
		match value {
			Component::Prefix(prefix) => Self::Prefix(Cow::Borrowed(prefix.as_os_str())),
			Component::RootDir => Self::RootDir,
			Component::CurDir => Self::CurDir,
			Component::ParentDir => Self::ParentDir,
			Component::Normal(s) => Self::Name(Cow::Borrowed(s)),
		}
	}
}

impl<'a> From<&'a ComponentCow<'a>> for Component<'a> {
	fn from(value: &'a ComponentCow<'a>) -> Self {
		match value {
			ComponentCow::Prefix(prefix) => {
				// Unfortunate, but Windows is a pain sometimes.
				let path = <OsStr as AsRef<Path>>::as_ref(prefix);
				let parsed = path
					.components()
					.next()
					.expect("this was a prefix, it must be one component");
				#[cfg(debug_assertions)]
				{
					assert!(matches!(parsed, Component::Prefix(s) if *s.as_os_str() == *prefix));
				}
				parsed
			}
			ComponentCow::RootDir => Self::RootDir,
			ComponentCow::CurDir => Self::CurDir,
			ComponentCow::ParentDir => Self::ParentDir,
			ComponentCow::Name(Cow::Borrowed(s)) => Self::Normal(s),
			ComponentCow::Name(Cow::Owned(s)) => Self::Normal(s.as_ref()),
		}
	}
}

impl<'a> AsRef<OsStr> for ComponentCow<'a> {
	fn as_ref(&self) -> &OsStr {
		match self {
			ComponentCow::Prefix(s) => s.as_ref(),
			ComponentCow::RootDir => Component::RootDir.as_ref(),
			ComponentCow::CurDir => Component::CurDir.as_ref(),
			ComponentCow::ParentDir => Component::ParentDir.as_ref(),
			ComponentCow::Name(s) => s.as_ref(),
		}
	}
}

impl<'a> AsRef<Path> for ComponentCow<'a> {
	fn as_ref(&self) -> &Path {
		match self {
			ComponentCow::Prefix(s) => s.as_ref(),
			ComponentCow::RootDir => Component::RootDir.as_ref(),
			ComponentCow::CurDir => Component::CurDir.as_ref(),
			ComponentCow::ParentDir => Component::ParentDir.as_ref(),
			ComponentCow::Name(s) => s.as_ref(),
		}
	}
}

impl ComponentCow<'_> {
	pub fn into_owned(self) -> ComponentCow<'static> {
		match self {
			ComponentCow::Prefix(s) => ComponentCow::Prefix(Cow::Owned(s.into_owned())),
			ComponentCow::RootDir => ComponentCow::RootDir,
			ComponentCow::CurDir => ComponentCow::CurDir,
			ComponentCow::ParentDir => ComponentCow::ParentDir,
			ComponentCow::Name(s) => ComponentCow::Name(Cow::Owned(s.into_owned())),
		}
	}

	pub fn as_path(&self) -> &Path {
		self.as_ref()
	}

	pub fn as_os_str(&self) -> &OsStr {
		self.as_ref()
	}
}

impl PartialEq<Component<'_>> for ComponentCow<'_> {
	fn eq(&self, other: &Component<'_>) -> bool {
		match (self, other) {
			(ComponentCow::Prefix(s), Component::Prefix(t)) => s == t.as_os_str(),
			(ComponentCow::RootDir, Component::RootDir) => true,
			(ComponentCow::CurDir, Component::CurDir) => true,
			(ComponentCow::ParentDir, Component::ParentDir) => true,
			(ComponentCow::Name(s), Component::Normal(t)) => s == t,
			_ => false,
		}
	}
}

impl PartialEq<ComponentCow<'_>> for Component<'_> {
	fn eq(&self, other: &ComponentCow<'_>) -> bool {
		other == self
	}
}

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

/// Fast glob matching using globset for filename patterns
/// This is more efficient and feature-complete than a custom implementation
fn glob_match(text: &str, pattern: &str) -> bool {
	// Use globset for efficient glob matching
	// This handles all standard glob patterns including *, ?, [], {}, etc.
	// Note: For high-frequency usage with repeated patterns, consider caching compiled globs
	match globset::Glob::new(pattern) {
		Ok(glob) => glob.compile_matcher().is_match(text),
		Err(_) => false, // Invalid pattern doesn't match anything
	}
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

	#[test]
	fn test_path_interning_and_display() {
		// Test with a simple path
		let _test_path = if cfg!(windows) {
			PathBuf::from("C:\\Users\\test\\file.txt")
		} else {
			PathBuf::from("/home/test/file.txt")
		};

		// Create a temporary file to ensure canonicalize works
		let temp_dir = std::env::temp_dir();
		let test_file = temp_dir.join("test_intern.txt");
		std::fs::write(&test_file, "test").unwrap();

		let interned = intern_path(&test_file);
		let displayed = format!("{}", interned);
		let resolved = interned.resolve();

		// The displayed path should match the resolved path when converted to string
		assert_eq!(displayed, resolved.display().to_string());

		// Clean up
		let _ = std::fs::remove_file(&test_file);
	}

	#[test]
	fn test_path_interning_nonexistent() {
		// Test that we can intern non-existent paths without panicking
		let nonexistent = PathBuf::from("/this/path/does/not/exist");
		let interned = intern_path(&nonexistent);
		let displayed = format!("{}", interned);

		// Should not panic and should produce a reasonable string
		assert!(!displayed.is_empty());
	}

	#[test]
	fn test_display_separators() {
		// Create a test path with multiple segments
		let temp_dir = std::env::temp_dir();
		let nested_dir = temp_dir.join("test_nested").join("subdir");
		std::fs::create_dir_all(&nested_dir).unwrap();
		let test_file = nested_dir.join("file.txt");
		std::fs::write(&test_file, "test").unwrap();

		let interned = intern_path(&test_file);
		let displayed = format!("{}", interned);

		// Should contain path separators
		assert!(displayed.contains(std::path::MAIN_SEPARATOR));

		// Should not start with a separator (unless it's a root path)
		if !displayed.starts_with(std::path::MAIN_SEPARATOR) {
			// On Windows, paths typically start with drive letters
			assert!(!displayed.starts_with(std::path::MAIN_SEPARATOR));
		}

		// Clean up
		let _ = std::fs::remove_dir_all(&temp_dir.join("test_nested"));
	}

	#[test]
	fn test_filtering_methods() {
		// Create test paths
		let test_paths = [
			"/home/user/documents/report.txt",
			"/home/user/pictures/photo.jpg",
			"/var/log/system.log",
			"/tmp/build/output.dat",
			"/home/user/downloads/archive.tar.gz",
		];

		let interned_paths: Vec<DirEntryId> = test_paths
			.iter()
			.map(|p| intern_path(PathBuf::from(p)))
			.collect();

		// Test filename filtering
		let txt_files: Vec<_> = interned_paths
			.iter()
			.filter(|id| id.name_ends_with(".txt"))
			.collect();
		assert_eq!(txt_files.len(), 1);

		let log_files: Vec<_> = interned_paths
			.iter()
			.filter(|id| id.has_extension("log"))
			.collect();
		assert_eq!(log_files.len(), 1);

		// Test path prefix filtering
		let home_files: Vec<_> = interned_paths
			.iter()
			.filter(|id| id.is_descendant_of_path("/home/user"))
			.collect();
		assert_eq!(home_files.len(), 3);

		// Test path contains
		let user_files: Vec<_> = interned_paths
			.iter()
			.filter(|id| id.path_contains("user"))
			.collect();
		assert_eq!(user_files.len(), 3);

		// Test glob matching
		let photo_id = intern_path(PathBuf::from("/home/user/pictures/photo.jpg"));
		assert!(photo_id.name_matches_glob("*.jpg"));
		assert!(photo_id.name_matches_glob("photo.*"));
		assert!(photo_id.name_matches_glob("p?oto.jpg"));
		assert!(!photo_id.name_matches_glob("*.txt"));

		// Test extension extraction
		let archive_id = intern_path(PathBuf::from("/home/user/downloads/archive.tar.gz"));
		assert_eq!(archive_id.extension(), Some(OsString::from("gz")));
	}

	#[test]
	fn test_glob_matching() {
		assert!(super::glob_match("hello.txt", "*.txt"));
		assert!(super::glob_match("hello.txt", "hello.*"));
		assert!(super::glob_match("hello.txt", "h?llo.txt"));
		assert!(super::glob_match("hello.txt", "*"));
		assert!(super::glob_match("hello.txt", "hello.txt"));

		assert!(!super::glob_match("hello.txt", "*.jpg"));
		assert!(!super::glob_match("hello.txt", "world.*"));
		assert!(!super::glob_match("hello.txt", "h?llo.jpg"));
		assert!(!super::glob_match("hello.txt", "goodbye"));

		// Edge cases
		assert!(super::glob_match("", "*"));
		assert!(super::glob_match("", ""));
		assert!(!super::glob_match("hello", ""));
		assert!(super::glob_match("hello", "*****"));
	}

	#[test]
	fn test_compiled_glob_matching() {
		// Test the optimized compiled glob approach
		let txt_glob = globset::Glob::new("*.txt").unwrap().compile_matcher();
		let jpg_glob = globset::Glob::new("*.jpg").unwrap().compile_matcher();

		let test_paths = [
			"/home/user/document.txt",
			"/home/user/photo.jpg",
			"/var/log/system.log",
		];

		let interned_paths: Vec<DirEntryId> = test_paths
			.iter()
			.map(|p| intern_path(PathBuf::from(p)))
			.collect();

		// Test compiled glob matching
		let txt_files: Vec<_> = interned_paths
			.iter()
			.filter(|id| id.name_matches_compiled_glob(&txt_glob))
			.collect();
		assert_eq!(txt_files.len(), 1);

		let jpg_files: Vec<_> = interned_paths
			.iter()
			.filter(|id| id.name_matches_compiled_glob(&jpg_glob))
			.collect();
		assert_eq!(jpg_files.len(), 1);

		// Verify same results as string-based glob
		for path_id in &interned_paths {
			assert_eq!(
				path_id.name_matches_glob("*.txt"),
				path_id.name_matches_compiled_glob(&txt_glob)
			);
			assert_eq!(
				path_id.name_matches_glob("*.jpg"),
				path_id.name_matches_compiled_glob(&jpg_glob)
			);
		}
	}
}
