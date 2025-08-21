//! Core data structures using Polars DataFrames

use chrono::{DateTime, Utc};
use polars::datatypes::{DataType, Field};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

use crate::error::{DetectorError, DetectorResult};
use crate::paths::intern_path;

// Re-export RelationStore from the relations module
pub use crate::relations::RelationStore;

/// File type classification for content-based categorization.
///
/// This enum categorizes files based on their content type and extension,
/// enabling efficient filtering and analysis of different file categories.
/// The classification is used throughout the system for:
///
/// - **UI Display**: Grouping files by type in GUI/TUI interfaces
/// - **Processing Optimization**: Different handling for text vs binary files
/// - **Similarity Analysis**: Type-specific duplicate detection algorithms
/// - **Filtering**: Include/exclude specific file types during scanning
///
/// # Examples
///
/// ```rust
/// use uncp::data::FileKind;
///
/// let file_type = FileKind::Text;
/// assert_eq!(file_type.to_string(), "text");
///
/// // Used in filtering operations
/// match file_type {
///     FileKind::Text => println!("Can analyze text content"),
///     FileKind::Image => println!("Can use perceptual hashing"),
///     FileKind::Binary => println!("Content-based comparison only"),
///     _ => println!("General duplicate detection"),
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileKind {
	/// Text files (source code, documents, configuration files)
	Text,
	/// Image files (JPEG, PNG, GIF, etc.)
	Image,
	/// Audio files (MP3, WAV, FLAC, etc.)
	Audio,
	/// Video files (MP4, AVI, MKV, etc.)
	Video,
	/// Archive files (ZIP, TAR, RAR, etc.)
	Archive,
	/// Binary executable files and libraries
	Binary,
	/// Files with unknown or unrecognized types
	Unknown,
}

impl std::fmt::Display for FileKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			FileKind::Text => write!(f, "text"),
			FileKind::Image => write!(f, "image"),
			FileKind::Audio => write!(f, "audio"),
			FileKind::Video => write!(f, "video"),
			FileKind::Archive => write!(f, "archive"),
			FileKind::Binary => write!(f, "binary"),
			FileKind::Unknown => write!(f, "unknown"),
		}
	}
}

/// Primary data structure containing all file metadata and processing state.
///
/// `ScanState` is the core data structure that holds all discovered file information
/// in a Polars DataFrame for efficient columnar operations. It serves as the central
/// repository for file metadata, processing status, and computed hashes.
///
/// ## DataFrame Schema
///
/// The internal DataFrame contains the following columns:
///
/// - `path`: File path as DirEntryId (Struct[idx: u64, gen: u64])
/// - `size`: File size in bytes (u64)
/// - `modified`: Last modification timestamp (`DateTime<Utc>`)
/// - `file_type`: Detected file type (FileKind enum)
/// - `hash`: Content hash (optional Blake3 hash as string)
/// - `hash_computed`: Whether hash has been computed (boolean)
/// - `scan_id`: Scan session identifier (u32)
///
/// ## Performance Characteristics
///
/// - **Memory Efficient**: Columnar storage minimizes memory overhead
/// - **Lazy Evaluation**: Supports datasets larger than available RAM
/// - **Fast Queries**: Optimized for filtering, grouping, and aggregation
/// - **Serializable**: Can be saved/loaded from Parquet format for caching
///
/// ## Usage Patterns
///
/// ```rust
/// use uncp::data::ScanState;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create new scan state
/// let mut scan_state = ScanState::new()?;
///
/// // Add files from discovery
/// let file_paths = vec!["/path/to/file1.txt", "/path/to/file2.jpg"];
/// scan_state.add_files(&file_paths).await?;
///
/// // Query for specific file types
/// let text_files = scan_state.files_by_type("text")?;
/// println!("Found {} text files", text_files.height());
///
/// // Get files that need hashing
/// let unhashed = scan_state.files_needing_hash()?;
/// println!("{} files need content hashing", unhashed.height());
/// # Ok(())
/// # }
/// ```
///
/// ## Thread Safety
///
/// `ScanState` is designed for single-threaded access within the detector.
/// For concurrent access, use the query interface which provides immutable
/// views of the data suitable for parallel processing.
#[derive(Debug, Clone)]
pub struct ScanState {
	/// Primary DataFrame containing all file data with columnar storage
	pub data: DataFrame,
	/// Current scan identifier for tracking processing sessions
	pub scan_id: u32,
	/// Timestamp when the current scan session started
	pub scan_started: DateTime<Utc>,
}

impl ScanState {
	/// Create a new empty ScanState
	pub fn new() -> DetectorResult<Self> {
		let data = Self::create_empty_dataframe()?;
		Ok(ScanState {
			data,
			scan_id: 1,
			scan_started: Utc::now(),
		})
	}

	/// Create the schema for the main DataFrame
	fn create_empty_dataframe() -> PolarsResult<DataFrame> {
		let path_dtype = DataType::Struct(vec![
			Field::new("idx".into(), DataType::UInt64),
			Field::new("gen".into(), DataType::UInt64),
		]);
		DataFrame::new(vec![
			Series::new_empty("path".into(), &path_dtype).into(),
			Series::new("size".into(), Vec::<u64>::new()).into(),
			Series::new("modified".into(), Vec::<i64>::new()).into(), // Unix timestamp in nanoseconds
			Series::new("file_type".into(), Vec::<String>::new()).into(),
			Series::new("content_loaded".into(), Vec::<bool>::new()).into(),
			Series::new("hashed".into(), Vec::<bool>::new()).into(),
			Series::new("similarity_computed".into(), Vec::<bool>::new()).into(),
			Series::new("scan_id".into(), Vec::<u32>::new()).into(),
			Series::new("last_processed".into(), Vec::<i64>::new()).into(),
			Series::new("blake3_hash".into(), Vec::<Option<String>>::new()).into(),
			Series::new("perceptual_hash".into(), Vec::<Option<String>>::new()).into(),
			Series::new("text_hash".into(), Vec::<Option<String>>::new()).into(),
		])
	}

	/// Add files to the scan state, deduplicating by path
	pub fn add_files(&mut self, files: Vec<FileRecord>) -> DetectorResult<()> {
		if files.is_empty() {
			return Ok(());
		}

		let new_df = self.files_to_dataframe(files)?;

		// If the existing dataframe is empty, just use the new data
		if self.data.height() == 0 {
			self.data = new_df;
			return Ok(());
		}

		// Combine existing and new data, then deduplicate by path
		// Keep the most recent entry for each path (from new_df)
		use polars::prelude::*;
		let combined = self.data.vstack(&new_df)?;

		// Deduplicate by path, keeping the last occurrence (most recent)
		self.data = combined
			.lazy()
			.unique(Some(cols(["path"])), UniqueKeepStrategy::Last)
			.collect()?;

		Ok(())
	}

	/// Convert file records to DataFrame
	fn files_to_dataframe(&self, files: Vec<FileRecord>) -> PolarsResult<DataFrame> {
		// Build Struct path column from idx/gen components
		let (idxs, gens): (Vec<u64>, Vec<u64>) = files
			.iter()
			.map(|f| {
				let (i, g) = intern_path(&f.path).raw_parts();
				(i as u64, g as u64)
			})
			.unzip();
		let fields = vec![
			Field::new("idx".into(), DataType::UInt64),
			Field::new("gen".into(), DataType::UInt64),
		];
		let values: Vec<AnyValue<'static>> = idxs
			.into_iter()
			.zip(gens)
			.map(|(i, g)| {
				AnyValue::StructOwned(Box::new((
					vec![AnyValue::UInt64(i), AnyValue::UInt64(g)],
					fields.clone(),
				)))
			})
			.collect();
		let path_series = Series::from_any_values_and_dtype(
			"path".into(),
			&values,
			&DataType::Struct(fields.clone()),
			true,
		)?;

		let sizes: Vec<u64> = files.iter().map(|f| f.size).collect();
		let modified: Vec<i64> = files
			.iter()
			.map(|f| f.modified.timestamp_nanos_opt().unwrap_or(0))
			.collect();
		let file_types: Vec<String> = files.iter().map(|f| f.file_type.to_string()).collect();
		let scan_ids: Vec<u32> = vec![self.scan_id; files.len()];
		let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
		let last_processed: Vec<i64> = vec![now; files.len()];

		// Initialize processing flags to false
		let content_loaded: Vec<bool> = vec![false; files.len()];
		let hashed: Vec<bool> = vec![false; files.len()];
		let similarity_computed: Vec<bool> = vec![false; files.len()];

		// Initialize hash fields to None
		let blake3_hash: Vec<Option<String>> = vec![None; files.len()];
		let perceptual_hash: Vec<Option<String>> = vec![None; files.len()];
		let text_hash: Vec<Option<String>> = vec![None; files.len()];

		DataFrame::new(vec![
			path_series.into(),
			Series::new("size".into(), sizes).into(),
			Series::new("modified".into(), modified).into(),
			Series::new("file_type".into(), file_types).into(),
			Series::new("content_loaded".into(), content_loaded).into(),
			Series::new("hashed".into(), hashed).into(),
			Series::new("similarity_computed".into(), similarity_computed).into(),
			Series::new("scan_id".into(), scan_ids).into(),
			Series::new("last_processed".into(), last_processed).into(),
			Series::new("blake3_hash".into(), blake3_hash).into(),
			Series::new("perceptual_hash".into(), perceptual_hash).into(),
			Series::new("text_hash".into(), text_hash).into(),
		])
	}

	/// Get files that need processing by a specific system
	pub fn files_needing_processing(&self, system_name: &str) -> DetectorResult<DataFrame> {
		let filter_expr = match system_name {
			"content_hash" => col("hashed").eq(lit(false)),
			"similarity" => col("similarity_computed").eq(lit(false)),
			_ => {
				return Err(DetectorError::Config(format!(
					"Unknown system: {}",
					system_name
				)));
			}
		};

		Ok(self.data.clone().lazy().filter(filter_expr).collect()?)
	}

	/// Update processing flags for files
	pub fn update_processing_flags(
		&mut self,
		paths: &[String],
		flag: &str,
		value: bool,
	) -> DetectorResult<()> {
		// This is a simplified implementation - in practice, we'd use more efficient Polars operations
		// TODO: implement efficient flag updates with Polars joins/select
		let _ = (paths, flag, value);
		Ok(())
	}

	/// Update hashes for files (for testing)
	pub fn update_hashes(
		&mut self,
		paths: Vec<String>,
		hashes: Vec<Option<String>>,
	) -> DetectorResult<()> {
		if paths.len() != hashes.len() {
			return Err(DetectorError::Config(
				"Paths and hashes length mismatch".to_string(),
			));
		}

		let height = self.data.height();
		if height == 0 {
			return Ok(());
		}
		// Build lookup from (idx,gen) -> row index
		let pcol = self
			.data
			.column("path")
			.map_err(DetectorError::Polars)?
			.struct_()
			.map_err(DetectorError::Polars)?;
		let idx_ca = pcol
			.field_by_name("idx")
			.map_err(DetectorError::Polars)?
			.u64()
			.map_err(DetectorError::Polars)?
			.clone();
		let gen_ca = pcol
			.field_by_name("gen")
			.map_err(DetectorError::Polars)?
			.u64()
			.map_err(DetectorError::Polars)?
			.clone();
		let mut row_by_key: HashMap<(u64, u64), usize> = HashMap::with_capacity(height);
		for i in 0..height {
			let idx = idx_ca.get(i).expect("idx not null");
			let r#gen = gen_ca.get(i).expect("gen not null");
			row_by_key.insert((idx, r#gen), i);
		}

		// Prepare new column values
		let mut blake_series = self
			.data
			.column("blake3_hash")
			.map_err(DetectorError::Polars)?
			.str()
			.map_err(DetectorError::Polars)?
			.into_iter()
			.map(|opt| opt.map(|s| s.to_string()))
			.collect::<Vec<Option<String>>>();
		let mut hashed_series = self
			.data
			.column("hashed")
			.map_err(DetectorError::Polars)?
			.bool()
			.map_err(DetectorError::Polars)?
			.into_iter()
			.map(|opt| opt.unwrap_or(false))
			.collect::<Vec<bool>>();

		for (p, h) in paths.iter().zip(hashes.into_iter()) {
			let (i, g) = intern_path(p).raw_parts();
			if let Some(&row) = row_by_key.get(&(i as u64, g as u64)) {
				blake_series[row] = h;
				hashed_series[row] = true;
			}
		}

		// Replace columns
		self.data
			.replace(
				"blake3_hash",
				Series::new("blake3_hash".into(), blake_series),
			)
			.map_err(DetectorError::Polars)?;
		self.data
			.replace("hashed", Series::new("hashed".into(), hashed_series))
			.map_err(DetectorError::Polars)?;
		Ok(())
	}

	/// Merge another scan state into this one, combining file data
	/// Files from the other state will be added, with existing files being updated
	pub fn merge_with(&mut self, other: &ScanState) -> DetectorResult<()> {
		if other.data.height() == 0 {
			return Ok(()); // Nothing to merge
		}

		if self.data.height() == 0 {
			// If current state is empty, just copy the other state
			self.data = other.data.clone();
			self.scan_id = other.scan_id;
			self.scan_started = other.scan_started;
			return Ok(());
		}

		// Combine the DataFrames, with new data taking precedence for duplicates
		use polars::prelude::concat;
		let combined = concat(
			[other.data.clone().lazy(), self.data.clone().lazy()],
			UnionArgs::default(),
		)
		.map_err(DetectorError::Polars)?
		.unique(Some(cols(["path"])), UniqueKeepStrategy::First)
		.collect()
		.map_err(DetectorError::Polars)?;

		self.data = combined;
		// Update metadata to reflect the merge
		self.scan_id = self.scan_id.max(other.scan_id);
		self.scan_started = self.scan_started.min(other.scan_started);

		Ok(())
	}
}

impl Default for ScanState {
	fn default() -> Self {
		Self::new().expect("Failed to create default ScanState")
	}
}

/// Individual file record for initial discovery
#[derive(Debug, Clone)]
pub struct FileRecord {
	pub path: PathBuf,
	pub size: u64,
	pub modified: DateTime<Utc>,
	pub file_type: FileKind,
}

/// Duplicate group result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup {
	pub group_id: Uuid,
	pub group_type: String,
	pub files: Vec<PathBuf>,
	pub similarity_score: f64,
	pub metadata: HashMap<String, String>,
}

/// Similarity group result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimilarityGroup {
	pub group_id: Uuid,
	pub group_type: String,
	pub files: Vec<PathBuf>,
	pub threshold: f64,
	pub created_at: DateTime<Utc>,
}

/// File relationships result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRelationships {
	pub file_path: PathBuf,
	pub exact_duplicates: Vec<PathBuf>,
	pub similar_files: Vec<(PathBuf, f64)>, // (path, similarity_score)
	pub groups: Vec<Uuid>,
}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::Utc;
	use std::path::PathBuf;

	fn create_test_file_record(path: &str, size: u64) -> FileRecord {
		FileRecord {
			path: PathBuf::from(path),
			size,
			modified: Utc::now(),
			file_type: FileKind::Text,
		}
	}

	#[test]
	fn test_file_kind_display() {
		assert_eq!(FileKind::Text.to_string(), "text");
		assert_eq!(FileKind::Image.to_string(), "image");
		assert_eq!(FileKind::Audio.to_string(), "audio");
		assert_eq!(FileKind::Video.to_string(), "video");
		assert_eq!(FileKind::Archive.to_string(), "archive");
		assert_eq!(FileKind::Binary.to_string(), "binary");
		assert_eq!(FileKind::Unknown.to_string(), "unknown");
	}

	#[test]
	fn test_scan_state_creation() {
		let state = ScanState::new().unwrap();
		assert_eq!(state.scan_id, 1);
		assert_eq!(state.data.height(), 0);
		assert!(state.scan_started <= Utc::now());
	}

	#[test]
	fn test_add_files() {
		let mut state = ScanState::new().unwrap();
		let files = vec![
			create_test_file_record("/test/file1.txt", 100),
			create_test_file_record("/test/file2.txt", 200),
		];

		let result = state.add_files(files);
		assert!(result.is_ok());
		assert_eq!(state.data.height(), 2);
	}

	#[test]
	fn test_data_frame_operations() {
		let mut state = ScanState::new().unwrap();
		assert_eq!(state.data.height(), 0);

		let files = vec![
			create_test_file_record("/test/file1.txt", 100),
			create_test_file_record("/test/file2.txt", 200),
		];
		state.add_files(files).unwrap();
		assert_eq!(state.data.height(), 2);

		// Test that we can access columns
		assert!(state.data.column("path").is_ok());
		assert!(state.data.column("size").is_ok());
		assert!(state.data.column("modified").is_ok());
		assert!(state.data.column("file_type").is_ok());
	}

	#[test]
	fn test_update_hashes() {
		let mut state = ScanState::new().unwrap();
		let files = vec![
			create_test_file_record("/test/file1.txt", 100),
			create_test_file_record("/test/file2.txt", 200),
		];
		state.add_files(files).unwrap();

		let paths = vec!["/test/file1.txt".to_string()];
		let hashes = vec![Some("abc123".to_string())];

		let result = state.update_hashes(paths, hashes);
		assert!(result.is_ok());

		// Verify the hash was updated
		let df = &state.data;
		let hash_col = df.column("blake3_hash").unwrap();
		let first_hash = hash_col.get(0).unwrap();
		let hash_str = first_hash.to_string();
		assert!(hash_str.contains("abc123"));
	}

	#[test]
	fn test_relation_store_creation() {
		let store = RelationStore::new();
		assert!(store.is_empty());
	}
}
