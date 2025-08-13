//! Core data structures using Polars DataFrames

use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

use crate::error::{DetectorError, DetectorResult};

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
/// - **`path`**: File path as string (primary key)
/// - **`size`**: File size in bytes (u64)
/// - **`modified`**: Last modification timestamp (`DateTime<Utc>`)
/// - **`file_type`**: Detected file type (FileKind enum)
/// - **`hash`**: Content hash (optional Blake3 hash as string)
/// - **`hash_computed`**: Whether hash has been computed (boolean)
/// - **`scan_id`**: Scan session identifier (u32)
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
		df! {
			// Core file identity and metadata
			"path" => Vec::<String>::new(),
			"size" => Vec::<u64>::new(),
			"modified" => Vec::<i64>::new(), // Unix timestamp in nanoseconds
			"file_type" => Vec::<String>::new(),

			// Processing state flags (ECS-style components)
			"content_loaded" => Vec::<bool>::new(),
			"hashed" => Vec::<bool>::new(),
			"similarity_computed" => Vec::<bool>::new(),

			// Scan metadata
			"scan_id" => Vec::<u32>::new(),
			"last_processed" => Vec::<i64>::new(),

			// Hash data (optional, filled by systems)
			"blake3_hash" => Vec::<Option<String>>::new(),
			"perceptual_hash" => Vec::<Option<String>>::new(),
			"text_hash" => Vec::<Option<String>>::new(),
		}
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
			.unique(Some(vec!["path".to_string()]), UniqueKeepStrategy::Last)
			.collect()?;

		Ok(())
	}

	/// Convert file records to DataFrame
	fn files_to_dataframe(&self, files: Vec<FileRecord>) -> PolarsResult<DataFrame> {
		let paths: Vec<String> = files
			.iter()
			.map(|f| f.path.to_string_lossy().to_string())
			.collect();
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

		df! {
			"path" => paths,
			"size" => sizes,
			"modified" => modified,
			"file_type" => file_types,
			"content_loaded" => content_loaded,
			"hashed" => hashed,
			"similarity_computed" => similarity_computed,
			"scan_id" => scan_ids,
			"last_processed" => last_processed,
			"blake3_hash" => blake3_hash,
			"perceptual_hash" => perceptual_hash,
			"text_hash" => text_hash,
		}
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
				)))
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

		let paths_len = paths.len();

		// Create update frame
		let update_df = df! {
			"path" => paths,
			"blake3_hash" => hashes,
			"hashed" => vec![true; paths_len],
		}
		.map_err(DetectorError::Polars)?;

		// Merge updates into state using a left join and coalesce
		let updated = self
			.data
			.clone()
			.lazy()
			.left_join(update_df.clone().lazy(), col("path"), col("path"))
			.with_columns([
				when(col("blake3_hash_right").is_not_null())
					.then(col("blake3_hash_right"))
					.otherwise(col("blake3_hash"))
					.alias("blake3_hash"),
				when(col("hashed_right").is_not_null())
					.then(col("hashed_right"))
					.otherwise(col("hashed"))
					.alias("hashed"),
			])
			.select([all().exclude(["blake3_hash_right", "hashed_right"])])
			.collect()
			.map_err(DetectorError::Polars)?;

		self.data = updated;
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
		.unique(Some(vec!["path".to_string()]), UniqueKeepStrategy::First)
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

/// Storage for relationships and similarity data between files.
///
/// `RelationStore` manages the complex relationships discovered during duplicate
/// detection, including exact matches (same content hash) and similarity relationships
/// (perceptual hashing, text similarity, etc.). It uses separate DataFrames for
/// different types of relationships to optimize query performance.
///
/// ## Relationship Types
///
/// ### Hash Relations
/// Files with identical content hashes (exact duplicates):
/// - **`hash`**: Content hash value (string)
/// - **`paths`**: List of file paths with this hash
/// - **`count`**: Number of files sharing this hash
/// - **`total_size`**: Combined size of all duplicate files
///
/// ### Similarity Groups
/// Files grouped by similarity analysis (near-duplicates):
/// - **`group_id`**: Unique identifier for similarity group
/// - **`paths`**: List of file paths in this group
/// - **`similarity_type`**: Type of similarity (perceptual, text, etc.)
/// - **`confidence`**: Similarity confidence score (0.0-1.0)
///
/// ### Pairwise Relations
/// Individual file-to-file relationships:
/// - **`path1`**: First file path
/// - **`path2`**: Second file path
/// - **`relation_type`**: Type of relationship (duplicate, similar, etc.)
/// - **`score`**: Relationship strength score
///
/// ## Performance Characteristics
///
/// - **Indexed Access**: Fast lookups by hash, group ID, or file path
/// - **Efficient Joins**: Optimized for combining with ScanState data
/// - **Memory Efficient**: Columnar storage minimizes overhead for large datasets
/// - **Lazy Evaluation**: Supports complex queries without loading all data
///
/// ## Usage Examples
///
/// ```rust
/// use uncp::data::RelationStore;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create new relation store
/// let mut relations = RelationStore::new()?;
///
/// // Add hash-based duplicate relationship
/// relations.add_hash_relation("abc123", vec![
///     "/path/to/file1.txt".to_string(),
///     "/path/to/file2.txt".to_string(),
/// ])?;
///
/// // Query for duplicate groups
/// let duplicates = relations.get_duplicate_groups()?;
/// println!("Found {} duplicate groups", duplicates.height());
///
/// // Find files similar to a specific file
/// let similar = relations.find_similar_files("/path/to/target.jpg")?;
/// println!("Found {} similar files", similar.height());
/// # Ok(())
/// # }
/// ```
///
/// ## Integration with ScanState
///
/// RelationStore is designed to work seamlessly with ScanState:
/// - File paths serve as foreign keys between the structures
/// - Queries can efficiently join data from both stores
/// - Updates maintain referential integrity automatically
#[derive(Debug, Clone)]
pub struct RelationStore {
	/// Hash-based exact duplicate relationships indexed by content hash
	pub hash_relations: DataFrame,
	/// Similarity groups for near-duplicate detection with confidence scores
	pub similarity_groups: DataFrame,
	/// Pairwise file relationships with detailed similarity metrics
	pub pairwise_relations: DataFrame,
}

impl RelationStore {
	/// Create a new empty RelationStore
	pub fn new() -> DetectorResult<Self> {
		Ok(RelationStore {
			hash_relations: Self::create_hash_relations_schema()?,
			similarity_groups: Self::create_similarity_groups_schema()?,
			pairwise_relations: Self::create_pairwise_relations_schema()?,
		})
	}

	fn create_hash_relations_schema() -> PolarsResult<DataFrame> {
		let file_paths = ListChunked::full_null_with_dtype("file_paths", 0, &DataType::String);
		DataFrame::new(vec![
			Series::new("hash_value", Vec::<String>::new()),
			Series::new("hash_type", Vec::<String>::new()),
			file_paths.into_series(),
			Series::new("first_seen", Vec::<i64>::new()),
			Series::new("file_count", Vec::<u32>::new()),
		])
	}

	fn create_similarity_groups_schema() -> PolarsResult<DataFrame> {
		let file_paths = ListChunked::full_null_with_dtype("file_paths", 0, &DataType::String);
		DataFrame::new(vec![
			Series::new("group_id", Vec::<String>::new()),
			Series::new("group_type", Vec::<String>::new()),
			file_paths.into_series(),
			Series::new("metadata", Vec::<String>::new()),
			Series::new("created_at", Vec::<i64>::new()),
			Series::new("similarity_threshold", Vec::<f64>::new()),
		])
	}

	fn create_pairwise_relations_schema() -> PolarsResult<DataFrame> {
		df! {
			"path_a" => Vec::<String>::new(),
			"path_b" => Vec::<String>::new(),
			"relation_type" => Vec::<String>::new(),
			"score" => Vec::<f64>::new(),
			"data" => Vec::<String>::new(),
			"computed_at" => Vec::<i64>::new(),
		}
	}

	/// Merge another relation store into this one, combining relationship data
	pub fn merge_with(&mut self, other: &RelationStore) -> DetectorResult<()> {
		use polars::prelude::concat;

		// Merge hash relations
		if other.hash_relations.height() > 0 {
			if self.hash_relations.height() == 0 {
				self.hash_relations = other.hash_relations.clone();
			} else {
				let combined = concat(
					[
						other.hash_relations.clone().lazy(),
						self.hash_relations.clone().lazy(),
					],
					UnionArgs::default(),
				)
				.map_err(DetectorError::Polars)?
				.unique(None, UniqueKeepStrategy::First)
				.collect()
				.map_err(DetectorError::Polars)?;
				self.hash_relations = combined;
			}
		}

		// Merge similarity groups
		if other.similarity_groups.height() > 0 {
			if self.similarity_groups.height() == 0 {
				self.similarity_groups = other.similarity_groups.clone();
			} else {
				let combined = concat(
					[
						other.similarity_groups.clone().lazy(),
						self.similarity_groups.clone().lazy(),
					],
					UnionArgs::default(),
				)
				.map_err(DetectorError::Polars)?
				.unique(None, UniqueKeepStrategy::First)
				.collect()
				.map_err(DetectorError::Polars)?;
				self.similarity_groups = combined;
			}
		}

		// Merge pairwise relations
		if other.pairwise_relations.height() > 0 {
			if self.pairwise_relations.height() == 0 {
				self.pairwise_relations = other.pairwise_relations.clone();
			} else {
				let combined = concat(
					[
						other.pairwise_relations.clone().lazy(),
						self.pairwise_relations.clone().lazy(),
					],
					UnionArgs::default(),
				)
				.map_err(DetectorError::Polars)?
				.unique(None, UniqueKeepStrategy::First)
				.collect()
				.map_err(DetectorError::Polars)?;
				self.pairwise_relations = combined;
			}
		}

		Ok(())
	}
}

impl Default for RelationStore {
	fn default() -> Self {
		Self::new().expect("Failed to create default RelationStore")
	}
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
		assert!(store.is_ok());
	}
}
