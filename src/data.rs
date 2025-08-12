//! Core data structures using Polars DataFrames

use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

use crate::error::{DetectorError, DetectorResult};

/// File type classification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileKind {
	Text,
	Image,
	Audio,
	Video,
	Archive,
	Binary,
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

/// Main data store containing all file metadata and processing state
#[derive(Debug, Clone)]
pub struct ScanState {
	/// Primary DataFrame containing all file data
	pub data: DataFrame,
	/// Current scan identifier
	pub scan_id: u32,
	/// Timestamp when scan started
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

	/// Add files to the scan state
	pub fn add_files(&mut self, files: Vec<FileRecord>) -> DetectorResult<()> {
		if files.is_empty() {
			return Ok(());
		}

		let new_df = self.files_to_dataframe(files)?;
		self.data = self.data.vstack(&new_df)?;
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

/// Store for relational data between files
#[derive(Debug, Clone)]
pub struct RelationStore {
	/// Hash relations indexed by hash value
	pub hash_relations: DataFrame,
	/// Similarity groups indexed by group ID
	pub similarity_groups: DataFrame,
	/// Pairwise relations between files
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
