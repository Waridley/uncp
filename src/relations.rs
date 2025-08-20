//! Relations module for managing file relationships with type-safe keys.
//!
//! This module provides a flexible system for storing and retrieving different types
//! of file relationships using type-safe keys. Each relation type is identified by
//! a unique type that implements the `RelationKey` trait.

use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::any::TypeId;
use std::collections::HashMap;

use crate::error::{DetectorError, DetectorResult};

/// Trait for types that can serve as relation keys.
///
/// Each relation type should implement this trait to provide:
/// - A unique type identifier for type-safe retrieval
/// - Schema definition for the DataFrame structure
/// - Metadata about the relation type
///
/// # Examples
///
/// ```rust
/// use uncp::relations::{RelationKey, RelationMetadata};
/// use polars::prelude::*;
///
/// struct IdenticalHashes;
///
/// impl RelationKey for IdenticalHashes {
///     fn name() -> &'static str {
///         "identical_hashes"
///     }
///     
///     fn description() -> &'static str {
///         "Files with identical content hashes"
///     }
///     
///     fn create_schema() -> PolarsResult<DataFrame> {
///         DataFrame::new(vec![
///             Series::new("hash_value", Vec::<String>::new()),
///             Series::new("file_paths", Vec::<Vec<String>>::new()),
///             Series::new("file_count", Vec::<u32>::new()),
///         ])
///     }
/// }
/// ```
pub trait RelationKey: 'static {
	/// Human-readable name for this relation type
	fn name() -> &'static str;

	/// Description of what this relation represents
	fn description() -> &'static str;

	/// Create the DataFrame schema for this relation type
	fn create_schema() -> PolarsResult<DataFrame>;

	/// Optional: Version for schema evolution
	fn version() -> u32 {
		1
	}
}

/// Metadata associated with a relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationMetadata {
	/// Human-readable name of the relation
	pub name: String,
	/// Description of what this relation represents
	pub description: String,
	/// Schema version for evolution support
	pub version: u32,
	/// When this relation was created
	pub created_at: DateTime<Utc>,
	/// When this relation was last updated
	pub updated_at: DateTime<Utc>,
	/// Number of rows in the relation
	pub row_count: usize,
	/// Custom metadata as key-value pairs
	pub custom_metadata: HashMap<String, String>,
}

impl RelationMetadata {
	/// Create new metadata for a relation key
	pub fn new<K: RelationKey>() -> Self {
		let now = Utc::now();
		Self {
			name: K::name().to_string(),
			description: K::description().to_string(),
			version: K::version(),
			created_at: now,
			updated_at: now,
			row_count: 0,
			custom_metadata: HashMap::new(),
		}
	}

	/// Update the metadata when the relation changes
	pub fn update(&mut self, row_count: usize) {
		self.updated_at = Utc::now();
		self.row_count = row_count;
	}

	/// Add custom metadata
	pub fn with_custom_metadata(mut self, key: String, value: String) -> Self {
		self.custom_metadata.insert(key, value);
		self
	}
}

/// Storage for an arbitrary number of typed relations.
///
/// `RelationStore` uses type-safe keys to store and retrieve different types
/// of file relationships. Each relation is stored as a Polars DataFrame with
/// associated metadata.
///
/// # Examples
///
/// ```rust
/// use uncp::relations::{RelationStore, RelationKey};
/// use polars::prelude::*;
///
/// // Define relation types
/// struct IdenticalHashes;
/// impl RelationKey for IdenticalHashes {
///     fn name() -> &'static str { "identical_hashes" }
///     fn description() -> &'static str { "Files with identical content hashes" }
///     fn create_schema() -> PolarsResult<DataFrame> {
///         DataFrame::new(vec![
///             Series::new("hash", Vec::<String>::new()),
///             Series::new("paths", Vec::<Vec<String>>::new()),
///         ])
///     }
/// }
///
/// struct SameFileName;
/// impl RelationKey for SameFileName {
///     fn name() -> &'static str { "same_filename" }
///     fn description() -> &'static str { "Files with identical names" }
///     fn create_schema() -> PolarsResult<DataFrame> {
///         DataFrame::new(vec![
///             Series::new("filename", Vec::<String>::new()),
///             Series::new("paths", Vec::<Vec<String>>::new()),
///         ])
///     }
/// }
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut store = RelationStore::new();
///
/// // Insert relations with type-safe keys
/// let hash_data = DataFrame::new(vec![
///     Series::new("hash", vec!["abc123"]),
///     Series::new("paths", vec![vec!["file1.txt", "file2.txt"]]),
/// ])?;
/// store.insert::<IdenticalHashes>(hash_data)?;
///
/// // Retrieve relations with type-safe keys
/// let retrieved = store.get::<IdenticalHashes>().unwrap();
/// assert_eq!(retrieved.height(), 1);
///
/// // Check if a relation exists
/// assert!(store.contains::<IdenticalHashes>());
/// assert!(!store.contains::<SameFileName>());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RelationStore {
	/// Storage for DataFrames indexed by TypeId
	relations: HashMap<TypeId, DataFrame>,
	/// Metadata for each relation
	metadata: HashMap<TypeId, RelationMetadata>,
}

impl RelationStore {
	/// Create a new empty RelationStore
	pub fn new() -> Self {
		Self {
			relations: HashMap::new(),
			metadata: HashMap::new(),
		}
	}

	/// Insert or update a relation with the given key type
	pub fn insert<K: RelationKey>(&mut self, data: DataFrame) -> DetectorResult<()> {
		let type_id = TypeId::of::<K>();

		// Update metadata
		let mut metadata = self
			.metadata
			.get(&type_id)
			.cloned()
			.unwrap_or_else(|| RelationMetadata::new::<K>());
		metadata.update(data.height());

		self.relations.insert(type_id, data);
		self.metadata.insert(type_id, metadata);

		Ok(())
	}

	/// Get a relation by its key type
	pub fn get<K: RelationKey>(&self) -> Option<&DataFrame> {
		let type_id = TypeId::of::<K>();
		self.relations.get(&type_id)
	}

	/// Get a mutable reference to a relation by its key type
	pub fn get_mut<K: RelationKey>(&mut self) -> Option<&mut DataFrame> {
		let type_id = TypeId::of::<K>();
		self.relations.get_mut(&type_id)
	}

	/// Check if a relation exists for the given key type
	pub fn contains<K: RelationKey>(&self) -> bool {
		let type_id = TypeId::of::<K>();
		self.relations.contains_key(&type_id)
	}

	/// Remove a relation by its key type
	pub fn remove<K: RelationKey>(&mut self) -> Option<DataFrame> {
		let type_id = TypeId::of::<K>();
		self.metadata.remove(&type_id);
		self.relations.remove(&type_id)
	}

	/// Get metadata for a relation
	pub fn get_metadata<K: RelationKey>(&self) -> Option<&RelationMetadata> {
		let type_id = TypeId::of::<K>();
		self.metadata.get(&type_id)
	}

	/// Get all relation metadata
	pub fn all_metadata(&self) -> impl Iterator<Item = &RelationMetadata> {
		self.metadata.values()
	}

	/// Get the number of relations stored
	pub fn len(&self) -> usize {
		self.relations.len()
	}

	/// Check if the store is empty
	pub fn is_empty(&self) -> bool {
		self.relations.is_empty()
	}

	/// Clear all relations
	pub fn clear(&mut self) {
		self.relations.clear();
		self.metadata.clear();
	}

	/// Get or create a relation with the default schema
	pub fn get_or_create<K: RelationKey>(&mut self) -> DetectorResult<&mut DataFrame> {
		let type_id = TypeId::of::<K>();

		if !self.relations.contains_key(&type_id) {
			let schema = K::create_schema().map_err(DetectorError::Polars)?;
			self.insert::<K>(schema)?;
		}

		Ok(self.relations.get_mut(&type_id).unwrap())
	}
}

impl Default for RelationStore {
	fn default() -> Self {
		Self::new()
	}
}

/// Merge another RelationStore into this one
impl RelationStore {
	/// Merge relations from another store, combining data where relation types match
	pub fn merge_with(&mut self, other: &RelationStore) -> DetectorResult<()> {
		use polars::prelude::concat;

		for (type_id, other_data) in &other.relations {
			if let Some(existing_data) = self.relations.get_mut(type_id) {
				// Merge DataFrames if relation already exists
				if other_data.height() > 0 {
					let combined = concat(
						[existing_data.clone().lazy(), other_data.clone().lazy()],
						UnionArgs::default(),
					)
					.map_err(DetectorError::Polars)?
					.collect()
					.map_err(DetectorError::Polars)?;

					*existing_data = combined;

					// Update metadata
					if let Some(metadata) = self.metadata.get_mut(type_id) {
						metadata.update(existing_data.height());
					}
				}
			} else {
				// Insert new relation if it doesn't exist
				self.relations.insert(*type_id, other_data.clone());
				if let Some(other_metadata) = other.metadata.get(type_id) {
					self.metadata.insert(*type_id, other_metadata.clone());
				}
			}
		}

		Ok(())
	}
}

// Predefined relation types for common use cases

/// Files with identical content hashes (exact duplicates)
pub struct IdenticalHashes;

impl RelationKey for IdenticalHashes {
	fn name() -> &'static str {
		"identical_hashes"
	}

	fn description() -> &'static str {
		"Files with identical content hashes indicating exact duplicates"
	}

	fn create_schema() -> PolarsResult<DataFrame> {
		use polars::prelude::*;

		let file_paths = ListChunked::full_null_with_dtype("file_paths", 0, &DataType::String);
		DataFrame::new(vec![
			Series::new("hash_value", Vec::<String>::new()),
			Series::new("hash_type", Vec::<String>::new()),
			file_paths.into_series(),
			Series::new("first_seen", Vec::<i64>::new()),
			Series::new("file_count", Vec::<u32>::new()),
		])
	}
}

/// Files with identical names but potentially different paths
pub struct SameFileName;

impl RelationKey for SameFileName {
	fn name() -> &'static str {
		"same_filename"
	}

	fn description() -> &'static str {
		"Files with identical names but potentially different paths"
	}

	fn create_schema() -> PolarsResult<DataFrame> {
		use polars::prelude::*;

		let file_paths = ListChunked::full_null_with_dtype("file_paths", 0, &DataType::String);
		DataFrame::new(vec![
			Series::new("filename", Vec::<String>::new()),
			file_paths.into_series(),
			Series::new("file_count", Vec::<u32>::new()),
			Series::new("first_seen", Vec::<i64>::new()),
		])
	}
}

/// Files with identical sizes
pub struct SameSize;

impl RelationKey for SameSize {
	fn name() -> &'static str {
		"same_size"
	}

	fn description() -> &'static str {
		"Files with identical sizes in bytes"
	}

	fn create_schema() -> PolarsResult<DataFrame> {
		use polars::prelude::*;

		let file_paths = ListChunked::full_null_with_dtype("file_paths", 0, &DataType::String);
		DataFrame::new(vec![
			Series::new("size_bytes", Vec::<u64>::new()),
			file_paths.into_series(),
			Series::new("file_count", Vec::<u32>::new()),
			Series::new("first_seen", Vec::<i64>::new()),
		])
	}
}

/// Similarity groups for near-duplicate detection
pub struct SimilarityGroups;

impl RelationKey for SimilarityGroups {
	fn name() -> &'static str {
		"similarity_groups"
	}

	fn description() -> &'static str {
		"Groups of files with similar content based on perceptual or semantic analysis"
	}

	fn create_schema() -> PolarsResult<DataFrame> {
		use polars::prelude::*;

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
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_type_safe_relation_storage() {
		let mut store = RelationStore::new();

		// Test that store starts empty
		assert!(store.is_empty());
		assert!(!store.contains::<IdenticalHashes>());
		assert!(!store.contains::<SameFileName>());

		// Create some test data for identical hashes
		let file_paths_series = Series::new(
			"file_paths",
			vec![
				Series::new("", vec!["file1.txt", "file2.txt"]),
				Series::new("", vec!["file3.txt", "file4.txt"]),
			],
		);

		let hash_data = DataFrame::new(vec![
			Series::new("hash_value", vec!["abc123", "def456"]),
			Series::new("hash_type", vec!["blake3", "blake3"]),
			file_paths_series,
			Series::new("first_seen", vec![1234567890i64, 1234567891i64]),
			Series::new("file_count", vec![2u32, 2u32]),
		])
		.unwrap();

		// Insert using type-safe key
		store.insert::<IdenticalHashes>(hash_data).unwrap();

		// Verify it was stored
		assert!(!store.is_empty());
		assert!(store.contains::<IdenticalHashes>());
		assert!(!store.contains::<SameFileName>());
		assert_eq!(store.len(), 1);

		// Retrieve using type-safe key
		let retrieved = store.get::<IdenticalHashes>().unwrap();
		assert_eq!(retrieved.height(), 2);

		// Test metadata
		let metadata = store.get_metadata::<IdenticalHashes>().unwrap();
		assert_eq!(metadata.name, "identical_hashes");
		assert_eq!(metadata.row_count, 2);

		// Test that we can't retrieve non-existent relations
		assert!(store.get::<SameFileName>().is_none());
		assert!(store.get_metadata::<SameFileName>().is_none());
	}

	#[test]
	fn test_multiple_relation_types() {
		let mut store = RelationStore::new();

		// Add identical hashes relation
		let file_paths_series = Series::new(
			"file_paths",
			vec![Series::new("", vec!["file1.txt", "file2.txt"])],
		);

		let hash_data = DataFrame::new(vec![
			Series::new("hash_value", vec!["abc123"]),
			Series::new("hash_type", vec!["blake3"]),
			file_paths_series,
			Series::new("first_seen", vec![1234567890i64]),
			Series::new("file_count", vec![2u32]),
		])
		.unwrap();
		store.insert::<IdenticalHashes>(hash_data).unwrap();

		// Add same filename relation
		let filename_paths_series = Series::new(
			"file_paths",
			vec![Series::new(
				"",
				vec!["/home/user/document.pdf", "/backup/document.pdf"],
			)],
		);

		let filename_data = DataFrame::new(vec![
			Series::new("filename", vec!["document.pdf"]),
			filename_paths_series,
			Series::new("file_count", vec![2u32]),
			Series::new("first_seen", vec![1234567890i64]),
		])
		.unwrap();
		store.insert::<SameFileName>(filename_data).unwrap();

		// Verify both are stored
		assert_eq!(store.len(), 2);
		assert!(store.contains::<IdenticalHashes>());
		assert!(store.contains::<SameFileName>());

		// Verify we can retrieve both independently
		let hashes = store.get::<IdenticalHashes>().unwrap();
		let filenames = store.get::<SameFileName>().unwrap();

		assert_eq!(hashes.height(), 1);
		assert_eq!(filenames.height(), 1);

		// Verify metadata for both
		assert_eq!(store.all_metadata().count(), 2);
	}

	#[test]
	fn test_relation_merge() {
		let mut store1 = RelationStore::new();
		let mut store2 = RelationStore::new();

		// Add data to first store
		let file_paths1_series =
			Series::new("file_paths", vec![Series::new("", vec!["file1.txt"])]);

		let data1 = DataFrame::new(vec![
			Series::new("hash_value", vec!["abc123"]),
			Series::new("hash_type", vec!["blake3"]),
			file_paths1_series,
			Series::new("first_seen", vec![1234567890i64]),
			Series::new("file_count", vec![1u32]),
		])
		.unwrap();
		store1.insert::<IdenticalHashes>(data1).unwrap();

		// Add different data to second store
		let file_paths2_series =
			Series::new("file_paths", vec![Series::new("", vec!["file2.txt"])]);

		let data2 = DataFrame::new(vec![
			Series::new("hash_value", vec!["def456"]),
			Series::new("hash_type", vec!["blake3"]),
			file_paths2_series,
			Series::new("first_seen", vec![1234567891i64]),
			Series::new("file_count", vec![1u32]),
		])
		.unwrap();
		store2.insert::<IdenticalHashes>(data2).unwrap();

		// Merge stores
		store1.merge_with(&store2).unwrap();

		// Verify merged data
		let merged = store1.get::<IdenticalHashes>().unwrap();
		assert_eq!(merged.height(), 2);
	}
}
