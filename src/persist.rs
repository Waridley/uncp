//! Disk caching and persistence (Parquet + JSON metadata)

use chrono::Utc;
use polars::io::parquet::{ParquetCompression, ParquetReader, ParquetWriter};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::PathBuf;

use crate::data::{RelationStore, ScanState};
use crate::error::{CacheError, CacheResult};
use crate::paths::{DirEntryId, intern_path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanMetadata {
	pub version: u32,
	pub scan_id: u32,
	pub last_scan_time: i64,
	pub file_count: usize,
}

#[derive(Debug)]
pub struct CacheManager {
	pub cache_dir: PathBuf,
}

// Convert in-memory DirEntryId struct path column to Utf8 for serialization
fn df_with_string_paths(mut df: DataFrame) -> PolarsResult<DataFrame> {
	if let Ok(col) = df.column("path") {
		if let Ok(s) = col.struct_() {
			let idx_ca = s.field_by_name("idx")?.u64()?.clone();
			let gen_ca = s.field_by_name("gen")?.u64()?.clone();
			let mut strings = Vec::with_capacity(df.height());
			for i in 0..df.height() {
				let idx = idx_ca.get(i).expect("idx not null") as usize;
				let r#gen = gen_ca.get(i).expect("gen not null") as usize;
				let id = DirEntryId::from_raw_parts_unchecked(idx, r#gen);
				strings.push(id.to_string());
			}
			let new_series = Series::new("path", strings);
			df.replace("path", new_series)?;
		}
	}
	Ok(df)
}

// Convert serialized Utf8 path column to DirEntryId Struct for in-memory use
fn df_with_interned_paths(mut df: DataFrame) -> PolarsResult<DataFrame> {
	if let Ok(col) = df.column("path") {
		if matches!(col.dtype(), DataType::String) {
			let ca = col.str().expect("path column should be Utf8 when on-disk");
			let (mut idxs, mut gens): (Vec<u64>, Vec<u64>) = (
				Vec::with_capacity(df.height()),
				Vec::with_capacity(df.height()),
			);
			for opt_s in ca.into_iter() {
				let s = opt_s.expect("path should not be null");
				let (i, g) = intern_path(s).raw_parts();
				idxs.push(i as u64);
				gens.push(g as u64);
			}
			let struct_series = StructChunked::new(
				"path",
				&[Series::new("idx", idxs), Series::new("gen", gens)],
			)?
			.into_series();
			df.replace("path", struct_series)?;
		}
	}
	Ok(df)
}

impl CacheManager {
	pub fn new(cache_dir: PathBuf) -> Self {
		Self { cache_dir }
	}

	fn meta_path(&self) -> PathBuf {
		self.cache_dir.join("meta.json")
	}
	fn state_path(&self) -> PathBuf {
		self.cache_dir.join("state.parquet")
	}
	fn rel_hashes_path(&self) -> PathBuf {
		self.cache_dir.join("relations_hashes.parquet")
	}
	fn rel_groups_path(&self) -> PathBuf {
		self.cache_dir.join("relations_groups.parquet")
	}

	pub fn ensure_dir(&self) -> CacheResult<()> {
		fs::create_dir_all(&self.cache_dir)?;
		Ok(())
	}

	/// Save state and relations, merging with existing cache data
	pub fn save_all(&self, state: &ScanState, relations: &RelationStore) -> CacheResult<()> {
		self.save_merged(state, relations)
	}

	/// Save state and relations, completely replacing existing cache data
	pub fn save_replace(&self, state: &ScanState, relations: &RelationStore) -> CacheResult<()> {
		self.ensure_dir()?;

		// Helper: write to temp file and atomically rename into place (cross-platform)
		fn atomic_write_parquet(path: PathBuf, mut df: DataFrame) -> CacheResult<()> {
			use std::fs::File;
			use std::io::Write;

			let tmp = path.with_extension("parquet.tmp");
			let mut f = File::create(&tmp)?;
			ParquetWriter::new(&mut f)
				.with_compression(ParquetCompression::Zstd(None))
				.finish(&mut df)?;
			f.flush()?;

			// Ensure data is written to disk before rename
			f.sync_all()?;
			drop(f); // Close file before rename

			// Cross-platform atomic rename: remove target first on Windows
			cross_platform_atomic_rename(&tmp, &path)?;
			Ok(())
		}

		fn atomic_write_json(path: PathBuf, bytes: &[u8]) -> CacheResult<()> {
			let tmp = path.with_extension("json.tmp");
			fs::write(&tmp, bytes)?;

			// Cross-platform atomic rename: remove target first on Windows
			cross_platform_atomic_rename(&tmp, &path)?;
			Ok(())
		}

		// Cross-platform atomic rename that handles Windows limitations
		fn cross_platform_atomic_rename(
			from: &std::path::Path,
			to: &std::path::Path,
		) -> CacheResult<()> {
			#[cfg(windows)]
			{
				// On Windows, remove target file first if it exists
				if to.exists() {
					std::fs::remove_file(to)?;
				}
				std::fs::rename(from, to)?;
			}

			#[cfg(not(windows))]
			{
				// On Unix-like systems, rename is atomic even if target exists
				std::fs::rename(from, to)?;
			}

			Ok(())
		}

		// Save state DataFrame and relations atomically
		use crate::relations::{IdenticalHashes, SimilarityGroups};

		// Convert path column to Utf8 for serialization
		let state_df = df_with_string_paths(state.clone().data.clone()).map_err(|e| {
			CacheError::InvalidationFailed {
				reason: e.to_string(),
			}
		})?;
		atomic_write_parquet(self.state_path(), state_df)?;

		// Save hash relations if they exist (already use Utf8 paths)
		if let Some(hash_relations) = relations.get::<IdenticalHashes>() {
			atomic_write_parquet(self.rel_hashes_path(), hash_relations.clone())?;
		}

		// Save similarity groups if they exist (already use Utf8 paths)
		if let Some(similarity_groups) = relations.get::<SimilarityGroups>() {
			atomic_write_parquet(self.rel_groups_path(), similarity_groups.clone())?;
		}

		// Save metadata JSON last
		let meta = ScanMetadata {
			version: 1,
			scan_id: state.scan_id,
			last_scan_time: Utc::now().timestamp_millis(),
			file_count: state.data.height(),
		};
		let meta_json = serde_json::to_vec_pretty(&meta)?;
		atomic_write_json(self.meta_path(), &meta_json)?;

		Ok(())
	}

	/// Save state and relations, merging with existing cache data
	pub fn save_merged(&self, state: &ScanState, relations: &RelationStore) -> CacheResult<()> {
		self.ensure_dir()?;

		// Load existing cache if it exists
		let (merged_state, merged_relations) =
			if let Some((mut existing_state, mut existing_relations)) = self.load_all()? {
				// Merge new data with existing data
				existing_state
					.merge_with(state)
					.map_err(|e| CacheError::InvalidationFailed {
						reason: format!("Failed to merge scan states: {}", e),
					})?;
				existing_relations.merge_with(relations).map_err(|e| {
					CacheError::InvalidationFailed {
						reason: format!("Failed to merge relation stores: {}", e),
					}
				})?;
				(existing_state, existing_relations)
			} else {
				// No existing cache, use new data as-is
				(state.clone(), relations.clone())
			};

		// Save the merged data using the replace method
		self.save_replace(&merged_state, &merged_relations)
	}

	pub fn load_all(&self) -> CacheResult<Option<(ScanState, RelationStore)>> {
		if !self.state_path().exists() || !self.meta_path().exists() {
			return Ok(None);
		}

		// Load state DF
		let f = File::open(self.state_path())?;
		let mut state_df = ParquetReader::new(f).finish()?;
		// Convert Utf8 path column from disk back to Struct DirEntryId for in-memory use
		state_df =
			df_with_interned_paths(state_df).map_err(|e| CacheError::InvalidationFailed {
				reason: e.to_string(),
			})?;

		// Load relations
		let rel_hashes = if self.rel_hashes_path().exists() {
			ParquetReader::new(File::open(self.rel_hashes_path())?).finish()?
		} else {
			DataFrame::default()
		};
		let rel_groups = if self.rel_groups_path().exists() {
			ParquetReader::new(File::open(self.rel_groups_path())?).finish()?
		} else {
			DataFrame::default()
		};

		// Load metadata
		let meta_bytes = fs::read(self.meta_path())?;
		let meta: ScanMetadata = serde_json::from_slice(&meta_bytes)?;
		let started =
			chrono::DateTime::from_timestamp_millis(meta.last_scan_time).unwrap_or_else(Utc::now);

		use crate::relations::{IdenticalHashes, SimilarityGroups};

		let state = ScanState {
			data: state_df,
			scan_id: meta.scan_id,
			scan_started: started,
		};

		let mut relations = RelationStore::new();

		// Insert the loaded relations using type-safe keys
		if rel_hashes.height() > 0 {
			relations
				.insert::<IdenticalHashes>(rel_hashes)
				.map_err(|e| CacheError::InvalidationFailed {
					reason: e.to_string(),
				})?;
		}
		if rel_groups.height() > 0 {
			relations
				.insert::<SimilarityGroups>(rel_groups)
				.map_err(|e| CacheError::InvalidationFailed {
					reason: e.to_string(),
				})?;
		}

		Ok(Some((state, relations)))
	}

	pub fn cache_exists(&self) -> bool {
		self.state_path().exists() && self.meta_path().exists()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::data::{FileKind, FileRecord};
	use chrono::Utc;
	use std::path::PathBuf;
	use tempfile::TempDir;

	fn create_test_state() -> ScanState {
		let mut state = ScanState::new().unwrap();
		let files = vec![
			FileRecord {
				path: PathBuf::from("/test/file1.txt"),
				size: 100,
				modified: Utc::now(),
				file_type: FileKind::Text,
			},
			FileRecord {
				path: PathBuf::from("/test/file2.jpg"),
				size: 200,
				modified: Utc::now(),
				file_type: FileKind::Image,
			},
		];
		state.add_files(files).unwrap();
		state
	}

	#[test]
	fn test_cache_manager_creation() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		assert_eq!(cache_manager.cache_dir, temp_dir.path());
		assert!(!cache_manager.cache_exists());
	}

	#[test]
	fn test_cache_paths() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		assert_eq!(cache_manager.meta_path(), temp_dir.path().join("meta.json"));
		assert_eq!(
			cache_manager.state_path(),
			temp_dir.path().join("state.parquet")
		);
		assert_eq!(
			cache_manager.rel_hashes_path(),
			temp_dir.path().join("relations_hashes.parquet")
		);
		assert_eq!(
			cache_manager.rel_groups_path(),
			temp_dir.path().join("relations_groups.parquet")
		);
	}

	#[test]
	fn test_ensure_dir() {
		let temp_dir = TempDir::new().unwrap();
		let cache_dir = temp_dir.path().join("cache");
		let cache_manager = CacheManager::new(cache_dir.clone());

		// Directory shouldn't exist initially
		assert!(!cache_dir.exists());

		// ensure_dir should create it
		let result = cache_manager.ensure_dir();
		assert!(result.is_ok());
		assert!(cache_dir.exists());

		// Calling again should be fine
		let result = cache_manager.ensure_dir();
		assert!(result.is_ok());
	}

	#[test]
	fn test_save_and_load_all() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		let state = create_test_state();
		let relations = RelationStore::new();

		// Save
		let result = cache_manager.save_all(&state, &relations);
		assert!(result.is_ok());

		// Verify files were created
		assert!(cache_manager.cache_exists());
		assert!(cache_manager.meta_path().exists());
		assert!(cache_manager.state_path().exists());

		// Load
		let result = cache_manager.load_all();
		assert!(result.is_ok());

		let loaded = result.unwrap();
		assert!(loaded.is_some());

		let (loaded_state, _loaded_relations) = loaded.unwrap();
		assert_eq!(loaded_state.scan_id, state.scan_id);
		assert_eq!(loaded_state.data.height(), state.data.height());
	}

	#[test]
	fn test_load_nonexistent_cache() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		let result = cache_manager.load_all();
		assert!(result.is_ok());

		let loaded = result.unwrap();
		assert!(loaded.is_none());
	}

	#[test]
	fn test_scan_metadata_serialization() {
		let metadata = ScanMetadata {
			version: 1,
			scan_id: 42,
			last_scan_time: 1234567890,
			file_count: 100,
		};

		let json = serde_json::to_string(&metadata).unwrap();
		let deserialized: ScanMetadata = serde_json::from_str(&json).unwrap();

		assert_eq!(deserialized.version, metadata.version);
		assert_eq!(deserialized.scan_id, metadata.scan_id);
		assert_eq!(deserialized.last_scan_time, metadata.last_scan_time);
		assert_eq!(deserialized.file_count, metadata.file_count);
	}

	#[test]
	fn test_cache_exists_partial() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		// No files exist
		assert!(!cache_manager.cache_exists());

		// Create only meta file
		cache_manager.ensure_dir().unwrap();
		std::fs::write(cache_manager.meta_path(), "{}").unwrap();
		assert!(!cache_manager.cache_exists()); // Still false, need both files

		// Create empty state file
		std::fs::write(cache_manager.state_path(), "").unwrap();
		assert!(cache_manager.cache_exists()); // Now true
	}

	#[test]
	fn test_save_with_hashes() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		let mut state = create_test_state();

		// Add some hashes
		let paths = vec!["/test/file1.txt".to_string()];
		let hashes = vec![Some("abc123".to_string())];
		state.update_hashes(paths, hashes).unwrap();

		let relations = RelationStore::new();

		// Save and load
		cache_manager.save_all(&state, &relations).unwrap();
		let (loaded_state, _) = cache_manager.load_all().unwrap().unwrap();

		// Verify hash was preserved
		let hash_col = loaded_state.data.column("blake3_hash").unwrap();
		let first_hash = hash_col.get(0).unwrap();
		let hash_str = first_hash.to_string();
		assert!(hash_str.contains("abc123"));
	}

	#[test]
	fn test_atomic_operations() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		let state = create_test_state();
		let relations = RelationStore::new();

		// Save multiple times to test atomic operations
		for i in 0..3 {
			let mut test_state = state.clone();
			test_state.scan_id = i + 1;

			let result = cache_manager.save_all(&test_state, &relations);
			assert!(result.is_ok());

			// Verify we can load after each save
			let loaded = cache_manager.load_all().unwrap().unwrap();
			assert_eq!(loaded.0.scan_id, i + 1);
		}
	}

	#[test]
	fn test_metadata_timestamp() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		let state = create_test_state();
		let relations = RelationStore::new();

		let before_save = Utc::now().timestamp_millis();
		cache_manager.save_all(&state, &relations).unwrap();
		let after_save = Utc::now().timestamp_millis();

		// Load metadata directly
		let meta_bytes = std::fs::read(cache_manager.meta_path()).unwrap();
		let meta: ScanMetadata = serde_json::from_slice(&meta_bytes).unwrap();

		// Timestamp should be between before and after save
		assert!(meta.last_scan_time >= before_save);
		assert!(meta.last_scan_time <= after_save);
	}

	#[test]
	fn test_cache_merging() {
		let temp_dir = TempDir::new().unwrap();
		let cache_manager = CacheManager::new(temp_dir.path().to_path_buf());

		// Create first state with some files
		let state1 = create_test_state();
		let relations1 = RelationStore::new();

		// Save first state
		cache_manager.save_all(&state1, &relations1).unwrap();

		// Create second state with different files
		let mut state2 = ScanState::new().unwrap();
		let files2 = vec![
			FileRecord {
				path: PathBuf::from("/test/file3.txt"),
				size: 300,
				modified: Utc::now(),
				file_type: FileKind::Text,
			},
			FileRecord {
				path: PathBuf::from("/test/file4.txt"),
				size: 400,
				modified: Utc::now(),
				file_type: FileKind::Text,
			},
		];
		state2.add_files(files2).unwrap();
		let relations2 = RelationStore::new();

		// Save second state (should merge with first)
		cache_manager.save_all(&state2, &relations2).unwrap();

		// Load and verify both sets of files are present
		let (merged_state, _merged_relations) = cache_manager.load_all().unwrap().unwrap();

		// Should have files from both states
		assert_eq!(merged_state.data.height(), 4); // 2 from state1 + 2 from state2

		// Verify all file paths are present
		let path_col = merged_state.data.column("path").unwrap();
		let paths: Vec<String> = match path_col.dtype() {
			DataType::String => path_col
				.str()
				.unwrap()
				.into_iter()
				.map(|opt| opt.unwrap().to_string())
				.collect(),
			DataType::Struct(_) => {
				let s = path_col.struct_().unwrap();
				let idx_ca = s.field_by_name("idx").unwrap().u64().unwrap().clone();
				let gen_ca = s.field_by_name("gen").unwrap().u64().unwrap().clone();
				let mut out = Vec::with_capacity(idx_ca.len());
				for i in 0..idx_ca.len() {
					let idx = idx_ca.get(i).unwrap() as usize;
					let r#gen = gen_ca.get(i).unwrap() as usize;
					out.push(
						crate::paths::DirEntryId::from_raw_parts_unchecked(idx, r#gen).to_string(),
					);
				}
				out
			}
			_ => unreachable!("unexpected dtype for path column"),
		};

		assert!(paths.contains(&"/test/file1.txt".to_string()));
		assert!(paths.contains(&"/test/file2.jpg".to_string()));
		assert!(paths.contains(&"/test/file3.txt".to_string()));
		assert!(paths.contains(&"/test/file4.txt".to_string()));
	}
}
