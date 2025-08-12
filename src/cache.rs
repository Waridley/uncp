//! Disk caching and persistence (Parquet + JSON metadata)

use chrono::Utc;
use polars::io::parquet::{ParquetCompression, ParquetReader, ParquetWriter};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::PathBuf;

use crate::data::{RelationStore, ScanState};
use crate::error::CacheResult;

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
	fn rel_pairs_path(&self) -> PathBuf {
		self.cache_dir.join("relations_pairs.parquet")
	}

	pub fn ensure_dir(&self) -> CacheResult<()> {
		fs::create_dir_all(&self.cache_dir)?;
		Ok(())
	}

	pub fn save_all(&self, state: &ScanState, relations: &RelationStore) -> CacheResult<()> {
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
		atomic_write_parquet(self.state_path(), state.clone().data.clone())?;
		atomic_write_parquet(
			self.rel_hashes_path(),
			relations.clone().hash_relations.clone(),
		)?;
		atomic_write_parquet(
			self.rel_groups_path(),
			relations.clone().similarity_groups.clone(),
		)?;
		atomic_write_parquet(
			self.rel_pairs_path(),
			relations.clone().pairwise_relations.clone(),
		)?;

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

	pub fn load_all(&self) -> CacheResult<Option<(ScanState, RelationStore)>> {
		if !self.state_path().exists() || !self.meta_path().exists() {
			return Ok(None);
		}

		// Load state DF
		let f = File::open(self.state_path())?;
		let state_df = ParquetReader::new(f).finish()?;

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
		let rel_pairs = if self.rel_pairs_path().exists() {
			ParquetReader::new(File::open(self.rel_pairs_path())?).finish()?
		} else {
			DataFrame::default()
		};

		// Load metadata
		let meta_bytes = fs::read(self.meta_path())?;
		let meta: ScanMetadata = serde_json::from_slice(&meta_bytes)?;
		let started =
			chrono::DateTime::from_timestamp_millis(meta.last_scan_time).unwrap_or_else(Utc::now);

		let state = ScanState {
			data: state_df,
			scan_id: meta.scan_id,
			scan_started: started,
		};
		let relations = RelationStore {
			hash_relations: rel_hashes,
			similarity_groups: rel_groups,
			pairwise_relations: rel_pairs,
		};

		Ok(Some((state, relations)))
	}

	pub fn cache_exists(&self) -> bool {
		self.state_path().exists() && self.meta_path().exists()
	}
}
