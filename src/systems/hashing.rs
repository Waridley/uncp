//! Content hashing system

use async_trait::async_trait;
use blake3;
use polars::prelude::*;

use crate::data::ScanState;
use crate::error::{SystemError, SystemResult};
use crate::memory::MemoryManager;
use crate::systems::{System, SystemRunner};
use tracing::{info, trace, warn};

#[derive(Debug)]
pub struct ContentHashSystem;

#[async_trait]
impl SystemRunner for ContentHashSystem {
	async fn run(
		&self,
		state: &mut ScanState,
		_memory_mgr: &mut MemoryManager,
	) -> SystemResult<()> {
		// Get rows needing hashing
		let to_hash_df = state
			.files_needing_processing("content_hash")
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?;
		info!("Hashing: {} files pending", to_hash_df.height());

		if to_hash_df.height() == 0 {
			return Ok(());
		}

		// Collect paths
		let paths_series = to_hash_df
			.column("path")
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.str()
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?;

		let mut upd_paths: Vec<String> = Vec::with_capacity(paths_series.len());
		let mut upd_hashes: Vec<Option<String>> = Vec::with_capacity(paths_series.len());
		let mut upd_flags: Vec<bool> = Vec::with_capacity(paths_series.len());

		for opt_path in paths_series.into_iter() {
			if let Some(path) = opt_path {
				upd_paths.push(path.to_string());
				// Try hashing; on error, skip but leave hashed=false so it can retry or be logged later
				match smol::fs::read(path).await {
					Ok(bytes) => {
						let digest = blake3::hash(&bytes);
						upd_hashes.push(Some(digest.to_hex().to_string()));
						upd_flags.push(true);
						trace!("Hashed {}", path);
					}
					Err(_) => {
						upd_hashes.push(None);
						upd_flags.push(false);
						warn!("Hashing: failed to read {}", path);
					}
				}
			}
		}

		if upd_paths.is_empty() {
			return Ok(());
		}

		// Create update frame
		let update_df = df! {
			"path" => upd_paths,
			"blake3_hash" => upd_hashes,
			"hashed" => upd_flags,
		}
		.map_err(|e| SystemError::ExecutionFailed {
			system: self.name().into(),
			reason: e.to_string(),
		})?;

		// Merge updates into state using a left join and coalesce
		let updated = state
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
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?;

		state.data = updated;
		Ok(())
	}

	fn can_run(&self, _state: &ScanState) -> bool {
		true
	}

	fn priority(&self) -> u8 {
		200
	}

	fn name(&self) -> &'static str {
		"ContentHash"
	}
}

impl System for ContentHashSystem {
	fn required_columns(&self) -> &[&'static str] {
		// In this PoC we only require path
		&["path"]
	}

	fn optional_columns(&self) -> &[&'static str] {
		&[]
	}

	fn description(&self) -> &'static str {
		"Computes content hashes (exact blake3) for files (PoC)"
	}
}
