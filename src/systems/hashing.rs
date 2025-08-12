//! Content hashing system

use async_trait::async_trait;
use blake3;
use polars::prelude::*;

use crate::data::ScanState;
use crate::error::{SystemError, SystemResult};
use crate::memory::MemoryManager;
use crate::systems::{System, SystemProgress, SystemRunner};
use tracing::{info, trace, warn};

#[derive(Debug)]
pub struct ContentHashSystem;

pub type ProgressCb = Option<std::sync::Arc<dyn Fn(SystemProgress) + Send + Sync>>;

pub struct ContentHashSystemWithProgress { pub callback: ProgressCb }

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


			let t_start = std::time::Instant::now();

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

			let dur = t_start.elapsed();
			info!("Hashing: committed updates in {:?}", dur);


		state.data = updated;
		Ok(())

        }

        fn can_run(&self, _state: &ScanState) -> bool { true }
        fn priority(&self) -> u8 { 200 }
        fn name(&self) -> &'static str { "ContentHash" }
    }





#[async_trait]
impl SystemRunner for ContentHashSystemWithProgress {
    async fn run(&self, state: &mut ScanState, _memory_mgr: &mut MemoryManager) -> SystemResult<()> {
        // mirror ContentHashSystem::run with progress reports
        let to_hash_df = state
            .files_needing_processing("content_hash")
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;
        info!("Hashing: {} files pending", to_hash_df.height());
        if to_hash_df.height() == 0 { return Ok(()); }

        let paths_series = to_hash_df
            .column("path")
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?
            .str()

			let t_start = std::time::Instant::now();

            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;

        let mut upd_paths: Vec<String> = Vec::with_capacity(paths_series.len());
        let mut upd_hashes: Vec<Option<String>> = Vec::with_capacity(paths_series.len());
        let mut upd_flags: Vec<bool> = Vec::with_capacity(paths_series.len());

        let total = paths_series.len();
        let mut progress = SystemProgress::new(self.name().to_string(), total);
        let mut processed = 0usize;

        for opt_path in paths_series.into_iter() {
            if let Some(path) = opt_path {
                upd_paths.push(path.to_string());
                match smol::fs::read(path).await {
                    Ok(bytes) => {
                        let digest = blake3::hash(&bytes);
                        upd_hashes.push(Some(digest.to_hex().to_string()));
                        upd_flags.push(true);
                    }
                    Err(_) => {
                        upd_hashes.push(None);
                        upd_flags.push(false);
                        warn!("Hashing: failed to read {}", path);
                    }
                }
                processed += 1;
                progress.update(processed, upd_paths.last().cloned());
                if let Some(ref cb) = self.callback { cb(progress.clone()); }
            }
        }

        if upd_paths.is_empty() { return Ok(()); }

        let update_df = df! { "path" => upd_paths, "blake3_hash" => upd_hashes, "hashed" => upd_flags }
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;

        let updated = state.data.clone().lazy()
            .left_join(update_df.clone().lazy(), col("path"), col("path"))
            .with_columns([
                when(col("blake3_hash_right").is_not_null()).then(col("blake3_hash_right")).otherwise(col("blake3_hash")).alias("blake3_hash"),
                when(col("hashed_right").is_not_null()).then(col("hashed_right")).otherwise(col("hashed")).alias("hashed"),
            ])
            .select([all().exclude(["blake3_hash_right", "hashed_right"])])
            .collect()
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;

        state.data = updated;
        Ok(())
    }

    fn can_run(&self, _state: &ScanState) -> bool { true }
    fn priority(&self) -> u8 { 200 }
    fn name(&self) -> &'static str { "ContentHash" }
}


// Keep ContentHashSystem impls below intact


#[derive(Debug)]
pub struct ContentHashSystemScoped { prefix: String }
impl ContentHashSystemScoped { pub fn new<P: AsRef<std::path::Path>>(p: P) -> Self { Self { prefix: p.as_ref().to_string_lossy().to_string() } } }

#[async_trait]
impl SystemRunner for ContentHashSystemScoped {
    async fn run(&self, state: &mut ScanState, _memory_mgr: &mut MemoryManager) -> SystemResult<()> {
        // Filter to only files under the requested path and needing hashing
        let to_hash_df = state
            .data
            .clone()
            .lazy()
            .filter(col("hashed").eq(lit(false)))
            .filter(col("path").str().starts_with(lit(self.prefix.as_str())))
            .collect()
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;
        info!("Hashing (scoped): {} files pending under {}", to_hash_df.height(), self.prefix);
        if to_hash_df.height() == 0 { return Ok(()); }

        let paths_series = to_hash_df
            .column("path")
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?
            .str()
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;

        let mut upd_paths: Vec<String> = Vec::with_capacity(paths_series.len());
        let mut upd_hashes: Vec<Option<String>> = Vec::with_capacity(paths_series.len());
        let mut upd_flags: Vec<bool> = Vec::with_capacity(paths_series.len());

        for opt_path in paths_series.into_iter() {
            if let Some(path) = opt_path {
                upd_paths.push(path.to_string());
                match smol::fs::read(path).await {
                    Ok(bytes) => {
                        let digest = blake3::hash(&bytes);
                        upd_hashes.push(Some(digest.to_hex().to_string()));
                        upd_flags.push(true);
                    }
                    Err(_) => {
                        upd_hashes.push(None);
                        upd_flags.push(false);
                        warn!("Hashing: failed to read {}", path);
                    }
                }
            }
        }

        if upd_paths.is_empty() { return Ok(()); }

        let update_df = df! { "path" => upd_paths, "blake3_hash" => upd_hashes, "hashed" => upd_flags }
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;

        let updated = state.data.clone().lazy()
            .left_join(update_df.clone().lazy(), col("path"), col("path"))
            .with_columns([
                when(col("blake3_hash_right").is_not_null()).then(col("blake3_hash_right")).otherwise(col("blake3_hash")).alias("blake3_hash"),
                when(col("hashed_right").is_not_null()).then(col("hashed_right")).otherwise(col("hashed")).alias("hashed"),
            ])
            .select([all().exclude(["blake3_hash_right", "hashed_right"])])
            .collect()
            .map_err(|e| SystemError::ExecutionFailed { system: self.name().into(), reason: e.to_string() })?;

        state.data = updated;
        Ok(())
    }
    fn can_run(&self, _state: &ScanState) -> bool { true }
    fn priority(&self) -> u8 { 200 }
    fn name(&self) -> &'static str { "ContentHashScoped" }
}

/*
/* BEGIN stray duplicate block (commented out)

	}

	fn can_run(&self, _state: &ScanState) -> bool {
		true
	}

	fn priority(&self) -> u8 {
		200
	}

	fn name(&self) -> &'static str {
		"ContentHash"
*/

*/

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
