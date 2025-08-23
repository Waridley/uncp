//! Content hashing system

use async_trait::async_trait;
use blake3;
use polars::prelude::*;

use crate::data::ScanState;
use crate::error::{SystemError, SystemResult};
use crate::memory::MemoryManager;
use crate::systems::{System, SystemProgress, SystemRunner};
use tracing::info;

#[derive(Default)]
pub struct ContentHashSystem {
	pub scope_prefix: Option<String>,
	pub callback: ProgressCb,
	pub load_queue:
		Option<std::sync::Arc<std::sync::Mutex<crate::content_queue::ContentLoadQueue>>>,
}
impl ContentHashSystem {
	pub fn new() -> Self {
		Self::default()
	}
	pub fn with_scope_prefix<S: Into<String>>(mut self, prefix: S) -> Self {
		self.scope_prefix = Some(prefix.into());
		self
	}
	pub fn with_progress_callback(
		mut self,
		cb: std::sync::Arc<dyn Fn(SystemProgress) + Send + Sync>,
	) -> Self {
		self.callback = Some(cb);
		self
	}
	pub fn with_load_queue(
		mut self,
		queue: std::sync::Arc<std::sync::Mutex<crate::content_queue::ContentLoadQueue>>,
	) -> Self {
		self.load_queue = Some(queue);
		self
	}
}

pub type ProgressCb = Option<std::sync::Arc<dyn Fn(SystemProgress) + Send + Sync>>;

#[async_trait]
impl SystemRunner for ContentHashSystem {
	async fn run(&self, state: &mut ScanState, memory_mgr: &mut MemoryManager) -> SystemResult<()> {
		// Get rows needing hashing
		let to_hash_df = state
			.files_needing_processing("content_hash")
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?;
		info!("Hashing: {} files pending", to_hash_df.height());

		// Apply scope filter if configured
		let to_hash_df = if let Some(ref pref) = self.scope_prefix {
			// Filter by idx/gen using DirEntryId descendant check
			let s = to_hash_df
				.column("path")
				.and_then(|c| c.struct_())
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?;
			let idx_series = s
				.field_by_name("idx")
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?
				.clone();
			let gen_series = s
				.field_by_name("gen")
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?
				.clone();
			let idx_ca = idx_series
				.u64()
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?
				.clone();
			let gen_ca = gen_series
				.u64()
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?
				.clone();
			let mut mask = Vec::with_capacity(to_hash_df.height());
			for i in 0..to_hash_df.height() {
				let keep = if let (Some(idx), Some(r#gen)) = (idx_ca.get(i), gen_ca.get(i)) {
					if let Some(id) =
						crate::paths::DirEntryId::from_raw_parts(idx as usize, r#gen as usize)
					{
						id.is_descendant_of_path(pref)
					} else {
						false
					}
				} else {
					false
				};
				mask.push(keep);
			}
			to_hash_df
				.filter(&BooleanChunked::from_slice("mask".into(), &mask))
				.map_err(|e| SystemError::ExecutionFailed {
					system: self.name().into(),
					reason: e.to_string(),
				})?
		} else {
			to_hash_df
		};

		let t_start = std::time::Instant::now();

		if to_hash_df.height() == 0 {
			return Ok(());
		}

		// Collect paths: resolve Struct[idx, gen] to Strings
		let s = to_hash_df
			.column("path")
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.struct_()
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?;
		let idx_series = s
			.field_by_name("idx")
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.clone();
		let gen_series = s
			.field_by_name("gen")
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.clone();
		let idx_ca = idx_series
			.u64()
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.clone();
		let gen_ca = gen_series
			.u64()
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?
			.clone();
		let mut ids: Vec<crate::paths::DirEntryId> = Vec::with_capacity(to_hash_df.height());
		for i in 0..to_hash_df.height() {
			if let (Some(idx), Some(r#gen)) = (idx_ca.get(i), gen_ca.get(i))
				&& let Some(id) =
					crate::paths::DirEntryId::from_raw_parts(idx as usize, r#gen as usize)
			{
				ids.push(id);
			}
		}

		let total_files = ids.len();
		let callback = self.callback.clone();

		// Sequential cache-first hashing to avoid double-allocations and borrows across threads
		let mut upd_paths: Vec<String> = Vec::with_capacity(total_files);
		let mut upd_hashes: Vec<Option<String>> = Vec::with_capacity(total_files);
		let mut upd_flags: Vec<bool> = Vec::with_capacity(total_files);
		let mut processed = 0usize;

		for id in ids.into_iter() {
			let path_str = id.to_string();
			let hash_opt = if let Some(ref queue) = self.load_queue {
				// Prefer cache; if missing, enqueue and skip this cycle
				match memory_mgr.get_file(&id.resolve()) {
					Some(bytes) => Some(blake3::hash(&bytes.data).to_hex().to_string()),
					None => {
						let mut q = queue.lock().unwrap();
						q.enqueue(id);
						None
					}
				}
			} else {
				// Legacy fallback: read from disk directly
				match std::fs::read(id.resolve()) {
					Ok(bytes) => Some(blake3::hash(&bytes).to_hex().to_string()),
					Err(_) => None,
				}
			};

			let current_item = path_str.clone();
			upd_paths.push(path_str);
			upd_hashes.push(hash_opt.clone());
			upd_flags.push(hash_opt.is_some());

			processed += 1;
			if let Some(ref cb) = callback {
				cb(SystemProgress {
					system_name: "ContentHash".to_string(),
					total_items: total_files,
					processed_items: processed,
					current_item: Some(current_item),
					estimated_remaining: None,
				});
			}
		}

		if upd_paths.is_empty() {
			return Ok(());
		}

		// Create update frame: convert paths back to Struct[idx, gen]
		let (idxs, gens): (Vec<u64>, Vec<u64>) = upd_paths
			.iter()
			.map(|p| {
				let (i, g) = crate::paths::intern_path(p).raw_parts();
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
		)
		.map_err(|e| SystemError::ExecutionFailed {
			system: self.name().into(),
			reason: e.to_string(),
		})?;
		let update_df = DataFrame::new(vec![
			path_series.into(),
			Series::new("blake3_hash".into(), upd_hashes).into(),
			Series::new("hashed".into(), upd_flags).into(),
		])
		.map_err(|e| SystemError::ExecutionFailed {
			system: self.name().into(),
			reason: e.to_string(),
		})?;

		// Merge updates into state using a left join and coalesce
		let left_lf = state.data.clone().lazy().with_columns([
			col("path").struct_().field_by_index(0).alias("__join_idx"),
			col("path").struct_().field_by_index(1).alias("__join_gen"),
		]);

		let right_lf = update_df.clone().lazy().with_columns([
			col("path").struct_().field_by_index(0).alias("__join_idx"),
			col("path").struct_().field_by_index(1).alias("__join_gen"),
		]);

		let updated = left_lf
			.join(
				right_lf,
				[col("__join_idx"), col("__join_gen")],
				[col("__join_idx"), col("__join_gen")],
				JoinArgs::new(JoinType::Left),
			)
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
			.drop(cols([
				"blake3_hash_right",
				"hashed_right",
				"__join_idx",
				"__join_gen",
			]))
			.collect()
			.map_err(|e| SystemError::ExecutionFailed {
				system: self.name().into(),
				reason: e.to_string(),
			})?;

		let dur = t_start.elapsed();
		info!("Hashing: committed updates in {:?}", dur);

		/* cleanup placeholder: removed old WithProgress and Scoped implementations */

		state.data = updated;
		Ok(())
	}

	fn can_run(&self, _state: &ScanState) -> bool {
		true
	}
	fn priority(&self) -> u8 {
		0
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::data::ScanState;
	use crate::memory::MemoryManager;

	#[test]
	fn test_content_hash_system_creation() {
		let hash_system = ContentHashSystem::new();

		assert_eq!(hash_system.name(), "ContentHash");
		assert_eq!(hash_system.priority(), 200);
		assert_eq!(hash_system.required_columns(), &["path"]);
		assert_eq!(hash_system.optional_columns(), &[] as &[&str]);
		assert!(hash_system.can_run(&ScanState::new().unwrap()));
	}

	#[test]
	fn test_blake3_hash_consistency() {
		let content = b"test content for hashing";
		let hash1 = blake3::hash(content);
		let hash2 = blake3::hash(content);

		// Same content should produce same hash
		assert_eq!(hash1.to_hex(), hash2.to_hex());

		// Different content should produce different hash
		let different_content = b"different test content";
		let hash3 = blake3::hash(different_content);
		assert_ne!(hash1.to_hex(), hash3.to_hex());
	}

	#[smol_potat::test]
	async fn test_hash_system_empty_state() {
		let hash_system = ContentHashSystem::new();
		let mut state = ScanState::new().unwrap();
		let mut memory_mgr = MemoryManager::new().unwrap();

		// Should handle empty state gracefully
		let result = hash_system.run(&mut state, &mut memory_mgr).await;
		assert!(result.is_ok());
		assert_eq!(state.data.height(), 0);
	}

	#[test]
	fn test_system_interface() {
		let hash_system = ContentHashSystem::new();

		assert_eq!(
			hash_system.description(),
			"Computes content hashes (exact blake3) for files (PoC)"
		);
		assert!(hash_system.can_run(&ScanState::new().unwrap()));
	}
}
