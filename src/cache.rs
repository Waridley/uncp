//! Disk caching stub module (to be expanded)

use std::path::PathBuf;
use serde::{Deserialize, Serialize};

use crate::error::{CacheResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanMetadata {
    pub scan_id: u32,
    pub last_scan_time: u64,
    pub file_count: usize,
    pub directory_tree_hash: String,
}

#[derive(Debug, Default)]
pub struct CacheManager {
    pub cache_dir: PathBuf,
}

impl CacheManager {
    pub fn new(cache_dir: PathBuf) -> Self { Self { cache_dir } }

    pub fn save_metadata(&self, _meta: &ScanMetadata) -> CacheResult<()> { Ok(()) }
    pub fn load_metadata(&self) -> CacheResult<Option<ScanMetadata>> { Ok(None) }
}
