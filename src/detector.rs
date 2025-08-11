//! Main API for duplicate detection

use std::path::{Path, PathBuf};

use crate::data::{RelationStore, ScanState};
use crate::error::{DetectorError, DetectorResult};
use crate::memory::{MemoryManager, Settings};
use crate::query::Query;
use crate::systems::{FileDiscoverySystem, ContentHashSystem, SystemScheduler};
use tracing::{info, debug};

#[derive(Debug)]
pub struct DetectorConfig {
    pub memory_settings: Settings,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            memory_settings: Settings::from_sysinfo().expect("failed to read sysinfo"),
        }
    }
}

pub struct DuplicateDetector {
    pub state: ScanState,
    pub relations: RelationStore,
    pub scheduler: SystemScheduler,
    pub memory_mgr: MemoryManager,
}

impl DuplicateDetector {
    pub fn new(config: DetectorConfig) -> DetectorResult<Self> {
        Ok(Self {
            state: ScanState::new()?,
            relations: RelationStore::new()?,
            scheduler: SystemScheduler::new(),
            memory_mgr: MemoryManager::with_settings(config.memory_settings)?,
        })
    }

    pub async fn scan_directory(&mut self, path: PathBuf) -> DetectorResult<()> {
        info!("Detector: scan_directory {}", path.display());
        let discovery = FileDiscoverySystem::new(vec![path]);
        self.scheduler.add_system(discovery);
        self.scheduler.run_all(&mut self.state, &mut self.memory_mgr).await.map_err(DetectorError::from)
    }

    pub async fn scan_and_hash(&mut self, path: PathBuf) -> DetectorResult<()> {
        info!("Detector: scan_and_hash {}", path.display());
        // Run discovery first
        let discovery = FileDiscoverySystem::new(vec![path]);
        self.scheduler.add_system(discovery);
        // Then hashing
        self.scheduler.add_system(ContentHashSystem);
        self.scheduler
            .run_all(&mut self.state, &mut self.memory_mgr)
            .await
            .map_err(DetectorError::from)
    }

    pub async fn process_until_complete(&mut self) -> DetectorResult<()> {
        self.scheduler.run_all(&mut self.state, &mut self.memory_mgr).await.map_err(DetectorError::from)
    }

    pub fn query(&self) -> Query {
        Query::new(&self.state, &self.relations)
    }

    // Disk caching stubs for now
    pub async fn save_cache(&self, _cache_path: &Path) -> DetectorResult<()> { Ok(()) }
    pub async fn load_cache(&mut self, _cache_path: &Path) -> DetectorResult<bool> { Ok(false) }
    pub fn is_cache_valid(&self, _cache_path: &Path) -> DetectorResult<bool> { Ok(false) }
    pub fn total_files(&self) -> usize { self.state.data.height() }
    pub fn files_pending_hash(&self) -> usize {
        self.state.data.column("hashed").ok()
            .and_then(|s| s.bool().ok())
            .map(|b| b.into_iter().filter(|v| matches!(v, Some(false))).count())
            .unwrap_or(0)
    }
    pub fn files_by_type_counts(&self) -> std::collections::HashMap<String, usize> {
        let mut map = std::collections::HashMap::new();
        if let Ok(s) = self.state.data.column("file_type") {
            if let Ok(utf8) = s.str() { for v in utf8.into_iter().flatten() { *map.entry(v.to_string()).or_insert(0) += 1; } }
        }
        map
    }

}

