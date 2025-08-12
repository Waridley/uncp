//! Shared UI presentation layer for TUI/GUI clients

use std::collections::BTreeMap;

use crate::DuplicateDetector;

#[derive(Debug, Clone, Default)]
pub struct PresentationState {
    pub total_files: usize,
    pub pending_hash: usize,
    pub by_type: Vec<(String, usize)>,
    pub status: String,
    // Optional: pending hash count for the most recent path filter, if provided by engine
    pub pending_hash_scoped: Option<usize>,
}

impl PresentationState {
    pub fn from_detector(detector: &DuplicateDetector) -> Self {
        let total_files = detector.total_files();
        let pending_hash = detector.files_pending_hash();
        let map: BTreeMap<String, usize> = detector.files_by_type_counts().into_iter().collect();
        let mut by_type: Vec<(String, usize)> = map.into_iter().collect();
        by_type.sort_by(|a, b| b.1.cmp(&a.1));

        Self { total_files, pending_hash, by_type, status: String::new(), pending_hash_scoped: None }
    }

    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = status.into();
        self
    }

}

impl PresentationState {
    pub fn pending_hash_under_prefix<S: AsRef<str>>(&self, _prefix: S) -> usize {
        self.pending_hash_scoped.unwrap_or(self.pending_hash)
    }
}
