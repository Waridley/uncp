//! Shared UI presentation layer for TUI/GUI clients

use std::collections::BTreeMap;

use crate::DuplicateDetector;

#[derive(Debug, Clone, Default)]
pub struct PresentationState {
    pub total_files: usize,
    pub pending_hash: usize,
    pub by_type: Vec<(String, usize)>,
    pub status: String,
}

impl PresentationState {
    pub fn from_detector(detector: &DuplicateDetector) -> Self {
        let total_files = detector.total_files();
        let pending_hash = detector.files_pending_hash();
        let map: BTreeMap<String, usize> = detector.files_by_type_counts().into_iter().collect();
        let mut by_type: Vec<(String, usize)> = map.into_iter().collect();
        by_type.sort_by(|a, b| b.1.cmp(&a.1));

        Self { total_files, pending_hash, by_type, status: String::new() }
    }

    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = status.into();
        self
    }
}
