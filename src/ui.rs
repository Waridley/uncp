//! Shared UI presentation layer for TUI/GUI clients

use std::collections::BTreeMap;

use crate::DuplicateDetector;
use crate::paths::DirEntryId;

#[derive(Debug, Clone, Default)]
pub struct PresentationState {
	pub total_files: usize,
	pub pending_hash: usize,
	pub by_type: Vec<(String, usize)>,
	pub status: String,
	// Optional: pending hash count for the most recent path filter, if provided by engine
	pub pending_hash_scoped: Option<usize>,
	// File table data: (path, size, file_type, hashed)
	pub file_table: Vec<(DirEntryId, u64, String, bool)>,
	// Current path filter for the file table
	pub current_path_filter: String,
}

impl PresentationState {
	pub fn from_detector(detector: &DuplicateDetector) -> Self {
		let total_files = detector.total_files();
		let pending_hash = detector.files_pending_hash();
		let map: BTreeMap<String, usize> = detector.files_by_type_counts().into_iter().collect();
		let mut by_type: Vec<(String, usize)> = map.into_iter().collect();
		by_type.sort_by(|a, b| b.1.cmp(&a.1));

		// Always get file table data for all files (no path filter)
		let file_table = detector.files_under_prefix_sorted_by_size("");

		Self {
			total_files,
			pending_hash,
			by_type,
			status: String::new(),
			pending_hash_scoped: None,
			file_table,
			current_path_filter: String::new(),
		}
	}

	pub fn from_detector_with_path_filter(detector: &DuplicateDetector, path_filter: &str) -> Self {
		let total_files = detector.total_files();
		let pending_hash = detector.files_pending_hash();
		let map: BTreeMap<String, usize> = detector.files_by_type_counts().into_iter().collect();
		let mut by_type: Vec<(String, usize)> = map.into_iter().collect();
		by_type.sort_by(|a, b| b.1.cmp(&a.1));

		let file_table = detector.files_under_prefix_sorted_by_size(path_filter);

		Self {
			total_files,
			pending_hash,
			by_type,
			status: String::new(),
			pending_hash_scoped: None,
			file_table,
			current_path_filter: path_filter.to_string(),
		}
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

use tracing::Level;

/// Cross-platform UI actions shared by TUI and GUI
#[derive(Debug, Clone, Copy)]
pub enum Action {
	// App lifecycle
	Quit,
	Refresh,
	// Operations
	Scan,
	Hash,
	// Popup management
	ShowPathInput,
	ShowFilterInput,
	ToggleLogView,
	ClosePopup,
	// Popup-specific actions
	SubmitPath,
	SubmitFilter,
	FilterSwitchColumn,
	// Navigation
	Up,
	Down,
	PageUp,
	PageDown,
	Home,
	End,
	/// Select a row at (row, column) in the current list/table
	SelectRow(u16, u16),
	// Terminal / viewport events
	/// New viewport size (width, height)
	Resize(u16, u16),
	// Log-specific actions
	ToggleLogLevel(Level),
	LogScrollUp,
	LogScrollDown,
	LogClear,
}

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::log_ui::{LevelToggles, LogDedup};

/// Shared application state for UI clients (TUI/GUI)
#[derive(Debug, Clone)]
pub struct AppState {
	// === UI State ===
	pub current_path: PathBuf,
	pub current_filter: crate::PathFilter,

	// === Progress and Display State ===
	pub progress_state: Arc<Mutex<Option<String>>>,
	pub progress_line: Option<String>,

	// === Log State ===
	pub log_levels: LevelToggles,
	pub log_dedup: LogDedup,
	pub log_scroll: usize,

	// === Engine State ===
	pub current_discovery_progress: Option<crate::systems::SystemProgress>,
	pub current_hashing_progress: Option<crate::systems::SystemProgress>,
	pub engine_status: String,
	pub processing_speed: Option<f64>,
	pub last_progress_update: Instant,
	pub last_processed_count: usize,

	// === Presentation State ===
	pub pres: PresentationState,

	// === Selection State ===
	pub selected_idx: usize,
}

impl Default for AppState {
	fn default() -> Self {
		Self {
			current_path: PathBuf::from("."),
			current_filter: crate::PathFilter::default(),
			progress_state: Arc::new(Mutex::new(None)),
			progress_line: None,
			log_levels: LevelToggles::default(),
			log_dedup: LogDedup::new(),
			log_scroll: 0,
			current_discovery_progress: None,
			current_hashing_progress: None,
			engine_status: "Starting...".to_string(),
			processing_speed: None,
			last_progress_update: Instant::now(),
			last_processed_count: 0,
			pres: PresentationState::default()
				.with_status("Press 's' to scan, 'h' to hash, 'r' to refresh, 'q' to quit"),
			selected_idx: 0,
		}
	}
}
