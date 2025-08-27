//! # UNCP TUI - Terminal User Interface for Duplicate File Detection
//!
//! This module provides a full-featured terminal user interface for the UNCP
//! duplicate file detection system. The TUI offers real-time progress monitoring,
//! interactive file browsing, filtering capabilities, and comprehensive error
//! reporting.
//!
//! ## Architecture
//!
//! The TUI is built using the ratatui library and follows a clean separation of
//! concerns with the following main components:
//!
//! - **Application State**: Centralized state management in `AppState`
//! - **Event Processing**: User input and engine event handling
//! - **Rendering**: Modular UI rendering functions for each interface section
//! - **Engine Communication**: Async communication with the background engine
//!
//! ## Key Features
//!
//! - **Performance Optimized**: Only renders visible table rows for large datasets
//! - **Real-time Updates**: Live progress monitoring and file discovery
//! - **Interactive Navigation**: Keyboard and mouse support for file browsing
//! - **Advanced Filtering**: Include/exclude pattern support with live preview
//! - **Error Reporting**: Comprehensive error display with log popup
//! - **Responsive Design**: Adaptive layout that works in various terminal sizes
//!
//! ## Main Components
//!
//! - Header: Summary information (file counts, active filters)
//! - File Table: Optimized display of discovered files with sorting
//! - Status Footer: Real-time progress and error information
//! - Popups: Modal dialogs for path input, filtering, and log viewing
//! - Keybinding Hints: Context-sensitive help text

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_channel as channel;
use async_executor::Executor;
use futures_lite::future;

use crossterm::event::{self, Event};
use crossterm::execute;
use ratatui::{
	layout::{Alignment, Constraint, Direction, Layout, Rect},
	style::{Color, Modifier, Style},
	text::{Line, Span},
	widgets::{Block, Borders, Clear, Paragraph, TableState, Wrap},
	Frame,
};

use tracing::{debug, info, trace};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use uncp::engine::{BackgroundEngine, EngineCommand, EngineEvent};
use uncp::log_ui::UiErrorEvent;
use uncp::log_ui::UiErrorLayer;
use uncp::ui::PresentationState;

const SPINNER: &[char] = &['⠋', '⠙', '⠸', '⠴', '⠦', '⠇'];

fn level_at_least(event_level: tracing::Level, min_level: tracing::Level) -> bool {
	fn to_int(lvl: tracing::Level) -> u8 {
		match lvl.as_str() {
			"ERROR" => 5,
			"WARN" => 4,
			"INFO" => 3,
			"DEBUG" => 2,
			"TRACE" => 1,
			_ => 0,
		}
	}
	to_int(event_level) >= to_int(min_level)
}

fn main() -> std::io::Result<()> {
	let log_dir =
		uncp::paths::default_cache_dir().unwrap_or_else(|| std::env::temp_dir().join("uncp"));
	let _ = std::fs::create_dir_all(&log_dir);
	let log_path = log_dir.join("tui.log");
	let file = std::fs::File::create(&log_path).expect("open log file");
	let (nb, guard) = tracing_appender::non_blocking(file);

	// Tracing subscriber -> write to file (non-blocking), not the terminal
	let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	let (ui_layer, ui_err_handle) = UiErrorLayer::new(200);
	tracing_subscriber::registry()
		.with(fmt::layer().with_writer(nb))
		.with(env_filter)
		.with(ui_layer)
		.init();
	// Tip: set RUST_LOG=uncp_tui=debug,uncp=info to change verbosity

	info!("Starting uncp TUI");

	// Build a hybrid rayon + smol executor setup
	let ex = std::sync::Arc::new(Executor::new());
	let (signal, shutdown) = channel::unbounded::<()>();

	// Configure rayon thread pool for CPU-bound work
	let nthreads = std::thread::available_parallelism()
		.map(|n| n.get())
		.unwrap_or(4)
		.max(2);

	// Initialize rayon with our desired thread count
	rayon::ThreadPoolBuilder::new()
		.num_threads(nthreads)
		.thread_name(|i| format!("uncp-rayon-{}", i))
		.build_global()
		.expect("Failed to initialize rayon thread pool");

	info!("Initialized rayon thread pool with {} threads", nthreads);

	// Spawn smol executor threads for async I/O work
	let executor_threads: Vec<_> = (0..2) // Use fewer threads for async work
		.map(|i| {
			let ex = ex.clone();
			let shutdown = shutdown.clone();
			std::thread::Builder::new()
				.name(format!("uncp-smol-{}", i))
				.spawn(move || future::block_on(ex.run(shutdown.recv())))
				.expect("Failed to spawn smol executor thread")
		})
		.collect();

	// Run the TUI on the main thread
	{
		// Explicitly enable mouse support and ensure resize events work
		let _ = execute!(
			std::io::stdout(),
			crossterm::event::EnableMouseCapture,
			crossterm::terminal::EnableLineWrap
		);

		let mut terminal = ratatui::init();
		let _ = run(&ex, &mut terminal, &ui_err_handle);

		// Disable mouse support on exit
		let _ = execute!(std::io::stdout(), crossterm::event::DisableMouseCapture);
		ratatui::restore();
	}

	// Signal shutdown to all worker threads
	drop(signal);

	// Wait for executor threads to finish
	for thread in executor_threads {
		let _ = thread.join();
	}

	// Flush appender and print tail of log after UI restores
	drop(guard);
	if let Ok(s) = std::fs::read_to_string(&log_path) {
		let tail: Vec<_> = s.lines().rev().take(80).collect();
		eprintln!("\n---- uncp TUI logs (last 80 lines) ----");
		for line in tail.into_iter().rev() {
			eprintln!("{line}");
		}
		eprintln!(
			"---------------------------------------\nLog file: {}",
			log_path.display()
		);
	} else {
		eprintln!("Logs at: {}", log_path.display());
	}

	Ok(())
}

/// Main TUI application entry point.
///
/// Initializes the application state, starts the background engine,
/// and runs the main event loop until the user quits.
///
/// # Arguments
/// * `_ex` - Async executor (currently unused)
/// * `terminal` - Ratatui terminal instance for rendering
/// * `ui_errs` - Handle to UI error queue for displaying errors
///
/// # Returns
/// Result indicating success or IO error
fn run(
	_ex: &std::sync::Arc<Executor<'_>>,
	terminal: &mut ratatui::DefaultTerminal,
	ui_errs: &uncp::log_ui::UiErrorQueueHandle,
) -> std::io::Result<()> {
	debug!("TUI loop start");

	// Initialize application state
	let mut app_state = AppState::new();
	let (evt_rx, cmds) = initialize_engine(&mut app_state)?;

	// Main event loop
	main_event_loop(terminal, ui_errs, &mut app_state, evt_rx, cmds)
}

/// Central application state container for the TUI.
///
/// This struct holds all the mutable state needed by the TUI application,
/// organized into logical groups for better maintainability. It serves as
/// the single source of truth for the application's current state.
///
/// The state is divided into several categories:
/// - UI state: Current path, filters, popups
/// - Progress state: Spinner animation, progress messages
/// - Log state: Error logging and display
/// - Engine state: Background engine status and progress
/// - Presentation state: File data and UI display state
/// - Table state: File table selection and scrolling
struct AppState {
	// === UI State ===
	/// Current directory path being scanned
	current_path: PathBuf,
	/// Active path filter with include/exclude patterns
	current_filter: uncp::PathFilter,
	/// Stack of active popup dialogs
	popup_stack: PopupStack,

	// === Progress and Display State ===
	/// Shared progress state for background tasks (thread-safe)
	progress_state: Arc<Mutex<Option<String>>>,
	/// Current spinner animation frame index
	spinner_idx: usize,
	/// Current progress message to display
	progress_line: Option<String>,

	// === Log State ===
	/// Minimum log level to display in log popup
	log_min_level: tracing::Level,
	/// Ring buffer of recent log events
	log_ring: std::collections::VecDeque<UiErrorEvent>,
	/// Deduplicated log entries with occurrence counts
	log_dedup: std::collections::VecDeque<(UiErrorEvent, u32)>,
	/// Current scroll position in log popup
	log_scroll: usize,

	// === Engine State ===
	/// Current file discovery progress information
	current_discovery_progress: Option<uncp::systems::SystemProgress>,
	/// Current file hashing progress information
	current_hashing_progress: Option<uncp::systems::SystemProgress>,
	/// Current engine status text
	engine_status: String,
	/// Processing speed in files per second
	processing_speed: Option<f64>,
	/// Timestamp of last progress update (for speed calculation)
	last_progress_update: std::time::Instant,
	/// Number of items processed at last update (for speed calculation)
	last_processed_count: usize,

	// === Presentation State ===
	/// Current file data and UI display state from engine
	pres: PresentationState,

	// === Table State ===
	/// Ratatui table widget state for selection and scrolling
	table_state: TableState,
	/// Currently selected row index
	selected_idx: usize,
}

impl AppState {
	fn new() -> Self {
		Self {
			current_path: PathBuf::from("."),
			current_filter: uncp::PathFilter::default(),
			popup_stack: PopupStack::default(),
			progress_state: Arc::new(Mutex::new(None)),
			spinner_idx: 0,
			progress_line: None,
			log_min_level: tracing::Level::ERROR,
			log_ring: std::collections::VecDeque::with_capacity(500),
			log_dedup: std::collections::VecDeque::with_capacity(500),
			log_scroll: 0,
			current_discovery_progress: None,
			current_hashing_progress: None,
			engine_status: "Starting...".to_string(),
			processing_speed: None,
			last_progress_update: std::time::Instant::now(),
			last_processed_count: 0,
			pres: PresentationState::default()
				.with_status("Press 's' to scan, 'h' to hash, 'r' to refresh, 'q' to quit"),
			table_state: TableState::default(),
			selected_idx: 0,
		}
	}
}

/// Initialize the background engine and return communication channels.
///
/// Sets up the duplicate detection engine, starts background cache loading,
/// and configures the initial scan path. Returns channels for receiving
/// engine events and sending commands.
///
/// # Arguments
/// * `app_state` - Mutable application state for initialization
///
/// # Returns
/// Tuple of (event_receiver, command_sender) for engine communication
fn initialize_engine(
	app_state: &mut AppState,
) -> std::io::Result<(
	smol::channel::Receiver<EngineEvent>,
	smol::channel::Sender<EngineCommand>,
)> {
	// Start background engine and subscribe to events
	let detector =
		uncp::DuplicateDetector::new(uncp::DetectorConfig::default()).expect("init detector");
	let (_engine, events, cmds) = BackgroundEngine::start(detector);

	// Load cache in background after TUI starts
	let cmds_clone = cmds.clone();
	smol::spawn(async move {
		if let Some(dir) = uncp::paths::default_cache_dir() {
			let _ = cmds_clone
				.send(uncp::engine::EngineCommand::LoadCache(dir))
				.await;
		}
	})
	.detach();

	// Set initial path and start
	cmds.try_send(EngineCommand::SetPath(app_state.current_path.clone()))
		.map_err(std::io::Error::other)?;
	cmds.try_send(EngineCommand::Start)
		.map_err(std::io::Error::other)?;

	Ok((events, cmds))
}

/// Main event loop that coordinates user input, engine events, and rendering.
///
/// This is the heart of the TUI application. It continuously:
/// 1. Processes user input events (keyboard, mouse)
/// 2. Updates progress state from background tasks
/// 3. Processes engine events (snapshots, progress updates)
/// 4. Updates UI state (logs, error messages)
/// 5. Renders the updated interface
///
/// The loop continues until the user quits the application.
///
/// # Arguments
/// * `terminal` - Ratatui terminal for rendering
/// * `ui_errs` - UI error queue handle
/// * `app_state` - Mutable application state
/// * `evt_rx` - Channel for receiving engine events
/// * `cmds` - Channel for sending commands to engine
///
/// # Returns
/// Result indicating success or IO error
fn main_event_loop(
	terminal: &mut ratatui::DefaultTerminal,
	ui_errs: &uncp::log_ui::UiErrorQueueHandle,
	app_state: &mut AppState,
	evt_rx: smol::channel::Receiver<EngineEvent>,
	cmds: smol::channel::Sender<EngineCommand>,
) -> std::io::Result<()> {
	let mut last_terminal_size = terminal.size()?;
	let mut resize_check_counter = 0;

	loop {
		let current_terminal_size = terminal.size()?;
		let terminal_area = Rect::new(
			0,
			0,
			current_terminal_size.width,
			current_terminal_size.height,
		);

		// Check for terminal size changes on every iteration (for terminals that don't send events)
		// Also do periodic forced redraws for terminals with poor resize event support
		resize_check_counter += 1;
		let force_size_check = resize_check_counter % 10 == 0; // Check every 10 iterations

		if current_terminal_size != last_terminal_size {
			info!(
				"Terminal size changed from {}x{} to {}x{} - forcing redraw",
				last_terminal_size.width,
				last_terminal_size.height,
				current_terminal_size.width,
				current_terminal_size.height
			);
			terminal.clear()?;
			last_terminal_size = current_terminal_size;
		} else if force_size_check {
			// Periodic check - just log if we would have missed a change
			trace!(
				"Periodic size check: {}x{}",
				current_terminal_size.width,
				current_terminal_size.height
			);
		}
		for action in handle_events(&mut app_state.popup_stack)? {
			if process_action(action, app_state, &cmds, terminal, terminal_area)? {
				return Ok(()); // Quit action
			}
		}

		// Update progress and re-render every loop
		update_progress_state(app_state);

		// Process engine events
		process_engine_events(app_state, &evt_rx);

		// Update UI state
		update_ui_state(app_state, ui_errs);

		// Render the UI (this will automatically use current terminal size)
		render_ui(terminal, app_state, ui_errs)?;
	}
}

/// Process a user action and return true if the application should quit
fn process_action(
	action: Action,
	app_state: &mut AppState,
	cmds: &smol::channel::Sender<EngineCommand>,
	terminal: &mut ratatui::DefaultTerminal,
	terminal_area: Rect,
) -> std::io::Result<bool> {
	debug!("Processing action: {:?}", action);

	match action {
		Action::Quit => {
			info!("Quitting...");
			return Ok(true);
		}
		Action::Refresh => {
			info!("Refreshing...");
			// Refresh just keeps current snapshot; engine will update
		}
		Action::ShowPathInput => {
			let buffer = app_state.current_path.to_string_lossy().to_string();
			app_state.popup_stack.push(PopupState::PathInput { buffer });
			app_state.pres = app_state
				.pres
				.clone()
				.with_status("Enter path, then press Enter (Esc to cancel)");
		}
		Action::ShowFilterInput => {
			let include_text = app_state.current_filter.include_patterns().join("\n");
			let exclude_text = app_state.current_filter.exclude_patterns().join("\n");
			app_state.popup_stack.push(PopupState::FilterInput {
				include_text,
				exclude_text,
				mode: FilterInputMode::Include,
			});
			app_state.pres = app_state.pres
				.clone()
				.with_status("Filter Manager: Tab to switch columns, type patterns (one per line), F10 to apply, Esc to cancel");
		}
		Action::ToggleLogView => {
			if app_state
				.popup_stack
				.stack
				.iter()
				.any(|state| matches!(state, PopupState::LogView))
			{
				app_state
					.popup_stack
					.stack
					.retain(|state| !matches!(state, PopupState::LogView));
			} else {
				app_state.popup_stack.push(PopupState::LogView);
			}
		}
		Action::ClosePopup => {
			if let Some(closed) = app_state.popup_stack.pop() {
				match closed {
					PopupState::PathInput { .. } => {
						app_state.pres = app_state.pres.clone().with_status("Path input canceled");
					}
					PopupState::FilterInput { .. } => {
						app_state.pres =
							app_state.pres.clone().with_status("Filter input canceled");
					}
					PopupState::LogView => {}
				}
			}
		}
		Action::FilterSwitchColumn => {
			if let Some(PopupState::FilterInput { mode, .. }) = app_state.popup_stack.top_mut() {
				*mode = match *mode {
					FilterInputMode::Include => FilterInputMode::Exclude,
					FilterInputMode::Exclude => FilterInputMode::Include,
				};
			}
		}
		Action::LogLevel(level) => {
			app_state.log_min_level = level;
			app_state.log_dedup.clear();
		}
		Action::LogScrollUp => {
			app_state.log_scroll = app_state.log_scroll.saturating_sub(1);
		}
		Action::LogScrollDown => {
			app_state.log_scroll = app_state.log_scroll.saturating_add(1);
		}
		Action::LogClear => {
			app_state.log_ring.clear();
			app_state.log_dedup.clear();
			app_state.log_scroll = 0;
		}
		Action::SubmitPath => {
			process_path_submission(app_state, cmds)?;
		}
		Action::SubmitFilter => {
			process_filter_submission(app_state, cmds)?;
		}
		Action::Scan => {
			process_scan_action(app_state, cmds)?;
		}
		Action::Hash => {
			cmds.try_send(EngineCommand::Start)
				.map_err(std::io::Error::other)?;
			app_state.pres = app_state.pres.clone().with_status("Hashing started...");
		}
		Action::Up => {
			app_state.selected_idx = app_state.selected_idx.saturating_sub(1);
			app_state.table_state.select(Some(app_state.selected_idx));
		}
		Action::Down => {
			let total = app_state.pres.file_table.len();
			if total > 0 && app_state.selected_idx + 1 < total {
				app_state.selected_idx += 1;
			}
			app_state.table_state.select(Some(app_state.selected_idx));
		}
		Action::PageUp => {
			let step = 10usize;
			app_state.selected_idx = app_state.selected_idx.saturating_sub(step);
			app_state.table_state.select(Some(app_state.selected_idx));
		}
		Action::PageDown => {
			let total = app_state.pres.file_table.len();
			let step = 10usize;
			if total > 0 {
				app_state.selected_idx =
					(app_state.selected_idx + step).min(total.saturating_sub(1));
			}
			app_state.table_state.select(Some(app_state.selected_idx));
		}
		Action::Home => {
			app_state.selected_idx = 0;
			app_state.table_state.select(Some(app_state.selected_idx));
		}
		Action::End => {
			let total = app_state.pres.file_table.len();
			if total > 0 {
				app_state.selected_idx = total - 1;
			}
			app_state.table_state.select(Some(app_state.selected_idx));
		}
		Action::SelectRow(mouse_row, mouse_col) => {
			process_mouse_selection(app_state, mouse_row, mouse_col, terminal_area);
		}
		Action::Resize(width, height) => {
			info!(
				"Processing resize action: {}x{} - clearing terminal for redraw",
				width, height
			);
			// Force a complete redraw by clearing the terminal
			// This ensures the UI properly adapts to the new dimensions
			terminal.clear()?;
		}
	}

	Ok(false)
}

/// Process path submission from the path input popup
fn process_path_submission(
	app_state: &mut AppState,
	cmds: &smol::channel::Sender<EngineCommand>,
) -> std::io::Result<()> {
	if let Some(PopupState::PathInput { buffer }) = app_state.popup_stack.pop() {
		let new_path_str = buffer.trim();
		info!("Submitting path: {}", new_path_str);

		// Expand tilde to home directory
		let expanded_path_str = if new_path_str.starts_with('~') {
			if let Some(home_dir) = dirs::home_dir() {
				if new_path_str == "~" {
					home_dir.to_string_lossy().to_string()
				} else if let Some(rest) = new_path_str.strip_prefix("~/") {
					home_dir.join(rest).to_string_lossy().to_string()
				} else {
					new_path_str.to_string()
				}
			} else {
				app_state.pres = app_state
					.pres
					.clone()
					.with_status("Error: Could not determine home directory".to_string());
				return Ok(());
			}
		} else {
			new_path_str.to_string()
		};

		// Validate the path exists
		let new_path = PathBuf::from(&expanded_path_str);
		if !new_path.exists() {
			app_state.pres = app_state.pres.clone().with_status(format!(
				"Error: Path '{}' does not exist",
				new_path.display()
			));
			return Ok(());
		}

		app_state.current_path = new_path;
		app_state.pres = app_state.pres.clone().with_status(format!(
			"Scanning path: {}",
			app_state.current_path.display()
		));

		// Restart scan with new path
		cmds.try_send(EngineCommand::Stop)
			.map_err(std::io::Error::other)?;
		cmds.try_send(EngineCommand::ClearState)
			.map_err(std::io::Error::other)?;
		std::thread::sleep(std::time::Duration::from_millis(200));
		cmds.try_send(EngineCommand::SetPath(app_state.current_path.clone()))
			.map_err(std::io::Error::other)?;

		// Apply current filter if any
		if !app_state.current_filter.include_patterns().is_empty()
			|| !app_state.current_filter.exclude_patterns().is_empty()
		{
			cmds.try_send(EngineCommand::SetPathFilter(app_state.current_filter.clone()))
				.map_err(std::io::Error::other)?;
		}

		cmds.try_send(EngineCommand::Start)
			.map_err(std::io::Error::other)?;
	}
	Ok(())
}

/// Process filter submission from the filter input popup
fn process_filter_submission(
	app_state: &mut AppState,
	cmds: &smol::channel::Sender<EngineCommand>,
) -> std::io::Result<()> {
	if let Some(PopupState::FilterInput {
		include_text,
		exclude_text,
		..
	}) = app_state.popup_stack.pop()
	{
		// Parse patterns from text buffers (one pattern per line)
		let include_patterns: Vec<String> = include_text
			.lines()
			.map(|line| line.trim().to_string())
			.filter(|line| !line.is_empty())
			.collect();

		let exclude_patterns: Vec<String> = exclude_text
			.lines()
			.map(|line| line.trim().to_string())
			.filter(|line| !line.is_empty())
			.collect();

		// Create new filter from the pattern lists
		match uncp::PathFilter::new(include_patterns.clone(), exclude_patterns.clone()) {
			Ok(new_filter) => {
				app_state.current_filter = new_filter.clone();

				let status_msg = if include_patterns.is_empty() && exclude_patterns.is_empty() {
					"Filter cleared".to_string()
				} else {
					format!(
						"Filter set: {} include, {} exclude patterns",
						include_patterns.len(),
						exclude_patterns.len()
					)
				};
				app_state.pres = app_state.pres.clone().with_status(status_msg);

				// Apply filter to engine
				if include_patterns.is_empty() && exclude_patterns.is_empty() {
					cmds.try_send(EngineCommand::ClearPathFilter)
						.map_err(std::io::Error::other)?;
				} else {
					cmds.try_send(EngineCommand::SetPathFilter(new_filter))
						.map_err(std::io::Error::other)?;
				}

				// Restart scan with new filter
				cmds.try_send(EngineCommand::Stop)
					.map_err(std::io::Error::other)?;
				cmds.try_send(EngineCommand::ClearState)
					.map_err(std::io::Error::other)?;
				cmds.try_send(EngineCommand::SetPath(app_state.current_path.clone()))
					.map_err(std::io::Error::other)?;
				cmds.try_send(EngineCommand::Start)
					.map_err(std::io::Error::other)?;
			}
			Err(e) => {
				app_state.pres = app_state
					.pres
					.clone()
					.with_status(format!("Invalid filter pattern: {}", e));
			}
		}
	}
	Ok(())
}

/// Process scan action
fn process_scan_action(
	app_state: &mut AppState,
	cmds: &smol::channel::Sender<EngineCommand>,
) -> std::io::Result<()> {
	let path = app_state.current_path.clone();
	info!("Scan requested for {}", path.display());

	if !Path::new(&path).exists() {
		app_state.pres = app_state
			.pres
			.clone()
			.with_status(format!("Path does not exist: {}", path.display()));
		return Ok(());
	}

	cmds.try_send(EngineCommand::SetPath(path))
		.map_err(std::io::Error::other)?;
	cmds.try_send(EngineCommand::Start)
		.map_err(std::io::Error::other)?;
	app_state.pres = app_state.pres.clone().with_status("Scanning...");
	Ok(())
}

/// Process mouse selection with dynamic layout calculation.
///
/// Calculates which table row was clicked based on the current terminal size
/// and dynamic layout configuration, rather than using hard-coded values.
fn process_mouse_selection(
	app_state: &mut AppState,
	mouse_row: u16,
	mouse_col: u16,
	terminal_area: Rect,
) {
	debug!("Mouse click at ({}, {})", mouse_row, mouse_col);

	// Calculate layout dynamically based on actual terminal size
	let layout = LayoutConfig::new(terminal_area);
	let table_data_start = layout.table_data_start_row();

	if mouse_row >= table_data_start {
		let clicked_row = (mouse_row - table_data_start) as usize;
		let total = app_state.pres.file_table.len();

		if total > 0 && clicked_row < total {
			app_state.selected_idx = clicked_row;
			app_state.table_state.select(Some(app_state.selected_idx));
			app_state.pres = app_state.pres.clone().with_status(format!(
				"Selected row {} via mouse",
				app_state.selected_idx + 1
			));
		}
	}
}

/// Update progress state from the progress state mutex
fn update_progress_state(app_state: &mut AppState) {
	if let Ok(guard) = app_state.progress_state.lock() {
		app_state.progress_line = guard.clone();
	}
}

/// Process engine events and update application state
fn process_engine_events(app_state: &mut AppState, evt_rx: &smol::channel::Receiver<EngineEvent>) {
	while let Ok(evt) = evt_rx.try_recv() {
		match evt {
			EngineEvent::SnapshotReady(snap) => {
				debug!(
					"TUI: Received snapshot with {} files, {} pending hash",
					snap.total_files, snap.pending_hash
				);
				app_state.pres = snap;
				// Clamp selection within bounds when data changes
				let total = app_state.pres.file_table.len();
				if total == 0 {
					app_state.selected_idx = 0;
					app_state.table_state.select(None);
				} else {
					if app_state.selected_idx >= total {
						app_state.selected_idx = total - 1;
					}
					app_state.table_state.select(Some(app_state.selected_idx));
				}
			}
			EngineEvent::Started => {
				info!("TUI: Engine started");
				app_state.engine_status = "Ready".to_string();
				app_state.pres = app_state.pres.clone().with_status("Scan engine ready");
			}
			EngineEvent::Stopped => {
				info!("TUI: Engine stopped");
				app_state.engine_status = "Stopped".to_string();
				app_state.pres = app_state.pres.clone().with_status("Scan engine stopped");
			}
			EngineEvent::Completed => {
				info!("TUI: Engine completed");
				app_state.engine_status = "Completed".to_string();
				app_state.pres = app_state.pres.clone().with_status("Scan completed");
			}
			EngineEvent::CacheLoading => {
				debug!("TUI: Cache loading");
				app_state.engine_status = "Loading cache".to_string();
				app_state.pres = app_state
					.pres
					.clone()
					.with_status("Loading cache from disk");
			}
			EngineEvent::CacheLoaded => {
				debug!("TUI: Cache loaded");
				app_state.engine_status = "Ready".to_string();
				app_state.pres = app_state
					.pres
					.clone()
					.with_status("Cache loaded successfully");
			}
			EngineEvent::CacheSaving => {
				debug!("TUI: Cache saving");
				app_state.engine_status = "Saving cache".to_string();
				app_state.pres = app_state.pres.clone().with_status("Saving cache to disk");
			}
			EngineEvent::CacheSaved => {
				debug!("TUI: Cache saved");
				app_state.pres = app_state
					.pres
					.clone()
					.with_status("Cache saved successfully");
			}
			EngineEvent::CacheValidating => {
				debug!("TUI: Cache validating");
				app_state.engine_status = "Validating cache".to_string();
				app_state.pres = app_state
					.pres
					.clone()
					.with_status("Validating cached files against filesystem");
			}
			EngineEvent::CacheValidated {
				files_removed,
				files_invalidated,
			} => {
				debug!(
					"TUI: Cache validated - {} removed, {} invalidated",
					files_removed, files_invalidated
				);
				app_state.engine_status = "Ready".to_string();
				let status_msg = if files_removed > 0 || files_invalidated > 0 {
					format!(
						"Cache validated: {} files removed, {} files marked for re-processing",
						files_removed, files_invalidated
					)
				} else {
					"Cache validated: all files up to date".to_string()
				};
				app_state.pres = app_state.pres.clone().with_status(status_msg);
			}
			EngineEvent::DiscoveryProgress(progress) => {
				update_discovery_progress(app_state, progress);
			}
			EngineEvent::HashingProgress(progress) => {
				update_hashing_progress(app_state, progress);
			}
			EngineEvent::Error(err) => {
				debug!("TUI: Engine error: {}", err);
				app_state.engine_status = format!("Error: {}", err);
			}
		}
	}
}

/// Update discovery progress and calculate processing speed
fn update_discovery_progress(app_state: &mut AppState, progress: uncp::systems::SystemProgress) {
	debug!(
		"TUI: Discovery progress: {}/{} - {:?}",
		progress.processed_items, progress.total_items, progress.current_item
	);

	// Calculate processing speed
	let now = std::time::Instant::now();
	if now.duration_since(app_state.last_progress_update).as_secs() >= 1 {
		let items_processed = progress
			.processed_items
			.saturating_sub(app_state.last_processed_count);
		let elapsed = now
			.duration_since(app_state.last_progress_update)
			.as_secs_f64();
		if elapsed > 0.0 {
			app_state.processing_speed = Some(items_processed as f64 / elapsed);
		}
		app_state.last_progress_update = now;
		app_state.last_processed_count = progress.processed_items;
	}

	app_state.current_discovery_progress = Some(progress);
	app_state.engine_status = "Discovering files".to_string();
}

/// Update hashing progress and calculate processing speed
fn update_hashing_progress(app_state: &mut AppState, progress: uncp::systems::SystemProgress) {
	debug!(
		"TUI: Hashing progress: {}/{} - {:?}",
		progress.processed_items, progress.total_items, progress.current_item
	);

	// Calculate processing speed for hashing
	let now = std::time::Instant::now();
	if now.duration_since(app_state.last_progress_update).as_secs() >= 1 {
		let items_processed = progress
			.processed_items
			.saturating_sub(app_state.last_processed_count);
		let elapsed = now
			.duration_since(app_state.last_progress_update)
			.as_secs_f64();
		if elapsed > 0.0 {
			app_state.processing_speed = Some(items_processed as f64 / elapsed);
		}
		app_state.last_progress_update = now;
		app_state.last_processed_count = progress.processed_items;
	}

	app_state.current_hashing_progress = Some(progress);
	app_state.engine_status = "Hashing files".to_string();
}

/// Update UI state including progress display and log processing
fn update_ui_state(app_state: &mut AppState, ui_errs: &uncp::log_ui::UiErrorQueueHandle) {
	// Handle progress completion
	if app_state.progress_line.as_deref() == Some("Scan complete") {
		app_state.pres = app_state.pres.clone().with_status("Scan complete");
		app_state.progress_line = None;
	}

	// Process UI error logs
	if let Some(err) = ui_errs.last() {
		if app_state.log_ring.back().map(|e| e.ts_unix_ms) != Some(err.ts_unix_ms) {
			if app_state.log_ring.len() >= 500 {
				let _ = app_state.log_ring.pop_front();
			}
			app_state.log_ring.push_back(err.clone());

			// Rebuild dedup: combine adjacent equal (level+target+message)
			app_state.log_dedup.clear();
			for ev in app_state.log_ring.iter().rev() {
				if !level_at_least(ev.level, app_state.log_min_level) {
					continue;
				}
				if let Some((last, count)) = app_state.log_dedup.front_mut() {
					if last.level == ev.level
						&& last.target == ev.target
						&& last.message == ev.message
					{
						*count += 1;
						continue;
					}
				}
				app_state.log_dedup.push_front((ev.clone(), 1));
			}
		}
	}
}

/// Render the UI
fn render_ui(
	terminal: &mut ratatui::DefaultTerminal,
	app_state: &mut AppState,
	ui_errs: &uncp::log_ui::UiErrorQueueHandle,
) -> std::io::Result<()> {
	// Update spinner animation
	if app_state.progress_line.is_some() {
		app_state.spinner_idx = (app_state.spinner_idx + 1) % SPINNER.len();
	}

	terminal.draw(|f| {
		let params = DrawParams {
			pres: &app_state.pres,
			popup_stack: &app_state.popup_stack,
			current_filter: &app_state.current_filter,
			engine_status: &app_state.engine_status,
			discovery_progress: &app_state.current_discovery_progress,
			hashing_progress: &app_state.current_hashing_progress,
			processing_speed: app_state.processing_speed,
			table_state: &mut app_state.table_state,
			ui_errs,
			log_dedup: &app_state.log_dedup,
			log_scroll: app_state.log_scroll,
		};
		draw(f, params);
	})?;

	Ok(())
}

#[derive(Debug, Clone, Copy)]
enum FilterInputMode {
	Include,
	Exclude,
}

#[derive(Debug, Clone)]
enum PopupState {
	PathInput {
		buffer: String,
	},
	FilterInput {
		include_text: String,
		exclude_text: String,
		mode: FilterInputMode,
	},
	LogView,
}

#[derive(Debug, Default)]
struct PopupStack {
	stack: Vec<PopupState>,
}

impl PopupStack {
	fn push(&mut self, state: PopupState) {
		self.stack.push(state);
	}

	fn pop(&mut self) -> Option<PopupState> {
		self.stack.pop()
	}

	fn top(&self) -> Option<&PopupState> {
		self.stack.last()
	}

	fn top_mut(&mut self) -> Option<&mut PopupState> {
		self.stack.last_mut()
	}

	fn is_empty(&self) -> bool {
		self.stack.is_empty()
	}
}

/// Optimized table builder that only processes visible rows to avoid performance issues
/// with large file lists. Only converts paths to strings for rows that will actually be displayed.
/// Uses dynamic column sizing based on terminal dimensions.
fn build_optimized_file_table<'a>(
	file_table: &[(uncp::paths::DirEntryId, u64, String, bool)],
	current_path_filter: &str,
	table_area: Rect,
	title: &str,
	table_state: &TableState,
	layout: &LayoutConfig,
) -> ratatui::widgets::Table<'a> {
	use ratatui::layout::Constraint;
	use ratatui::style::{Modifier, Style};
	use ratatui::widgets::{Cell, Row, Table};
	use std::path::PathBuf;

	// Calculate visible row range based on table area and scroll position
	let table_height = table_area.height.saturating_sub(3) as usize; // subtract borders + header
	let total_rows = file_table.len();

	// Get scroll offset from table state
	let scroll_offset = table_state.offset();
	let visible_start = scroll_offset;
	let visible_end = (scroll_offset + table_height).min(total_rows);

	// Resolve scan base path once for relative path calculation
	let scan_base: PathBuf = {
		let p = std::path::Path::new(current_path_filter);
		if p.is_absolute() {
			p.to_path_buf()
		} else {
			std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
		}
	};

	// Calculate path column width for ellipsis based on dynamic layout
	let inner_width = table_area.width.saturating_sub(2) as usize;
	let column_constraints = layout.table_column_constraints();

	// Calculate actual path width based on the first constraint (Path column)
	let path_width = if let Some(Constraint::Percentage(pct)) = column_constraints.first() {
		inner_width * (*pct as usize) / 100
	} else {
		inner_width * 60 / 100 // fallback
	};

	// Header row
	let header = Row::new(vec![
		Cell::from("Path"),
		Cell::from("Size"),
		Cell::from("Type"),
		Cell::from("Hashed"),
	])
	.style(Style::default().add_modifier(Modifier::BOLD));

	// Only process visible rows
	let mut rows = Vec::with_capacity(visible_end - visible_start);
	for (idx, (path_id, size, file_type, hashed)) in file_table.iter().enumerate() {
		// Convert path to string only for visible rows
		let (path_str, size_str) = if idx >= visible_start && idx < visible_end {
			let abs = path_id.resolve();
			let display_path = if let Ok(rel) = abs.strip_prefix(&scan_base) {
				rel.display().to_string()
			} else {
				abs.display().to_string()
			};

			// Apply tail ellipsis if path is too long
			let path_str = if path_width > 0 && display_path.chars().count() > path_width {
				let take = path_width.saturating_sub(2);
				let tail: String = display_path
					.chars()
					.rev()
					.take(take)
					.collect::<String>()
					.chars()
					.rev()
					.collect();
				format!(" …{}", tail)
			} else {
				display_path
			};
			(path_str, format_bytes(*size))
		} else {
			// Actually add rows to table, but don't bother allocating invisible displayed paths.
			// Empty strings don't allocate any memory.
			(String::new(), String::new())
		};

		// Format hashed status
		let hashed_str = if *hashed { "✓" } else { "○" };

		let row = Row::new(vec![
			Cell::from(path_str),
			Cell::from(size_str),
			Cell::from(file_type.clone()),
			Cell::from(hashed_str),
		]);

		rows.push(row);
	}

	// Use dynamic column constraints from layout
	let constraints = layout.table_column_constraints();

	Table::new(rows, constraints)
		.header(header)
		.row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
		.block(
			Block::default()
				.borders(Borders::ALL)
				.title(title.to_string()),
		)
}

/// Format bytes as human-readable string (moved from df_render.rs)
fn format_bytes(size: u64) -> String {
	const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
	let mut size_f = size as f64;
	let mut unit_index = 0;
	while size_f >= 1024.0 && unit_index < UNITS.len() - 1 {
		size_f /= 1024.0;
		unit_index += 1;
	}
	if unit_index == 0 {
		format!("{} {}", size, UNITS[unit_index])
	} else {
		format!("{:.1} {}", size_f, UNITS[unit_index])
	}
}

#[derive(Debug, Clone, Copy)]
enum Action {
	Quit,
	Refresh,
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
	SelectRow(u16, u16), // row, column
	// Terminal events
	Resize(u16, u16), // width, height
	// Log-specific actions
	LogLevel(tracing::Level),
	LogScrollUp,
	LogScrollDown,
	LogClear,
}

/// Parameters for the main draw function to avoid too many arguments.
///
/// This struct groups all the data needed to render the TUI interface,
/// making the function signature cleaner and more maintainable.
struct DrawParams<'a> {
	/// Current presentation state containing file data, totals, and UI state
	pres: &'a PresentationState,

	/// Stack of active popups (path input, filter input, log view, etc.)
	popup_stack: &'a PopupStack,

	/// Current path filter configuration for include/exclude patterns
	current_filter: &'a uncp::PathFilter,

	/// Current engine status text (e.g., "Ready", "Scanning", "Completed")
	engine_status: &'a str,

	/// Real-time progress information for file discovery phase
	discovery_progress: &'a Option<uncp::systems::SystemProgress>,

	/// Real-time progress information for file hashing phase
	hashing_progress: &'a Option<uncp::systems::SystemProgress>,

	/// Current processing speed in files per second (for display)
	processing_speed: Option<f64>,

	/// Mutable reference to table selection/scroll state
	table_state: &'a mut TableState,

	/// Handle to UI error queue for displaying recent errors
	ui_errs: &'a uncp::log_ui::UiErrorQueueHandle,

	/// Deduplicated log entries with occurrence counts for log popup
	log_dedup: &'a std::collections::VecDeque<(UiErrorEvent, u32)>,

	/// Current scroll position in the log popup (lines from top)
	log_scroll: usize,
}

/// Dynamic layout configuration for responsive UI design.
///
/// This struct replaces hard-coded panel sizes with a flexible system that
/// calculates appropriate dimensions based on terminal size. It ensures the
/// interface remains usable across different screen sizes while maintaining
/// optimal proportions for each UI component.
///
/// ## Features
///
/// - **Adaptive panel heights**: Scales header, footer, and body based on terminal height
/// - **Responsive column widths**: Adjusts table columns for narrow, medium, and wide terminals
/// - **Dynamic popup sizing**: Calculates popup dimensions with min/max bounds
/// - **Mouse coordinate mapping**: Provides accurate click-to-row mapping for any terminal size
///
/// ## Design Philosophy
///
/// The layout system prioritizes usability over fixed aesthetics:
/// - Minimum sizes ensure functionality on small terminals
/// - Proportional scaling maintains visual balance on larger screens
/// - Content-aware sizing gives more space to important information (file paths)
#[derive(Debug, Clone)]
struct LayoutConfig {
	/// Height of the header panel
	header_height: u16,
	/// Minimum height for the file table body
	body_min_height: u16,
	/// Height of the status footer panel
	footer_height: u16,
	/// Height of the keybinding hints panel
	hints_height: u16,
	/// Total terminal area
	terminal_area: Rect,
}

impl LayoutConfig {
	/// Create a new layout configuration based on terminal size.
	///
	/// Calculates appropriate panel sizes that scale with terminal dimensions
	/// while maintaining minimum usable sizes for each section.
	///
	/// # Arguments
	/// * `terminal_area` - The full terminal area available for rendering
	///
	/// # Returns
	/// Layout configuration with calculated panel sizes
	fn new(terminal_area: Rect) -> Self {
		let height = terminal_area.height;

		// Calculate dynamic panel heights based on terminal size
		let header_height = if height < 20 { 3 } else { 4 };
		let footer_height = if height < 15 {
			4
		} else if height < 25 {
			6
		} else {
			7
		};
		let hints_height = 1; // Always 1 line for hints

		// Ensure minimum body height
		let reserved_height = header_height + footer_height + hints_height;
		let body_min_height = if height > reserved_height + 5 {
			5
		} else {
			height.saturating_sub(reserved_height).max(3)
		};

		Self {
			header_height,
			body_min_height,
			footer_height,
			hints_height,
			terminal_area,
		}
	}

	/// Get the layout constraints for the main vertical layout.
	///
	/// # Returns
	/// Vector of constraints for header, body, footer, and hints panels
	fn main_constraints(&self) -> Vec<Constraint> {
		vec![
			Constraint::Length(self.header_height),
			Constraint::Min(self.body_min_height),
			Constraint::Length(self.footer_height),
			Constraint::Length(self.hints_height),
		]
	}

	/// Calculate the starting row for table data based on layout.
	///
	/// This is used for mouse click handling to determine which table row
	/// was clicked based on screen coordinates.
	///
	/// # Returns
	/// Row number where table data starts (after header and table header)
	fn table_data_start_row(&self) -> u16 {
		self.header_height + 2 // header panel + table border + table header
	}

	/// Get adaptive column constraints for the file table.
	///
	/// Adjusts column widths based on terminal width to ensure optimal
	/// display of file information across different screen sizes.
	///
	/// # Returns
	/// Vector of constraints for Path, Size, Type, and Hashed columns
	fn table_column_constraints(&self) -> Vec<Constraint> {
		let width = self.terminal_area.width;

		if width < 80 {
			// Narrow terminal - prioritize path, compress other columns
			vec![
				Constraint::Percentage(70), // Path
				Constraint::Percentage(15), // Size
				Constraint::Percentage(10), // Type
				Constraint::Percentage(5),  // Hashed
			]
		} else if width < 120 {
			// Medium terminal - balanced layout
			vec![
				Constraint::Percentage(60), // Path
				Constraint::Percentage(15), // Size
				Constraint::Percentage(15), // Type
				Constraint::Percentage(10), // Hashed
			]
		} else {
			// Wide terminal - more space for path and details
			vec![
				Constraint::Percentage(55), // Path
				Constraint::Percentage(20), // Size
				Constraint::Percentage(15), // Type
				Constraint::Percentage(10), // Hashed
			]
		}
	}
}

/// Main UI rendering function that coordinates drawing all interface components.
///
/// This function creates the main layout and delegates rendering to specialized
/// functions for each UI section. The layout consists of:
/// - Header: Summary information (total files, pending hashes, filters)
/// - Body: File table with optimized rendering for large datasets
/// - Footer: Status information (scan progress, errors, engine state)
/// - Hints: Keybinding help text
/// - Popups: Modal dialogs overlaid on top of main interface
///
/// # Arguments
/// * `frame` - Ratatui frame for rendering widgets
/// * `params` - All data needed for rendering, grouped to avoid too many parameters
fn draw(frame: &mut Frame, params: DrawParams) {
	// Destructure params for cleaner code
	let DrawParams {
		pres,
		popup_stack,
		current_filter,
		engine_status,
		discovery_progress,
		hashing_progress,
		processing_speed,
		table_state,
		ui_errs,
		log_dedup,
		log_scroll,
	} = params;

	// Calculate dynamic layout based on terminal size
	let layout = LayoutConfig::new(frame.area());
	let chunks = Layout::default()
		.direction(Direction::Vertical)
		.constraints(layout.main_constraints())
		.split(frame.area());

	// Render each section
	render_header(frame, pres, current_filter, chunks[0]);
	render_file_table(frame, pres, table_state, chunks[1], &layout);
	let status_params = StatusFooterParams {
		pres,
		engine_status,
		discovery_progress,
		hashing_progress,
		processing_speed,
		ui_errs,
	};
	render_status_footer(frame, status_params, chunks[2]);
	render_keybinding_hints(frame, popup_stack, chunks[3]);
	render_popups(frame, popup_stack, log_dedup, log_scroll);
}

/// Render the header section with summary information.
///
/// Displays the application title, total file count, pending hash count,
/// and active filter information in a bordered box at the top of the interface.
///
/// # Arguments
/// * `frame` - Ratatui frame for rendering
/// * `pres` - Presentation state containing file counts
/// * `current_filter` - Active path filter configuration
/// * `area` - Screen area to render into
fn render_header(
	frame: &mut Frame,
	pres: &PresentationState,
	current_filter: &uncp::PathFilter,
	area: Rect,
) {
	let mut header_spans = vec![
		Span::styled("uncp TUI", Style::default().fg(Color::Cyan)),
		Span::raw("  |  Total: "),
		Span::raw(pres.total_files.to_string()),
		Span::raw("  Pending: "),
		Span::raw(pres.pending_hash.to_string()),
	];

	// Add filter information if any filters are active
	if current_filter.has_patterns() {
		header_spans.push(Span::raw("  |  Filters: "));
		header_spans.push(Span::styled(
			format!(
				"{}inc, {}exc",
				current_filter.include_patterns().len(),
				current_filter.exclude_patterns().len()
			),
			Style::default().fg(Color::Yellow),
		));
	}

	let header = Paragraph::new(Line::from(header_spans))
		.block(Block::default().borders(Borders::ALL).title("Summary"));
	frame.render_widget(header, area);
}

/// Render the file table section with optimized performance.
///
/// Creates and displays the main file table using an optimized approach that
/// only processes visible rows to maintain performance with large datasets.
/// The table shows file paths, sizes, types, and hash status with adaptive
/// column widths based on terminal size.
///
/// # Arguments
/// * `frame` - Ratatui frame for rendering
/// * `pres` - Presentation state containing file data
/// * `table_state` - Mutable table state for selection and scrolling
/// * `area` - Screen area to render into
/// * `layout` - Layout configuration for adaptive sizing
fn render_file_table(
	frame: &mut Frame,
	pres: &PresentationState,
	table_state: &mut TableState,
	area: Rect,
	layout: &LayoutConfig,
) {
	let title = if pres.current_path_filter.is_empty() {
		"Files (sorted by size)".to_string()
	} else {
		format!("Files in '{}' (sorted by size)", pres.current_path_filter)
	};

	// Render optimized table directly without DataFrame overhead
	let table = build_optimized_file_table(
		&pres.file_table,
		&pres.current_path_filter,
		area,
		&title,
		table_state,
		layout,
	);
	frame.render_stateful_widget(table, area, table_state);
}

/// Render the status footer section
/// Parameters for rendering the status footer section.
///
/// Groups all the data needed to display status information including
/// scan progress, engine state, error messages, and processing statistics.
struct StatusFooterParams<'a> {
	/// Current presentation state for accessing file counts and error info
	pres: &'a PresentationState,

	/// Current engine status text (Ready, Scanning, Completed, etc.)
	engine_status: &'a str,

	/// Real-time file discovery progress information
	discovery_progress: &'a Option<uncp::systems::SystemProgress>,

	/// Real-time file hashing progress information
	hashing_progress: &'a Option<uncp::systems::SystemProgress>,

	/// Current processing speed in files per second
	processing_speed: Option<f64>,

	/// Handle to UI error queue for displaying recent errors
	ui_errs: &'a uncp::log_ui::UiErrorQueueHandle,
}

/// Render the status footer section with detailed progress information.
///
/// Displays comprehensive status information including:
/// - Scan engine status with processing speed
/// - File discovery progress with current file being processed
/// - File hashing progress with completion percentage
/// - Recent error messages from the UI error queue
///
/// # Arguments
/// * `frame` - Ratatui frame for rendering
/// * `params` - Status footer parameters containing all needed data
/// * `area` - Screen area to render into
fn render_status_footer(frame: &mut Frame, params: StatusFooterParams, area: Rect) {
	// Destructure params for cleaner code
	let StatusFooterParams {
		pres,
		engine_status,
		discovery_progress,
		hashing_progress,
		processing_speed,
		ui_errs,
	} = params;

	let mut status_lines = Vec::new();

	// Scan status line
	let mut scan_line = format!("Scan status: {}", engine_status);
	if let Some(speed) = processing_speed {
		scan_line.push_str(&format!(" ({:.1} files/sec)", speed));
	}
	status_lines.push(Line::from(Span::raw(scan_line)));

	// Discovery status
	status_lines.push(Line::from(Span::raw(build_discovery_status_line(
		pres,
		discovery_progress,
	))));

	// Hashing status
	status_lines.push(Line::from(Span::raw(build_hashing_status_line(
		pres,
		hashing_progress,
	))));

	// Error messages
	add_error_lines(&mut status_lines, ui_errs);

	let footer =
		Paragraph::new(status_lines).block(Block::default().borders(Borders::ALL).title("Status"));
	frame.render_widget(footer, area);
}

/// Build the discovery status line showing file discovery progress.
///
/// Creates a status line that shows either:
/// - Real-time discovery progress with current file being processed
/// - Completion status when discovery is finished
/// - Initial state when no files have been discovered yet
///
/// # Arguments
/// * `pres` - Presentation state containing total file counts
/// * `discovery_progress` - Optional real-time discovery progress data
///
/// # Returns
/// Formatted status string for display
fn build_discovery_status_line(
	pres: &PresentationState,
	discovery_progress: &Option<uncp::systems::SystemProgress>,
) -> String {
	let total_files = pres.total_files;
	let discovered_files = total_files;

	if let Some(ref disc) = discovery_progress {
		// Use progress event data if available - show current file being discovered
		let mut line = format!("Discovery: {} files", disc.processed_items);
		if let Some(ref current) = disc.current_item {
			// Truncate long paths for display
			let display_path = if current.len() > 60 {
				format!("...{}", &current[current.len() - 57..])
			} else {
				current.clone()
			};
			line.push_str(&format!(" - {}", display_path));
		}
		line
	} else {
		// Show current detector state when no progress events
		if discovered_files == 0 {
			"Discovery: 0 files - No files discovered yet".to_string()
		} else {
			format!(
				"Discovery: {} files - Discovery completed",
				discovered_files
			)
		}
	}
}

/// Build the hashing status line showing file hashing progress.
///
/// Creates a status line that shows either:
/// - Real-time hashing progress with completion percentage and current file
/// - Completion status when all files are hashed
/// - Initial state when no files need hashing
///
/// # Arguments
/// * `pres` - Presentation state containing file counts
/// * `hashing_progress` - Optional real-time hashing progress data
///
/// # Returns
/// Formatted status string for display
fn build_hashing_status_line(
	pres: &PresentationState,
	hashing_progress: &Option<uncp::systems::SystemProgress>,
) -> String {
	let total_files = pres.total_files;
	let pending_files = pres.pending_hash;

	if let Some(ref hash) = hashing_progress {
		// Use progress event data if available
		let percentage = if hash.total_items > 0 {
			(hash.processed_items as f64 / hash.total_items as f64 * 100.0) as u32
		} else {
			0
		};
		let mut line = format!(
			"Hashing: {}/{} ({}%)",
			hash.processed_items, hash.total_items, percentage
		);
		if let Some(ref current) = hash.current_item {
			// Truncate long paths for display
			let display_path = if current.len() > 50 {
				format!("...{}", &current[current.len() - 47..])
			} else {
				current.clone()
			};
			line.push_str(&format!(" - {}", display_path));
		}
		line
	} else {
		// Show current detector state when no progress events
		if total_files == 0 {
			"Hashing: 0/0 (0%) - No files to hash".to_string()
		} else if pending_files == 0 {
			format!(
				"Hashing: {}/{} (100%) - All files hashed",
				total_files, total_files
			)
		} else {
			let hashed_files = total_files - pending_files;
			let percentage = if total_files > 0 {
				(hashed_files as f64 / total_files as f64 * 100.0) as u32
			} else {
				0
			};
			format!(
				"Hashing: {}/{} ({}%) - {} files pending",
				hashed_files, total_files, percentage, pending_files
			)
		}
	}
}

/// Add error message lines to the status display.
///
/// Checks for recent errors from both the UI error queue and engine snapshots,
/// and adds them to the status lines with red styling for visibility.
///
/// # Arguments
/// * `status_lines` - Mutable vector to append error lines to
/// * `pres` - Presentation state containing engine snapshot errors
/// * `ui_errs` - UI error queue handle for recent tracing errors
fn add_error_lines(
	status_lines: &mut Vec<Line>,
	ui_errs: &uncp::log_ui::UiErrorQueueHandle,
) {
	// UI subscriber errors
	if let Some(err) = ui_errs.last() {
		let msg = format!("{} - {}: {}", err.level, err.target, err.message);
		let err_line = Line::from(Span::styled(
			format!("Last error: {}", msg),
			Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
		));
		status_lines.push(err_line);
	}
}

/// Render the keybinding hints section
fn render_keybinding_hints(frame: &mut Frame, popup_stack: &PopupStack, area: Rect) {
	let mut hint_spans = build_base_keybinding_hints();
	add_popup_specific_hints(&mut hint_spans, popup_stack);

	let keybinding_hints = Paragraph::new(Line::from(hint_spans))
		.style(Style::default().bg(Color::Black).fg(Color::White));
	frame.render_widget(keybinding_hints, area);
}

/// Build the base keybinding hints that are always shown
fn build_base_keybinding_hints() -> Vec<Span<'static>> {
	vec![
		Span::styled(
			"Keys: ",
			Style::default()
				.fg(Color::Yellow)
				.add_modifier(Modifier::BOLD),
		),
		// App control
		Span::styled(
			"q",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" quit  "),
		Span::styled(
			"r",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" refresh  "),
		Span::styled(
			"s",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" scan  "),
		Span::styled(
			"h",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" hash  "),
		Span::styled(
			"p",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" path  "),
		Span::styled(
			"f",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" filter  "),
		// Table navigation
		Span::styled(
			"↑/↓",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" move  "),
		Span::styled(
			"PgUp/PgDn",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" page  "),
		Span::styled(
			"Home",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" top  "),
		Span::styled(
			"End",
			Style::default()
				.fg(Color::Green)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" bottom  "),
		// Mouse support
		Span::styled(
			"🖱️",
			Style::default()
				.fg(Color::Cyan)
				.add_modifier(Modifier::BOLD),
		),
		Span::raw(" click to select"),
	]
}

/// Add popup-specific keybinding hints
fn add_popup_specific_hints(hint_spans: &mut Vec<Span<'static>>, popup_stack: &PopupStack) {
	match popup_stack.top() {
		Some(PopupState::PathInput { .. }) => {
			hint_spans.extend_from_slice(&[
				Span::raw("  "),
				Span::styled(
					"Enter",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" confirm  "),
				Span::styled(
					"Esc",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" cancel"),
			]);
		}
		Some(PopupState::FilterInput { .. }) => {
			hint_spans.extend_from_slice(&[
				Span::raw("  "),
				Span::styled(
					"F10",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" apply  "),
				Span::styled(
					"Tab",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" switch  "),
				Span::styled(
					"Esc",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" cancel"),
			]);
		}
		Some(PopupState::LogView) => {
			hint_spans.extend_from_slice(&[
				Span::raw("  "),
				Span::styled(
					"L",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" close  "),
				Span::styled(
					"j/k",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" scroll  "),
				Span::styled(
					"1-5",
					Style::default()
						.fg(Color::Green)
						.add_modifier(Modifier::BOLD),
				),
				Span::raw(" level"),
			]);
		}
		None => {}
	}
}

/// Render popups based on the popup stack
fn render_popups(
	frame: &mut Frame,
	popup_stack: &PopupStack,
	log_dedup: &std::collections::VecDeque<(UiErrorEvent, u32)>,
	log_scroll: usize,
) {
	debug!(
		"Rendering with popup stack size: {}",
		popup_stack.stack.len()
	);
	match popup_stack.top() {
		Some(PopupState::FilterInput {
			include_text,
			exclude_text,
			mode,
		}) => {
			debug!("Rendering FilterInput popup");
			render_filter_dialog(frame, include_text, exclude_text, mode);
		}
		Some(PopupState::PathInput { buffer }) => {
			debug!("Rendering PathInput popup");
			render_path_input_popup(frame, buffer);
		}
		Some(PopupState::LogView) => {
			debug!("Rendering LogView popup");
			render_log_view_popup(frame, log_dedup, log_scroll);
		}
		None => {
			debug!("No popup to render");
		}
	}
}

/// Render the path input popup
fn render_path_input_popup(frame: &mut Frame, buffer: &str) {
	let popup_rect = dynamic_popup_area(frame.area(), PopupType::PathInput);
	frame.render_widget(Clear, popup_rect);
	let input_display = format!("{}_", buffer);
	let input_widget = Paragraph::new(input_display).block(
		Block::default()
			.borders(Borders::ALL)
			.title("Set Path")
			.style(Style::default().fg(Color::Yellow)),
	);
	frame.render_widget(input_widget, popup_rect);
}

/// Render the log view popup
fn render_log_view_popup(
	frame: &mut Frame,
	log_dedup: &std::collections::VecDeque<(UiErrorEvent, u32)>,
	log_scroll: usize,
) {
	let area = dynamic_popup_area(frame.area(), PopupType::LogView);
	frame.render_widget(Clear, area);
	let block = Block::default()
		.borders(Borders::ALL)
		.title("Logs (L: toggle log view, Ctrl-L: clear log, 1-5: level, j/k: scroll)");
	let inner = block.inner(area);
	frame.render_widget(block, area);

	let max_rows = inner.height.saturating_sub(2) as usize;
	let total_rows = log_dedup.len();
	let start = total_rows.saturating_sub(max_rows + log_scroll);
	let end = total_rows.saturating_sub(log_scroll);
	let rows_iter = log_dedup.iter().skip(start).take(end - start);

	let mut lines = Vec::with_capacity(max_rows);
	for (ev, count) in rows_iter.rev() {
		let level_style = get_log_level_style(ev.level);
		let ts = ev.ts_unix_ms;
		let msg = format!(
			"[{}] {} {} — {}{}",
			get_log_level_string(ev.level),
			ev.target,
			ts,
			ev.message,
			if *count > 1 {
				format!(" (x{})", count)
			} else {
				String::new()
			}
		);
		lines.push(Line::from(Span::styled(msg, level_style)));
	}

	// Show message when empty
	if lines.is_empty() {
		lines.push(Line::from("No log entries"));
	}

	let para = Paragraph::new(lines).wrap(Wrap { trim: true });
	frame.render_widget(para, inner);
}

/// Get the style for a log level
fn get_log_level_style(level: tracing::Level) -> Style {
	match level {
		tracing::Level::ERROR => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
		tracing::Level::WARN => Style::default().fg(Color::Yellow),
		tracing::Level::INFO => Style::default().fg(Color::White),
		tracing::Level::DEBUG => Style::default().fg(Color::Blue),
		tracing::Level::TRACE => Style::default().fg(Color::Gray),
	}
}

/// Get the string representation of a log level
fn get_log_level_string(level: tracing::Level) -> &'static str {
	match level {
		tracing::Level::ERROR => "ERROR",
		tracing::Level::WARN => "WARN",
		tracing::Level::INFO => "INFO",
		tracing::Level::DEBUG => "DEBUG",
		tracing::Level::TRACE => "TRACE",
	}
}

/// Create a dynamically sized popup area based on terminal dimensions.
///
/// Calculates appropriate popup size that scales with terminal size while
/// maintaining minimum and maximum bounds for usability.
///
/// # Arguments
/// * `area` - Terminal area to center the popup within
/// * `popup_type` - Type of popup to determine appropriate sizing
///
/// # Returns
/// Centered rectangle for the popup
fn dynamic_popup_area(area: Rect, popup_type: PopupType) -> Rect {
	let (width_pct, height_pct, min_width, min_height, max_width, max_height) = match popup_type {
		PopupType::PathInput => (60, 20, 40, 5, 100, 10),
		PopupType::FilterInput => (80, 60, 60, 15, 120, 40),
		PopupType::LogView => (85, 70, 70, 20, 150, 50),
	};

	// Calculate dimensions with bounds
	let popup_width = (area.width * width_pct / 100).clamp(min_width, max_width.min(area.width));
	let popup_height =
		(area.height * height_pct / 100).clamp(min_height, max_height.min(area.height));

	// Center the popup
	let x = (area.width.saturating_sub(popup_width)) / 2;
	let y = (area.height.saturating_sub(popup_height)) / 2;

	Rect::new(area.x + x, area.y + y, popup_width, popup_height)
}

/// Types of popups for dynamic sizing
#[derive(Debug, Clone, Copy)]
enum PopupType {
	PathInput,
	FilterInput,
	LogView,
}

fn render_filter_dialog(
	frame: &mut Frame,
	include_text: &str,
	exclude_text: &str,
	input_mode: &FilterInputMode,
) {
	trace!("Drawing filter dialog");
	let popup_rect = dynamic_popup_area(frame.area(), PopupType::FilterInput);
	frame.render_widget(Clear, popup_rect);

	// Split into two columns
	let columns = Layout::default()
		.direction(Direction::Horizontal)
		.constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
		.split(popup_rect);

	// Include patterns column
	let include_active = matches!(input_mode, FilterInputMode::Include);
	let include_style = if include_active {
		Style::default()
			.fg(Color::Yellow)
			.add_modifier(Modifier::BOLD)
	} else {
		Style::default().fg(Color::White)
	};

	// Create content with header and text buffer
	let mut include_content = vec![Line::from("Glob patterns to include (one per line):")];
	include_content.push(Line::from(""));

	// Add the text content with cursor if active
	let include_display_text = if include_active {
		format!("{}_", include_text) // Add cursor
	} else {
		include_text.to_string()
	};

	for line in include_display_text.lines() {
		include_content.push(Line::from(line.to_string()));
	}

	let include_widget = Paragraph::new(include_content)
		.block(
			Block::default()
				.borders(Borders::ALL)
				.title("Include")
				.style(include_style),
		)
		.wrap(Wrap { trim: true });
	frame.render_widget(include_widget, columns[0]);

	// Exclude patterns column
	let exclude_active = matches!(input_mode, FilterInputMode::Exclude);
	let exclude_style = if exclude_active {
		Style::default()
			.fg(Color::Yellow)
			.add_modifier(Modifier::BOLD)
	} else {
		Style::default().fg(Color::White)
	};

	// Create content with header and text buffer
	let mut exclude_content = vec![Line::from("Glob patterns to exclude (one per line):")];
	exclude_content.push(Line::from(""));

	// Add the text content with cursor if active
	let exclude_display_text = if exclude_active {
		format!("{}_", exclude_text) // Add cursor
	} else {
		exclude_text.to_string()
	};

	for line in exclude_display_text.lines() {
		exclude_content.push(Line::from(line.to_string()));
	}

	let exclude_widget = Paragraph::new(exclude_content)
		.block(
			Block::default()
				.borders(Borders::ALL)
				.title("Exclude")
				.style(exclude_style),
		)
		.wrap(Wrap { trim: true });
	frame.render_widget(exclude_widget, columns[1]);

	// Instructions at the bottom
	let instructions = Paragraph::new("Tab: Switch columns  |  Enter: New line  |  Backspace: Delete  |  F10: Apply  |  Esc: Cancel")
		.style(Style::default().fg(Color::Cyan))
		.alignment(Alignment::Center);

	let instruction_area = Rect {
		x: popup_rect.x,
		y: popup_rect.y + popup_rect.height - 1,
		width: popup_rect.width,
		height: 1,
	};
	frame.render_widget(instructions, instruction_area);
}

/// Handle terminal events and return a list of actions to process.
///
/// This is the main event processing function that polls for terminal events
/// (keyboard, mouse, resize) and converts them into application actions. It uses
/// a non-blocking approach to drain all available events efficiently.
///
/// The function handles:
/// - Keyboard events (global shortcuts, popup input, main interface)
/// - Mouse events (clicks, scrolling)
/// - Resize events (trigger layout recalculation)
///
/// Event processing is broken down into specialized functions for better
/// maintainability and reduced nesting.
///
/// # Arguments
/// * `popup_stack` - Mutable reference to popup stack for state-dependent handling
///
/// # Returns
/// Vector of actions to be processed by the main event loop
///
/// # Errors
/// Returns IO error if terminal event reading fails
fn handle_events(popup_stack: &mut PopupStack) -> std::io::Result<Vec<Action>> {
	let mut actions = Vec::new();

	// Use shorter polling timeout for better resize responsiveness
	if !event::poll(Duration::from_millis(5))? {
		return Ok(actions);
	}

	// Process all available events
	loop {
		let event = event::read()?;

		match event {
			Event::Key(key) => {
				if let Some(action) = handle_key_event(key, popup_stack)? {
					actions.push(action);
					break; // Process one action at a time for responsiveness
				}
			}
			Event::Mouse(mouse_event) => {
				if let Some(action) = handle_mouse_event(mouse_event, popup_stack) {
					actions.push(action);
				}
			}
			Event::Resize(width, height) => {
				// Handle resize events to trigger layout recalculation
				if let Some(action) = handle_resize_event(width, height) {
					actions.push(action);
					// Don't break here - continue processing other events
					// but prioritize resize events
				}
			}
			_ => {}
		}

		// Drain remaining events without blocking
		if !event::poll(Duration::from_millis(0))? {
			break;
		}
	}

	Ok(actions)
}

/// Handle keyboard events and return the corresponding action.
///
/// Processes keyboard input based on the current popup state. Global shortcuts
/// (like log toggle) work in any mode, while other keys are handled differently
/// depending on whether a popup is active.
///
/// # Arguments
/// * `key` - The keyboard event to process
/// * `popup_stack` - Current popup stack state
///
/// # Returns
/// Optional action if the key event should trigger an action
fn handle_key_event(
	key: crossterm::event::KeyEvent,
	popup_stack: &mut PopupStack,
) -> std::io::Result<Option<Action>> {
	debug!("Key event: {:?}", key);

	// Only handle key press events to avoid double-triggering
	if !key.is_press() {
		return Ok(None);
	}

	// Check for global shortcuts first (work in any mode)
	if let Some(action) = handle_global_shortcuts(key) {
		return Ok(Some(action));
	}

	// Handle popup-specific or main interface events
	match popup_stack.top_mut() {
		Some(PopupState::FilterInput {
			include_text,
			exclude_text,
			mode,
		}) => Ok(handle_filter_input_keys(
			key.code,
			include_text,
			exclude_text,
			mode,
		)),
		Some(PopupState::PathInput { buffer }) => Ok(handle_path_input_keys(key.code, buffer)),
		Some(PopupState::LogView) => {
			// Log view is handled by global controls
			Ok(None)
		}
		None => {
			// No popup active - handle main interface events
			Ok(handle_main_interface_keys(key.code))
		}
	}
}

/// Handle global keyboard shortcuts that work in any mode.
///
/// These shortcuts include log popup controls and level settings that
/// should be accessible regardless of the current popup state.
///
/// # Arguments
/// * `key` - The keyboard event to check
///
/// # Returns
/// Optional action if the key is a global shortcut
fn handle_global_shortcuts(key: crossterm::event::KeyEvent) -> Option<Action> {
	use crossterm::event::{KeyCode, KeyModifiers};

	match key.code {
		KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
			Some(Action::LogClear)
		}
		KeyCode::Char('l') | KeyCode::Char('L') => {
			debug!("Key 'L' pressed - toggling log view");
			Some(Action::ToggleLogView)
		}
		KeyCode::Char('1') => Some(Action::LogLevel(tracing::Level::ERROR)),
		KeyCode::Char('2') => Some(Action::LogLevel(tracing::Level::WARN)),
		KeyCode::Char('3') => Some(Action::LogLevel(tracing::Level::INFO)),
		KeyCode::Char('4') => Some(Action::LogLevel(tracing::Level::DEBUG)),
		KeyCode::Char('5') => Some(Action::LogLevel(tracing::Level::TRACE)),
		KeyCode::Char('j') => Some(Action::LogScrollDown),
		KeyCode::Char('k') => Some(Action::LogScrollUp),
		_ => None,
	}
}

/// Handle keyboard input for the filter input popup.
///
/// Manages text input for include/exclude patterns, column switching,
/// and filter submission/cancellation.
///
/// # Arguments
/// * `key_code` - The key that was pressed
/// * `include_text` - Mutable reference to include patterns text
/// * `exclude_text` - Mutable reference to exclude patterns text
/// * `mode` - Current input mode (include or exclude)
///
/// # Returns
/// Optional action if the key should trigger an action
fn handle_filter_input_keys(
	key_code: crossterm::event::KeyCode,
	include_text: &mut String,
	exclude_text: &mut String,
	mode: &FilterInputMode,
) -> Option<Action> {
	use crossterm::event::KeyCode;

	match key_code {
		KeyCode::Esc => Some(Action::ClosePopup),
		KeyCode::Tab => Some(Action::FilterSwitchColumn),
		KeyCode::F(10) => Some(Action::SubmitFilter),
		KeyCode::Enter => {
			match mode {
				FilterInputMode::Include => include_text.push('\n'),
				FilterInputMode::Exclude => exclude_text.push('\n'),
			}
			None
		}
		KeyCode::Backspace => {
			match mode {
				FilterInputMode::Include => {
					include_text.pop();
				}
				FilterInputMode::Exclude => {
					exclude_text.pop();
				}
			}
			None
		}
		KeyCode::Char(c) => {
			match mode {
				FilterInputMode::Include => include_text.push(c),
				FilterInputMode::Exclude => exclude_text.push(c),
			}
			None
		}
		_ => None,
	}
}

/// Handle keyboard input for the path input popup.
///
/// Manages text input for path entry, submission, and cancellation.
///
/// # Arguments
/// * `key_code` - The key that was pressed
/// * `buffer` - Mutable reference to the path input buffer
///
/// # Returns
/// Optional action if the key should trigger an action
fn handle_path_input_keys(
	key_code: crossterm::event::KeyCode,
	buffer: &mut String,
) -> Option<Action> {
	use crossterm::event::KeyCode;

	match key_code {
		KeyCode::Esc => Some(Action::ClosePopup),
		KeyCode::Enter => Some(Action::SubmitPath),
		KeyCode::Backspace => {
			buffer.pop();
			None
		}
		KeyCode::Char(c) => {
			buffer.push(c);
			None
		}
		_ => None,
	}
}

/// Handle keyboard input for the main interface (no popup active).
///
/// Processes navigation, application control, and popup activation keys.
///
/// # Arguments
/// * `key_code` - The key that was pressed
///
/// # Returns
/// Optional action if the key should trigger an action
fn handle_main_interface_keys(key_code: crossterm::event::KeyCode) -> Option<Action> {
	use crossterm::event::KeyCode;

	let action = match key_code {
		KeyCode::Char('q') => Some(Action::Quit),
		KeyCode::Char('r') => Some(Action::Refresh),
		KeyCode::Char('s') => Some(Action::Scan),
		KeyCode::Char('h') => Some(Action::Hash),
		KeyCode::Char('p') => Some(Action::ShowPathInput),
		KeyCode::Char('f') => Some(Action::ShowFilterInput),
		KeyCode::Up => Some(Action::Up),
		KeyCode::Down => Some(Action::Down),
		KeyCode::PageUp => Some(Action::PageUp),
		KeyCode::PageDown => Some(Action::PageDown),
		KeyCode::Home => Some(Action::Home),
		KeyCode::End => Some(Action::End),
		_ => None,
	};

	debug!("Key action: {:?}", action);
	action
}
/// Handle mouse events and return the corresponding action.
///
/// Processes mouse clicks and scroll events for navigation and selection.
/// Mouse events are only processed when no popup is active.
///
/// # Arguments
/// * `mouse_event` - The mouse event to process
/// * `popup_stack` - Current popup stack state
///
/// # Returns
/// Optional action if the mouse event should trigger an action
fn handle_mouse_event(
	mouse_event: crossterm::event::MouseEvent,
	popup_stack: &PopupStack,
) -> Option<Action> {
	use crossterm::event::{MouseButton, MouseEventKind};

	// Log mouse events for debugging
	info!(
		"Mouse event: {:?} at ({}, {})",
		mouse_event.kind, mouse_event.row, mouse_event.column
	);

	// Only handle mouse events when no popup is active
	if !popup_stack.is_empty() {
		debug!("Mouse event ignored (popup active)");
		return None;
	}

	match mouse_event.kind {
		MouseEventKind::ScrollUp => {
			debug!("Mouse scroll up");
			Some(Action::Up)
		}
		MouseEventKind::ScrollDown => {
			debug!("Mouse scroll down");
			Some(Action::Down)
		}
		MouseEventKind::Down(MouseButton::Left) => {
			debug!(
				"Left mouse button clicked at ({}, {})",
				mouse_event.row, mouse_event.column
			);
			Some(Action::SelectRow(mouse_event.row, mouse_event.column))
		}
		MouseEventKind::Down(button) => {
			debug!("Other mouse button down: {:?}", button);
			None
		}
		_ => {
			debug!("Other mouse event: {:?}", mouse_event.kind);
			None
		}
	}
}

/// Handle terminal resize events.
///
/// Creates a resize action to trigger layout recalculation and redraw.
/// This ensures the UI properly adapts to the new terminal dimensions.
///
/// # Arguments
/// * `width` - New terminal width
/// * `height` - New terminal height
///
/// # Returns
/// Optional resize action to trigger UI update
fn handle_resize_event(width: u16, height: u16) -> Option<Action> {
	info!(
		"Terminal resized to {}x{} - triggering layout recalculation",
		width, height
	);
	Some(Action::Resize(width, height))
}
