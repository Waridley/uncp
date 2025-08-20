use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use std::time::Duration;
// Usage: q=quit, r=refresh, s=scan, h=scan+hash, p=path input, Enter=confirm path, Esc=cancel

use async_channel as channel;
use async_executor::Executor;
use futures_lite::future;

use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use ratatui::{
	layout::{Alignment, Constraint, Direction, Flex, Layout, Rect},
	style::{Color, Modifier, Style},
	text::{Line, Span},
	widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap},
	Frame,
};

use tracing::{debug, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use uncp::engine::{BackgroundEngine, EngineCommand, EngineEvent};
use uncp::ui::PresentationState;

const SPINNER: &[char] = &['⠋', '⠙', '⠸', '⠴', '⠦', '⠇'];

fn main() -> std::io::Result<()> {
	let log_dir =
		uncp::paths::default_cache_dir().unwrap_or_else(|| std::env::temp_dir().join("uncp"));
	let _ = std::fs::create_dir_all(&log_dir);
	let log_path = log_dir.join("tui.log");
	let file = std::fs::File::create(&log_path).expect("open log file");
	let (nb, guard) = tracing_appender::non_blocking(file);

	// Tracing subscriber -> write to file (non-blocking), not the terminal
	let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	tracing_subscriber::registry()
		.with(fmt::layer().with_writer(nb))
		.with(env_filter)
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
		// Explicitly enable mouse support
		let _ = execute!(std::io::stdout(), crossterm::event::EnableMouseCapture);

		let mut terminal = ratatui::init();
		let _ = run(&ex, &mut terminal);

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
		println!("\n---- uncp TUI logs (last 80 lines) ----");
		for line in tail.into_iter().rev() {
			println!("{line}");
		}
		println!(
			"---------------------------------------\nLog file: {}",
			log_path.display()
		);
	} else {
		println!("Logs at: {}", log_path.display());
	}

	Ok(())
}

fn run(
	_ex: &std::sync::Arc<Executor<'_>>,
	terminal: &mut ratatui::DefaultTerminal,
) -> std::io::Result<()> {
	// UI state
	let mut current_path = PathBuf::from(".");
	let mut in_path_input = false;
	let mut in_filter_input = false;
	let mut current_filter = uncp::PathFilter::default();
	let mut filter_include_text = String::new(); // Multi-line text buffer for include patterns
	let mut filter_exclude_text = String::new(); // Multi-line text buffer for exclude patterns
	let mut filter_input_mode = FilterInputMode::Include; // Which column is active

	debug!("TUI loop start");

	let progress_state: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
	let mut spinner_idx: usize = 0;

	// Start background engine and subscribe to events
	// Use non-blocking detector creation to avoid startup delay
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
	let evt_rx = events;
	// Set initial path and start
	let _ = cmds.try_send(EngineCommand::SetPath(current_path.clone()));
	let _ = cmds.try_send(EngineCommand::Start);

	let mut input_buffer = String::new();
	let mut progress_line: Option<String> = None;

	// Enhanced status tracking
	let mut current_discovery_progress: Option<uncp::systems::SystemProgress> = None;
	let mut current_hashing_progress: Option<uncp::systems::SystemProgress> = None;
	let mut engine_status = "Starting...".to_string();
	let mut processing_speed: Option<f64> = None; // files per second
	let mut last_progress_update = std::time::Instant::now();
	let mut last_processed_count = 0;

	// Engine handles all background work; TUI just sends commands and receives snapshots
	// Initialize presentation state empty; engine snapshots will update it
	let mut pres = PresentationState::default()
		.with_status("Press 's' to scan, 'h' to hash, 'r' to refresh, 'q' to quit");

	// Table selection/scroll state
	let mut table_state = TableState::default();
	let mut selected_idx: usize = 0;

	loop {
		for action in handle_events(
			in_path_input,
			in_filter_input,
			&mut input_buffer,
			&mut filter_include_text,
			&mut filter_exclude_text,
			&mut filter_input_mode,
		)? {
			match action {
				Action::Quit => {
					info!("Quitting...");
					return Ok(());
				}
				Action::Refresh => {
					info!("Refreshing...");
					// Refresh just keeps current snapshot; engine will update
				}
				Action::EnterPathMode => {
					info!("Entering path mode");
					in_path_input = true;
					input_buffer = current_path.to_string_lossy().to_string();
					pres = pres
						.clone()
						.with_status("Enter path, then press Enter (Esc to cancel)");
				}
				Action::CancelPath => {
					info!("Canceling path input");
					if in_path_input {
						in_path_input = false;
						pres = pres.clone().with_status("Path input canceled");
					}
				}
				Action::EnterFilterMode => {
					info!("Entering filter mode");
					in_filter_input = true;
					filter_input_mode = FilterInputMode::Include;
					// Initialize text buffers from current filter
					filter_include_text = current_filter.include_patterns().join("\n");
					filter_exclude_text = current_filter.exclude_patterns().join("\n");
					input_buffer = String::new(); // Clear input buffer
					pres = pres
						.clone()
						.with_status("Filter Manager: Tab to switch columns, type patterns (one per line), F10 to apply, Esc to cancel");
				}
				Action::CancelFilter => {
					info!("Canceling filter input");
					if in_filter_input {
						in_filter_input = false;
						filter_include_text.clear();
						filter_exclude_text.clear();
						input_buffer.clear();
						pres = pres.clone().with_status("Filter input canceled");
					}
				}
				Action::FilterSwitchColumn => {
					if in_filter_input {
						filter_input_mode = match filter_input_mode {
							FilterInputMode::Include => FilterInputMode::Exclude,
							FilterInputMode::Exclude => FilterInputMode::Include,
						};
					}
				}

				Action::SubmitPath => {
					if in_path_input {
						let new_path_str = input_buffer.trim();
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
								pres = pres.clone().with_status(
									"Error: Could not determine home directory".to_string(),
								);
								// Stay in path input mode to allow correction
								continue;
							}
						} else {
							new_path_str.to_string()
						};

						// Validate the path exists
						let new_path = PathBuf::from(&expanded_path_str);
						if !new_path.exists() {
							pres = pres.clone().with_status(format!(
								"Error: Path '{}' does not exist",
								new_path.display()
							));
							// Stay in path input mode to allow correction
							continue;
						}

						current_path = new_path;
						in_path_input = false;
						pres = pres
							.clone()
							.with_status(format!("Scanning path: {}", current_path.display()));
						info!("Path set to {}, starting new scan", current_path.display());

						// Immediately stop current scan and clear state
						let _ = cmds.try_send(EngineCommand::Stop);
						let _ = cmds.try_send(EngineCommand::ClearState);

						// Wait a bit longer for the stop command to be processed and cancellation to take effect
						std::thread::sleep(std::time::Duration::from_millis(200));

						// Set new path and restart
						let _ = cmds.try_send(EngineCommand::SetPath(current_path.clone()));

						// Apply current filter if any
						if !current_filter.include_patterns().is_empty()
							|| !current_filter.exclude_patterns().is_empty()
						{
							let _ =
								cmds.try_send(EngineCommand::SetPathFilter(current_filter.clone()));
						}

						// Start the scan
						let _ = cmds.try_send(EngineCommand::Start);

						// Clear input buffer
						input_buffer.clear();
					}
				}
				Action::SubmitFilter => {
					if in_filter_input {
						// Parse patterns from text buffers (one pattern per line)
						let include_patterns: Vec<String> = filter_include_text
							.lines()
							.map(|line| line.trim().to_string())
							.filter(|line| !line.is_empty())
							.collect();

						let exclude_patterns: Vec<String> = filter_exclude_text
							.lines()
							.map(|line| line.trim().to_string())
							.filter(|line| !line.is_empty())
							.collect();

						info!(
							"Submitting filter with {} include, {} exclude patterns",
							include_patterns.len(),
							exclude_patterns.len()
						);

						// Create new filter from the pattern lists
						match uncp::PathFilter::new(
							include_patterns.clone(),
							exclude_patterns.clone(),
						) {
							Ok(new_filter) => {
								current_filter = new_filter.clone();
								in_filter_input = false;

								let status_msg =
									if include_patterns.is_empty() && exclude_patterns.is_empty() {
										"Filter cleared".to_string()
									} else {
										format!(
											"Filter set: {} include, {} exclude patterns",
											include_patterns.len(),
											exclude_patterns.len()
										)
									};
								pres = pres.clone().with_status(status_msg);

								// Apply filter to engine
								if include_patterns.is_empty() && exclude_patterns.is_empty() {
									let _ = cmds.try_send(EngineCommand::ClearPathFilter);
								} else {
									let _ = cmds.try_send(EngineCommand::SetPathFilter(new_filter));
								}

								// Restart scan with new filter
								let _ = cmds.try_send(EngineCommand::Stop);
								let _ = cmds.try_send(EngineCommand::ClearState);
								let _ = cmds.try_send(EngineCommand::SetPath(current_path.clone()));
								let _ = cmds.try_send(EngineCommand::Start);

								// Clear temporary state
								filter_include_text.clear();
								filter_exclude_text.clear();
								input_buffer.clear();
							}
							Err(e) => {
								pres = pres
									.clone()
									.with_status(format!("Invalid filter pattern: {}", e));
								// Stay in filter input mode to allow correction
							}
						}
					}
				}

				Action::Scan => {
					// Path validation before starting
					let path = current_path.clone();
					info!("Scan requested for {}", path.display());

					if !Path::new(&path).exists() {
						pres = pres
							.clone()
							.with_status(format!("Path does not exist: {}", path.display()));
						continue;
					}
					// Delegate to engine: set path and start
					let _ = cmds.try_send(EngineCommand::SetPath(path));
					let _ = cmds.try_send(EngineCommand::Start);
					pres = pres.clone().with_status("Scanning...");
				}
				Action::Hash => {
					// Delegate hashing to engine; just ensure engine runs
					let _ = cmds.try_send(EngineCommand::Start);
					pres = pres.clone().with_status("Hashing started...");
				}
				Action::Up => {
					selected_idx = selected_idx.saturating_sub(1);
					table_state.select(Some(selected_idx));
				}
				Action::Down => {
					let total = pres.file_table.len();
					if total > 0 && selected_idx + 1 < total {
						selected_idx += 1;
					}
					table_state.select(Some(selected_idx));
				}
				Action::PageUp => {
					let step = 10usize;
					selected_idx = selected_idx.saturating_sub(step);
					table_state.select(Some(selected_idx));
				}
				Action::PageDown => {
					let total = pres.file_table.len();
					let step = 10usize;
					if total > 0 {
						selected_idx = (selected_idx + step).min(total.saturating_sub(1));
					}
					table_state.select(Some(selected_idx));
				}
				Action::Home => {
					selected_idx = 0;
					table_state.select(Some(selected_idx));
				}
				Action::End => {
					let total = pres.file_table.len();
					if total > 0 {
						selected_idx = total - 1;
					}
					table_state.select(Some(selected_idx));
				}
				Action::SelectRow(mouse_row, mouse_col) => {
					// Debug: Always log mouse clicks to understand coordinates
					debug!(
						"Mouse click at ({}, {}), terminal size: {:?}",
						mouse_row,
						mouse_col,
						terminal.size()
					);

					// Calculate which table row was clicked based on mouse coordinates
					// Layout structure:
					// - chunks[0]: header (4 lines) - Constraint::Length(4)
					// - chunks[1]: table body - Constraint::Min(5)
					// - chunks[2]: status footer (5 lines) - Constraint::Length(5)
					// - chunks[3]: keybinding hints (1 line) - Constraint::Length(1)
					//
					// Within the table chunk (chunks[1]):
					// - 1 line for top border
					// - 1 line for column headers
					// - Data rows start after that

					let header_height = 4; // chunks[0] height
					let table_border_and_header = 2; // top border + column header
					let table_data_start = header_height + table_border_and_header;

					debug!(
						"Table calculation: header_height={}, table_data_start={}",
						header_height, table_data_start
					);

					// Check if click is within the table area vertically
					if mouse_row >= table_data_start {
						let clicked_row = (mouse_row - table_data_start) as usize;
						let total = pres.file_table.len();

						debug!(
							"Clicked row calculation: mouse_row={}, clicked_row={}, total_files={}",
							mouse_row, clicked_row, total
						);

						// Simple bounds check - just verify we're clicking on a valid row
						if total > 0 && clicked_row < total {
							selected_idx = clicked_row;
							table_state.select(Some(selected_idx));
							debug!(
								"✅ Mouse selection successful: selected_idx={}",
								selected_idx
							);

							// Update status to show mouse selection worked
							pres = pres.clone().with_status(format!(
								"Selected row {} via mouse",
								selected_idx + 1
							));
						} else {
							debug!(
								"❌ Mouse click outside valid range: clicked_row={}, total={}",
								clicked_row, total
							);
						}
					} else {
						debug!(
							"❌ Mouse click above table area: mouse_row={}, table_data_start={}",
							mouse_row, table_data_start
						);
					}
				}
			}
		}
		// Update progress and re-render every loop
		if let Ok(guard) = progress_state.lock() {
			progress_line = guard.clone();
		}
		// Consume engine events opportunistically to refresh UI
		while let Ok(evt) = evt_rx.try_recv() {
			match evt {
				EngineEvent::SnapshotReady(snap) => {
					debug!(
						"TUI: Received snapshot with {} files, {} pending hash",
						snap.total_files, snap.pending_hash
					);
					pres = snap;
					// Clamp selection within bounds when data changes
					let total = pres.file_table.len();
					if total == 0 {
						selected_idx = 0;
						table_state.select(None);
					} else {
						if selected_idx >= total {
							selected_idx = total - 1;
						}
						table_state.select(Some(selected_idx));
					}
				}
				EngineEvent::Started => {
					debug!("TUI: Engine started");
					engine_status = "Ready".to_string();
					pres = pres.clone().with_status("Scan engine ready");
				}
				EngineEvent::Completed => {
					debug!("TUI: Engine completed");
					engine_status = "Completed".to_string();
					pres = pres.clone().with_status("Scan completed");
				}
				EngineEvent::CacheLoading => {
					debug!("TUI: Cache loading");
					engine_status = "Loading cache".to_string();
					pres = pres.clone().with_status("Loading cache from disk");
				}
				EngineEvent::CacheLoaded => {
					debug!("TUI: Cache loaded");
					engine_status = "Ready".to_string();
					pres = pres.clone().with_status("Cache loaded successfully");
				}
				EngineEvent::CacheSaving => {
					debug!("TUI: Cache saving");
					engine_status = "Saving cache".to_string();
					pres = pres.clone().with_status("Saving cache to disk");
				}
				EngineEvent::CacheSaved => {
					debug!("TUI: Cache saved");
					// Don't change engine_status here, keep current operation status
					pres = pres.clone().with_status("Cache saved successfully");
				}
				EngineEvent::CacheValidating => {
					debug!("TUI: Cache validating");
					engine_status = "Validating cache".to_string();
					pres = pres
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
					engine_status = "Ready".to_string();
					let status_msg = if files_removed > 0 || files_invalidated > 0 {
						format!(
							"Cache validated: {} files removed, {} files marked for re-processing",
							files_removed, files_invalidated
						)
					} else {
						"Cache validated: all files up to date".to_string()
					};
					pres = pres.clone().with_status(status_msg);
				}
				EngineEvent::DiscoveryProgress(progress) => {
					debug!(
						"TUI: Discovery progress: {}/{} - {:?}",
						progress.processed_items, progress.total_items, progress.current_item
					);

					// Calculate processing speed
					let now = std::time::Instant::now();
					if now.duration_since(last_progress_update).as_secs() >= 1 {
						let items_processed = progress
							.processed_items
							.saturating_sub(last_processed_count);
						let elapsed = now.duration_since(last_progress_update).as_secs_f64();
						if elapsed > 0.0 {
							processing_speed = Some(items_processed as f64 / elapsed);
						}
						last_progress_update = now;
						last_processed_count = progress.processed_items;
					}

					current_discovery_progress = Some(progress);
					engine_status = "Discovering files".to_string();
				}
				EngineEvent::HashingProgress(progress) => {
					debug!(
						"TUI: Hashing progress: {}/{} - {:?}",
						progress.processed_items, progress.total_items, progress.current_item
					);

					// Calculate processing speed for hashing
					let now = std::time::Instant::now();
					if now.duration_since(last_progress_update).as_secs() >= 1 {
						let items_processed = progress
							.processed_items
							.saturating_sub(last_processed_count);
						let elapsed = now.duration_since(last_progress_update).as_secs_f64();
						if elapsed > 0.0 {
							processing_speed = Some(items_processed as f64 / elapsed);
						}
						last_progress_update = now;
						last_processed_count = progress.processed_items;
					}

					current_hashing_progress = Some(progress);
					engine_status = "Hashing files".to_string();
				}
				EngineEvent::Error(err) => {
					debug!("TUI: Engine error: {}", err);
					engine_status = format!("Error: {}", err);
				}
			}
		}

		if progress_line.as_deref() == Some("Scan complete") {
			pres = pres.clone().with_status("Scan complete");
			progress_line = None;
		}
		// Spinner effect while progress is active
		let progress_display = if let Some(ref p) = progress_line {
			spinner_idx = (spinner_idx + 1) % SPINNER.len();
			Some(format!("{} {}", SPINNER[spinner_idx], p))
		} else {
			None
		};
		terminal.draw(|f| {
			draw_enhanced(
				f,
				&pres,
				in_path_input,
				in_filter_input,
				&input_buffer,
				&current_filter,
				&filter_include_text,
				&filter_exclude_text,
				&filter_input_mode,
				progress_display.as_deref(),
				&engine_status,
				&current_discovery_progress,
				&current_hashing_progress,
				processing_speed,
				&mut table_state,
			)
		})?;
	}
}

#[derive(Debug, Clone, Copy)]
enum FilterInputMode {
	Include,
	Exclude,
}

#[derive(Debug, Clone, Copy)]
enum Action {
	Quit,
	Refresh,
	Scan,
	Hash,
	EnterPathMode,
	CancelPath,
	SubmitPath,
	EnterFilterMode,
	CancelFilter,
	SubmitFilter,
	FilterSwitchColumn,
	Up,
	Down,
	PageUp,
	PageDown,
	Home,
	End,
	SelectRow(u16, u16), // row, column
}

#[allow(clippy::too_many_arguments)]
fn draw_enhanced(
	frame: &mut Frame,
	pres: &PresentationState,
	in_path_input: bool,
	in_filter_input: bool,
	input_buffer: &str,
	current_filter: &uncp::PathFilter,
	filter_include_text: &str,
	filter_exclude_text: &str,
	filter_input_mode: &FilterInputMode,
	progress: Option<&str>,
	engine_status: &str,
	discovery_progress: &Option<uncp::systems::SystemProgress>,
	hashing_progress: &Option<uncp::systems::SystemProgress>,
	processing_speed: Option<f64>,
	table_state: &mut TableState,
) {
	let in_input = in_path_input || in_filter_input;
	let chunks = Layout::default()
		.direction(Direction::Vertical)
		.constraints([
			Constraint::Length(4), // header
			Constraint::Min(5),    // body
			Constraint::Length(5), // status footer
			Constraint::Length(1), // keybinding hints (no box, just one line)
		])
		.split(frame.area());

	// Header with summary information
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
	frame.render_widget(header, chunks[0]);

	// Body: file table sorted by size
	let title = if pres.current_path_filter.is_empty() {
		"Files (sorted by size)".to_string()
	} else {
		format!("Files in '{}' (sorted by size)", pres.current_path_filter)
	};

	// Always show file table
	let header = Row::new(vec![
		Cell::from("Path"),
		Cell::from("Size"),
		Cell::from("Type"),
		Cell::from("Hashed"),
	])
	.style(Style::default().add_modifier(Modifier::BOLD));

	let rows: Vec<Row> = pres
		.file_table
		.iter()
		.map(|(path, size, file_type, hashed)| {
			let size_str = format_file_size(*size);
			let hashed_str = if *hashed { "✓" } else { "○" };
			Row::new(vec![
				Cell::from(format!("{path}")),
				Cell::from(size_str),
				Cell::from(file_type.as_str()),
				Cell::from(hashed_str),
			])
		})
		.collect();

	let table = Table::new(
		rows,
		[
			Constraint::Percentage(60), // Path
			Constraint::Percentage(15), // Size
			Constraint::Percentage(15), // Type
			Constraint::Percentage(10), // Hashed
		],
	)
	.header(header)
	.row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
	.block(Block::default().borders(Borders::ALL).title(title));

	frame.render_stateful_widget(table, chunks[1], table_state);

	// Enhanced footer with detailed status
	let mut status_lines = Vec::new();

	// Scan status line
	let mut scan_line = format!("Scan status: {}", engine_status);
	if let Some(speed) = processing_speed {
		scan_line.push_str(&format!(" ({:.1} files/sec)", speed));
	}
	status_lines.push(Line::from(Span::raw(scan_line)));

	// Always show discovery status based on current detector state
	let total_files = pres.total_files;
	let pending_files = pres.pending_hash;
	let discovered_files = total_files;

	// Discovery line - show just count and current file path
	let disc_line = if let Some(ref disc) = discovery_progress {
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
	};
	status_lines.push(Line::from(Span::raw(disc_line)));

	// Always show hashing status based on current detector state
	let hash_line = if let Some(ref hash) = hashing_progress {
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
	};
	status_lines.push(Line::from(Span::raw(hash_line)));

	// Legacy status and input
	let mut legacy_status = pres.status.clone();
	if let Some(p) = progress {
		legacy_status = if legacy_status.is_empty() {
			p.to_string()
		} else {
			format!("{} | {}", legacy_status, p)
		};
	}
	if in_input {
		legacy_status = if legacy_status.is_empty() {
			format!("Path: {}", input_buffer)
		} else {
			format!("{} | Path: {}", legacy_status, input_buffer)
		};
	}
	if !legacy_status.is_empty() {
		status_lines.push(Line::from(Span::raw(legacy_status)));
	}

	let footer =
		Paragraph::new(status_lines).block(Block::default().borders(Borders::ALL).title("Status"));
	frame.render_widget(footer, chunks[2]);

	// Keybinding hints at the bottom with distinct styling (no box, better contrast)
	let mut hint_spans = vec![
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
	];

	// Only show Enter/Esc hints when in path input mode
	if in_path_input {
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

	let keybinding_hints = Paragraph::new(Line::from(hint_spans))
		.style(Style::default().bg(Color::Black).fg(Color::White));
	frame.render_widget(keybinding_hints, chunks[3]);

	// Render input popup if in any input mode
	if in_input {
		if in_filter_input {
			// Render advanced filter dialog
			render_filter_dialog(
				frame,
				filter_include_text,
				filter_exclude_text,
				filter_input_mode,
			);
		} else if in_path_input {
			// Render simple path input dialog
			let popup_area = popup_area(frame.area(), 60, 20);
			frame.render_widget(Clear, popup_area);

			let input_display = format!("{}_", input_buffer);
			let input_widget = Paragraph::new(input_display).block(
				Block::default()
					.borders(Borders::ALL)
					.title("Set Path")
					.style(Style::default().fg(Color::Yellow)),
			);
			frame.render_widget(input_widget, popup_area);
		}
	}
}

/// Helper function to create a centered popup area
fn popup_area(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
	let vertical = Layout::vertical([Constraint::Percentage(percent_y)]).flex(Flex::Center);
	let horizontal = Layout::horizontal([Constraint::Percentage(percent_x)]).flex(Flex::Center);
	let [area] = vertical.areas(area);
	let [area] = horizontal.areas(area);
	area
}

fn render_filter_dialog(
	frame: &mut Frame,
	include_text: &str,
	exclude_text: &str,
	input_mode: &FilterInputMode,
) {
	let popup_area = popup_area(frame.area(), 80, 60);
	frame.render_widget(Clear, popup_area);

	// Split into two columns
	let columns = Layout::default()
		.direction(Direction::Horizontal)
		.constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
		.split(popup_area);

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
		x: popup_area.x,
		y: popup_area.y + popup_area.height - 1,
		width: popup_area.width,
		height: 1,
	};
	frame.render_widget(instructions, instruction_area);
}

fn handle_events(
	in_path_input: bool,
	in_filter_input: bool,
	input_buffer: &mut String,
	filter_include_text: &mut String,
	filter_exclude_text: &mut String,
	filter_input_mode: &mut FilterInputMode,
) -> std::io::Result<Vec<Action>> {
	let in_input = in_path_input || in_filter_input;
	let mut actions = Vec::new();
	// Wait briefly for at least one event, then drain the rest without waiting
	if event::poll(Duration::from_millis(10))? {
		loop {
			match event::read()? {
				Event::Key(key) => {
					if in_input {
						if in_filter_input {
							// Special handling for filter input mode
							match key.code {
								KeyCode::Esc => actions.push(Action::CancelFilter),
								KeyCode::Tab => actions.push(Action::FilterSwitchColumn),
								KeyCode::F(10) => actions.push(Action::SubmitFilter), // F10 to apply filters
								KeyCode::Enter => {
									// Add newline to the active text buffer
									match filter_input_mode {
										FilterInputMode::Include => filter_include_text.push('\n'),
										FilterInputMode::Exclude => filter_exclude_text.push('\n'),
									}
								}
								KeyCode::Backspace => {
									// Remove character from the active text buffer
									match filter_input_mode {
										FilterInputMode::Include => {
											filter_include_text.pop();
										}
										FilterInputMode::Exclude => {
											filter_exclude_text.pop();
										}
									}
								}
								KeyCode::Char(c) => {
									// Add character to the active text buffer
									match filter_input_mode {
										FilterInputMode::Include => filter_include_text.push(c),
										FilterInputMode::Exclude => filter_exclude_text.push(c),
									}
								}
								_ => {}
							}
						} else if in_path_input {
							// Path input mode
							match key.code {
								KeyCode::Esc => actions.push(Action::CancelPath),
								KeyCode::Enter => actions.push(Action::SubmitPath),
								KeyCode::Backspace => {
									input_buffer.pop();
								}
								KeyCode::Char(c) => {
									input_buffer.push(c);
								}
								_ => {}
							}
						}
					} else {
						let action = match key.code {
							KeyCode::Char('q') => Some(Action::Quit),
							KeyCode::Char('r') => Some(Action::Refresh),
							KeyCode::Char('s') => Some(Action::Scan),
							KeyCode::Char('h') => Some(Action::Hash),
							KeyCode::Char('p') => Some(Action::EnterPathMode),
							KeyCode::Char('f') => Some(Action::EnterFilterMode),
							KeyCode::Up => Some(Action::Up),
							KeyCode::Down => Some(Action::Down),
							KeyCode::PageUp => Some(Action::PageUp),
							KeyCode::PageDown => Some(Action::PageDown),
							KeyCode::Home => Some(Action::Home),
							KeyCode::End => Some(Action::End),
							_ => None,
						};
						if let Some(a) = action {
							actions.push(a);
						}
					}
				}
				Event::Mouse(me) => {
					use crossterm::event::MouseEventKind;
					// Debug: Log ALL mouse events to see what we're receiving
					info!("Mouse event: {:?} at ({}, {})", me.kind, me.row, me.column);

					if !in_input {
						match me.kind {
							MouseEventKind::ScrollUp => {
								debug!("Mouse scroll up");
								actions.push(Action::Up);
							}
							MouseEventKind::ScrollDown => {
								debug!("Mouse scroll down");
								actions.push(Action::Down);
							}
							MouseEventKind::Down(button) => {
								debug!("Mouse button down: {:?}", button);
								// Only handle left mouse button clicks for row selection
								if matches!(button, crossterm::event::MouseButton::Left) {
									debug!(
										"Left mouse button clicked at ({}, {})",
										me.row, me.column
									);
									actions.push(Action::SelectRow(me.row, me.column));
								}
							}
							_ => {
								debug!("Other mouse event: {:?}", me.kind);
							}
						}
					} else {
						debug!("Mouse event ignored (in input mode)");
					}
				}
				Event::Resize(_, _) => {
					// ignore
				}
				_ => {}
			}
			// drain without blocking
			if !event::poll(Duration::from_millis(0))? {
				break;
			}
		}
	}
	Ok(actions)
}

/// Format file size in human-readable format
fn format_file_size(size: u64) -> String {
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
