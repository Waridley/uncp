use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use std::time::Duration;
// Usage: q=quit, r=refresh, s=scan, h=scan+hash, p=path input, Enter=confirm path, Esc=cancel

use async_channel as channel;
use async_executor::Executor;
use easy_parallel::Parallel;
use futures_lite::future;

use crossterm::event::{self, Event, KeyCode};
use ratatui::{
	layout::{Constraint, Direction, Layout},
	style::{Color, Modifier, Style},
	text::{Line, Span},
	widgets::{Block, Borders, List, ListItem, Paragraph},
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

	// Build a smol-compatible multithreaded executor (async-executor + easy-parallel)
	let ex = Executor::new();
	let (signal, shutdown) = channel::unbounded::<()>();
	let nthreads = std::thread::available_parallelism()
		.map(|n| n.get())
		.unwrap_or(4)
		.max(2);
	let ex_ref = &ex;

	// Run executor worker threads and the TUI on the current thread
	Parallel::new()
		.each(0..nthreads, |_| {
			future::block_on(ex_ref.run(shutdown.recv()))
		})
		.finish(|| {
			let mut terminal = ratatui::init();
			let _ = run(ex_ref, &mut terminal);
			ratatui::restore();

			// Stop executor threads
			drop(signal);

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
		});

	Ok(())
}

fn run(_ex: &Executor<'_>, terminal: &mut ratatui::DefaultTerminal) -> std::io::Result<()> {
	// UI state
	let mut current_path = PathBuf::from(".");
	let mut in_path_input = false;
	debug!("TUI loop start");

	let progress_state: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
	let mut spinner_idx: usize = 0;

	// Start background engine and subscribe to events
	// Use non-blocking detector creation to avoid startup delay
	let detector = uncp::DuplicateDetector::new(uncp::DetectorConfig::default()).expect("init detector");
	let (_engine, events, cmds) = BackgroundEngine::start(detector);

	// Load cache in background after TUI starts
	let cmds_clone = cmds.clone();
	smol::spawn(async move {
		if let Some(dir) = uncp::paths::default_cache_dir() {
			let _ = cmds_clone.send(uncp::engine::EngineCommand::LoadCache(dir)).await;
		}
	}).detach();
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

	loop {
		if let Some(action) = handle_events(in_path_input, &mut input_buffer)? {
			match action {
				Action::Quit => {
					info!("Quitting...");
					break Ok(());
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

				Action::SubmitPath => {
					info!("Submitting path: {}", input_buffer.trim());
					if in_path_input {
						current_path = PathBuf::from(input_buffer.trim());
						in_path_input = false;
						pres = pres
							.clone()
							.with_status(format!("Path set to {}", current_path.display()));
						info!("Path set to {}", current_path.display());
					}
					// Also guide engine to new path
					let _ = cmds.try_send(EngineCommand::SetPath(current_path.clone()));
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
					debug!("TUI: Received snapshot with {} files, {} pending hash", snap.total_files, snap.pending_hash);
					pres = snap;
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
					pres = pres.clone().with_status("Validating cached files against filesystem");
				}
				EngineEvent::CacheValidated { files_removed, files_invalidated } => {
					debug!("TUI: Cache validated - {} removed, {} invalidated", files_removed, files_invalidated);
					engine_status = "Ready".to_string();
					let status_msg = if files_removed > 0 || files_invalidated > 0 {
						format!("Cache validated: {} files removed, {} files marked for re-processing", files_removed, files_invalidated)
					} else {
						"Cache validated: all files up to date".to_string()
					};
					pres = pres.clone().with_status(status_msg);
				}
				EngineEvent::DiscoveryProgress(progress) => {
					debug!("TUI: Discovery progress: {}/{} - {:?}",
						progress.processed_items, progress.total_items, progress.current_item);

					// Calculate processing speed
					let now = std::time::Instant::now();
					if now.duration_since(last_progress_update).as_secs() >= 1 {
						let items_processed = progress.processed_items.saturating_sub(last_processed_count);
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
					debug!("TUI: Hashing progress: {}/{} - {:?}",
						progress.processed_items, progress.total_items, progress.current_item);

					// Calculate processing speed for hashing
					let now = std::time::Instant::now();
					if now.duration_since(last_progress_update).as_secs() >= 1 {
						let items_processed = progress.processed_items.saturating_sub(last_processed_count);
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
				&input_buffer,
				progress_display.as_deref(),
				&engine_status,
				&current_discovery_progress,
				&current_hashing_progress,
				processing_speed,
			)
		})?;
	}
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
}

fn draw_enhanced(
	frame: &mut Frame,
	pres: &PresentationState,
	in_input: bool,
	input: &str,
	progress: Option<&str>,
	engine_status: &str,
	discovery_progress: &Option<uncp::systems::SystemProgress>,
	hashing_progress: &Option<uncp::systems::SystemProgress>,
	processing_speed: Option<f64>,
) {
	let chunks = Layout::default()
		.direction(Direction::Vertical)
		.constraints([
			Constraint::Length(4), // header
			Constraint::Min(5),    // body
			Constraint::Length(5), // status footer
			Constraint::Length(3), // keybinding hints
		])
		.split(frame.area());

	// Header with summary information
	let header = Paragraph::new(Line::from(vec![
		Span::styled("uncp TUI", Style::default().fg(Color::Cyan)),
		Span::raw("  |  Total: "),
		Span::raw(pres.total_files.to_string()),
		Span::raw("  Pending: "),
		Span::raw(pres.pending_hash.to_string()),
	]))
	.block(Block::default().borders(Borders::ALL).title("Summary"));
	frame.render_widget(header, chunks[0]);

	// Body: list by type
	let items: Vec<ListItem> = pres
		.by_type
		.iter()
		.map(|(k, v)| ListItem::new(format!("{:<16} {:>8}", k, v)))
		.collect();
	let list = List::new(items).block(Block::default().borders(Borders::ALL).title("By Type"));
	frame.render_widget(list, chunks[1]);

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
				format!("...{}", &current[current.len()-57..])
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
			format!("Discovery: {} files - Discovery completed", discovered_files)
		}
	};
	status_lines.push(Line::from(Span::raw(disc_line)));

	// Always show hashing status based on current detector state
	let mut hash_line = if let Some(ref hash) = hashing_progress {
		// Use progress event data if available
		let percentage = if hash.total_items > 0 {
			(hash.processed_items as f64 / hash.total_items as f64 * 100.0) as u32
		} else {
			0
		};
		let mut line = format!("Hashing: {}/{} ({}%)",
			hash.processed_items, hash.total_items, percentage);
		if let Some(ref current) = hash.current_item {
			// Truncate long paths for display
			let display_path = if current.len() > 50 {
				format!("...{}", &current[current.len()-47..])
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
			format!("Hashing: {}/{} (100%) - All files hashed",
				total_files, total_files)
		} else {
			let hashed_files = total_files - pending_files;
			let percentage = if total_files > 0 {
				(hashed_files as f64 / total_files as f64 * 100.0) as u32
			} else {
				0
			};
			format!("Hashing: {}/{} ({}%) - {} files pending",
				hashed_files, total_files, percentage, pending_files)
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
			format!("Path: {}", input)
		} else {
			format!("{} | Path: {}", legacy_status, input)
		};
	}
	if !legacy_status.is_empty() {
		status_lines.push(Line::from(Span::raw(legacy_status)));
	}

	let footer = Paragraph::new(status_lines)
		.block(Block::default().borders(Borders::ALL).title("Status"));
	frame.render_widget(footer, chunks[2]);

	// Keybinding hints at the bottom with distinct styling
	let keybinding_hints = Paragraph::new(Line::from(vec![
		Span::styled("Keys: ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
		Span::styled("q", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
		Span::raw(" quit  "),
		Span::styled("r", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
		Span::raw(" refresh  "),
		Span::styled("s", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
		Span::raw(" scan  "),
		Span::styled("h", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
		Span::raw(" hash  "),
		Span::styled("p", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
		Span::raw(" path"),
	]))
	.block(Block::default().borders(Borders::ALL).title("Controls"))
	.style(Style::default().bg(Color::DarkGray));
	frame.render_widget(keybinding_hints, chunks[3]);
}

fn handle_events(in_input: bool, input_buffer: &mut String) -> std::io::Result<Option<Action>> {
	if event::poll(Duration::from_millis(100))? {
		if let Event::Key(key) = event::read()? {
			if in_input {
				match key.code {
					KeyCode::Esc => return Ok(Some(Action::CancelPath)),
					KeyCode::Enter => return Ok(Some(Action::SubmitPath)),
					KeyCode::Backspace => {
						input_buffer.pop();
						return Ok(None);
					}
					KeyCode::Char(c) => {
						input_buffer.push(c);
						return Ok(None);
					}
					_ => {}
				}
				return Ok(None);
			}
			return Ok(match key.code {
				KeyCode::Char('q') => Some(Action::Quit),
				KeyCode::Char('r') => Some(Action::Refresh),
				KeyCode::Char('s') => Some(Action::Scan),
				KeyCode::Char('h') => Some(Action::Hash),
				KeyCode::Char('p') => Some(Action::EnterPathMode),
				_ => None,
			});
		}
	}
	Ok(None)
}
