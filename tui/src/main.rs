use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use std::time::Duration;
// Usage: q=quit, r=refresh, s=scan, h=scan+hash, p=path input, Enter=confirm path, Esc=cancel

use async_channel as channel;
use async_executor::Executor;
use easy_parallel::Parallel;
use futures_lite::future;

use crossterm::event::{self, Event, KeyCode};
use ratatui::{layout::{Constraint, Direction, Layout}, style::{Color, Style}, text::{Line, Span}, widgets::{Block, Borders, List, ListItem, Paragraph}, Frame};
use uncp::{DetectorConfig, DuplicateDetector};
use uncp::ui::PresentationState;
use tracing::{debug, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};


const SPINNER: &[char] = &['⠋','⠙','⠸','⠴','⠦','⠇'];

fn main() -> std::io::Result<()> {
    let log_dir = uncp::paths::default_cache_dir().unwrap_or_else(|| std::env::temp_dir().join("uncp"));
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
    let nthreads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).max(2);
    let ex_ref = &ex;

    // Run executor worker threads and the TUI on the current thread
    Parallel::new()
        .each(0..nthreads, |_| future::block_on(ex_ref.run(shutdown.recv())))
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
                for line in tail.into_iter().rev() { println!("{line}"); }
                println!("---------------------------------------\nLog file: {}", log_path.display());
            } else {
                println!("Logs at: {}", log_path.display());
            }
        });

    Ok(())
}

fn run(ex: &Executor<'_>, terminal: &mut ratatui::DefaultTerminal) -> std::io::Result<()> {
    let mut detector = DuplicateDetector::new(DetectorConfig::default()).expect("init detector");
    // UI state
    let mut current_path = PathBuf::from(".");
    let mut in_path_input = false;
    debug!("TUI loop start");

    let progress_state: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let mut spinner_idx: usize = 0;


    let mut input_buffer = String::new();

    let mut progress_line: Option<String> = None;

    // Build per-task detectors when needed to avoid Send bounds on MutexGuard (placeholder, may reuse later)
    let _task_detector_scan = Arc::new(Mutex::new(DuplicateDetector::new(DetectorConfig::default()).expect("detector")));
    let _task_detector_hash = Arc::new(Mutex::new(DuplicateDetector::new(DetectorConfig::default()).expect("detector")));

    let mut pres = PresentationState::from_detector(&detector).with_status("Press 's' to scan, 'h' to hash, 'r' to refresh, 'q' to quit");

    loop {
        if let Some(action) = handle_events(in_path_input, &mut input_buffer)? {
            match action {
                Action::Quit => {
                    info!("Quitting...");
                    break Ok(())
                },
                Action::Refresh => {
                    info!("Refreshing...");
                    pres = PresentationState::from_detector(&detector).with_status("Refreshed");
                }
                Action::EnterPathMode => {
                    info!("Entering path mode");
                    in_path_input = true;
                    input_buffer = current_path.to_string_lossy().to_string();
                    pres = pres.clone().with_status("Enter path, then press Enter (Esc to cancel)");
                }
                Action::CancelPath => {
                    info!("Canceling path input");
                    if in_path_input { in_path_input = false; pres = pres.clone().with_status("Path input canceled"); }
                }
                Action::SubmitPath => {
                    info!("Submitting path: {}", input_buffer.trim());
                    if in_path_input {
                        current_path = PathBuf::from(input_buffer.trim());
                        in_path_input = false;
                        pres = pres.clone().with_status(format!("Path set to {}", current_path.display()));
                        info!("Path set to {}", current_path.display());
                    }
                }

                Action::Scan => {
                    // Path validation before starting
                    let path = current_path.clone();
                    info!("Scan requested for {}", path.display());

                    if !Path::new(&path).exists() {
                        pres = pres.clone().with_status(format!("Path does not exist: {}", path.display()));
                        continue;
                    }
                    // Run scan with progress callback (non-blocking)
                    let progress_state_cb = Arc::clone(&progress_state);
                    let cb = Arc::new(move |p: uncp::systems::SystemProgress| {
                        let msg = if let Some(item) = p.current_item { format!("{}: {} / {} - {}", p.system_name, p.processed_items, p.total_items, item) } else { format!("{}: {} / {}", p.system_name, p.processed_items, p.total_items) };
                        if let Ok(mut guard) = progress_state_cb.lock() { *guard = Some(msg); }
                    });
                    let progress_state_done = Arc::clone(&progress_state);
                    let path_for_task = path.clone();
                    ex.spawn(async move {
                        debug!("Spawned scan task");
                        let mut det = DuplicateDetector::new(DetectorConfig::default()).expect("detector");
                        let _ = det.scan_with_progress(path_for_task, cb).await;
                        if let Ok(mut guard) = progress_state_done.lock() { *guard = Some("Scan complete".to_string()); }
                    }).detach();
                    pres = pres.clone().with_status("Scanning...");
                }
                Action::Hash => {
                    let path_for_task = current_path.clone();
                    ex.spawn(async move {
                        let mut det = DuplicateDetector::new(DetectorConfig::default()).expect("detector");
                        let _ = det.scan_and_hash(path_for_task).await;
                    }).detach();
                    pres = pres.clone().with_status("Hashing started...");
                }
            }
        }
        // Update progress and re-render every loop
        if let Ok(guard) = progress_state.lock() { progress_line = guard.clone(); }
        if progress_line.as_deref() == Some("Scan complete") {
            if let Some(dir) = uncp::paths::default_cache_dir() {
                let _ = detector.load_cache_all(dir);
                pres = PresentationState::from_detector(&detector).with_status("Scan complete");
            }
            progress_line = None;
        }
        // Spinner effect while progress is active
        let progress_display = if let Some(ref p) = progress_line {
            spinner_idx = (spinner_idx + 1) % SPINNER.len();
            Some(format!("{} {}", SPINNER[spinner_idx], p))
        } else { None };
        terminal.draw(|f| draw(f, &pres, in_path_input, &input_buffer, progress_display.as_deref()))?;
    }
}

#[derive(Debug, Clone, Copy)]
enum Action { Quit, Refresh, Scan, Hash, EnterPathMode, CancelPath, SubmitPath }

fn draw(frame: &mut Frame, pres: &PresentationState, in_input: bool, input: &str, progress: Option<&str>) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),   // header
            Constraint::Min(5),      // body
            Constraint::Length(2),   // footer
        ])
        .split(frame.area());

    // Header with key bindings
    let header = Paragraph::new(Line::from(vec![
        Span::styled("uncp TUI", Style::default().fg(Color::Cyan)),
        Span::raw("  |  Total: "),
        Span::raw(pres.total_files.to_string()),
        Span::raw("  Pending: "),
        Span::raw(pres.pending_hash.to_string()),
        Span::raw("  |  Keys: q quit, r refresh, s scan, h hash, p path"),
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

    // Footer / status with input/progress
    let mut status = pres.status.clone();
    if let Some(p) = progress {
        status = if status.is_empty() { p.to_string() } else { format!("{} | {}", status, p) };
    }
    if in_input {
        status = if status.is_empty() {
            format!("Path: {}", input)
        } else {
            format!("{} | Path: {}", status, input)
        };
    }
    let footer = Paragraph::new(status).block(Block::default().borders(Borders::ALL).title("Status"));
    frame.render_widget(footer, chunks[2]);
}

fn handle_events(in_input: bool, input_buffer: &mut String) -> std::io::Result<Option<Action>> {
    if event::poll(Duration::from_millis(100))? {
        if let Event::Key(key) = event::read()? {

            if in_input {
                match key.code {
                    KeyCode::Esc => return Ok(Some(Action::CancelPath)),
                    KeyCode::Enter => return Ok(Some(Action::SubmitPath)),
                    KeyCode::Backspace => { input_buffer.pop(); return Ok(None); }
                    KeyCode::Char(c) => { input_buffer.push(c); return Ok(None); }
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
