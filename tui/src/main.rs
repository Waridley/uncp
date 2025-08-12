use ratatui::widgets::Paragraph;
use ratatui::Frame;

fn main() -> std::io::Result<()> {
	let mut terminal = ratatui::init();
	let result = run(&mut terminal);
	ratatui::restore();
	result
}

fn run(terminal: &mut ratatui::DefaultTerminal) -> std::io::Result<()> {
	loop {
		terminal.draw(draw)?;
		if handle_events()? {
			break Ok(());
		}
	}
}

fn draw(frame: &mut Frame) {
	let text = Paragraph::new("Hello World!");
	frame.render_widget(text, frame.area());
}

fn handle_events() -> std::io::Result<bool> {
	use crossterm::event::{self, Event, KeyCode};
	if event::poll(std::time::Duration::from_millis(100))? {
		if let Event::Key(key) = event::read()? {
			if let KeyCode::Char('q') = key.code {
				return Ok(true);
			}
		}
	}
	Ok(false)
}
