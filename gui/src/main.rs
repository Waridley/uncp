#![cfg_attr(
	all(not(debug_assertions), target_os = "windows"),
	windows_subsystem = "windows"
)]

use iced::widget::{column, text};
use uncp::ui::{Action, PresentationState};

fn main() -> iced::Result {
	iced::run("uncp", update, view)
}

fn update(state: &mut GuiState, action: Action) {
	match action {
		Action::Quit => {}
		Action::Refresh => {}
		Action::Scan => {}
		Action::Hash => {}
		Action::ShowPathInput => {}
		Action::ShowFilterInput => {}
		Action::ToggleLogView => {}
		Action::ClosePopup => {}
		Action::SubmitPath => {}
		Action::SubmitFilter => {}
		Action::FilterSwitchColumn => {}
		Action::Up => {}
		Action::Down => {}
		Action::PageUp => {}
		Action::PageDown => {}
		Action::Home => {}
		Action::End => {}
		Action::SelectRow(_, _) => {}
		Action::Resize(_, _) => {}
		Action::ToggleLogLevel(_) => {}
		Action::LogScrollUp => {}
		Action::LogScrollDown => {}
		Action::LogClear => {}
	}
}

fn view(state: &'_ GuiState) -> iced::Element<'_, Action> {
	column![text("Hello, world!"),].into()
}

#[derive(Debug, Default)]
struct GuiState {
	app_state: uncp::ui::AppState,
	presentation_state: PresentationState,
}
