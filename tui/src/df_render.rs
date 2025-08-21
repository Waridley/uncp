use polars::prelude::*;
use ratatui::layout::Constraint;
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

#[derive(Default)]
pub struct RenderOptions<'a> {
	pub column_constraints: Option<Vec<Constraint>>,
	pub byte_size_cols: &'a [&'a str],
	pub title: Option<String>,
}

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

pub fn build_table<'a>(df: &DataFrame, opts: &RenderOptions<'a>) -> Table<'a> {
	let col_names: Vec<String> = df
		.get_column_names()
		.into_iter()
		.map(|s| s.to_string())
		.collect();

	// Header row from column names
	let header = Row::new(col_names.iter().map(|n| Cell::from(n.clone())))
		.style(Style::default().add_modifier(Modifier::BOLD));

	// Rows: iterate over height and build cells per column
	let height = df.height();
	let mut rows: Vec<Row> = Vec::with_capacity(height);

	for row_idx in 0..height {
		let mut cells: Vec<Cell> = Vec::with_capacity(col_names.len());
		for name in &col_names {
			let s = df.column(name).expect("column must exist");
			let av = s.get(row_idx).unwrap_or(AnyValue::Null);
			let text = if matches!(s.dtype(), DataType::Boolean) {
				match av {
					AnyValue::Boolean(b) => {
						if b {
							"✓".to_string()
						} else {
							"○".to_string()
						}
					}
					_ => av.to_string(),
				}
			} else if opts.byte_size_cols.contains(&name.as_str()) {
				// Format integer-like values as human-readable bytes
				match av {
					AnyValue::UInt64(v) => format_bytes(v),
					AnyValue::UInt32(v) => format_bytes(v as u64),
					AnyValue::Int64(v) if v >= 0 => format_bytes(v as u64),
					AnyValue::Int32(v) if v >= 0 => format_bytes(v as u64),
					_ => av.to_string(),
				}
			} else {
				av.to_string()
			};
			cells.push(Cell::from(text));
		}
		rows.push(Row::new(cells));
	}

	// Column width constraints
	let constraints: Vec<Constraint> = if let Some(c) = &opts.column_constraints {
		c.clone()
	} else {
		let n = col_names.len().max(1) as u16;
		let pct = 100 / n; // floor; last col will share the remainder
		(0..col_names.len())
			.map(|_| Constraint::Percentage(pct))
			.collect()
	};

	let mut table = Table::new(rows, constraints)
		.header(header)
		.row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

	if let Some(title) = &opts.title {
		// Pass owned title to avoid tying table lifetime to opts
		table = table.block(Block::default().borders(Borders::ALL).title(title.clone()));
	} else {
		table = table.block(Block::default().borders(Borders::ALL));
	}

	table
}
