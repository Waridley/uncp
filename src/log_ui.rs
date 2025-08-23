//! Telemetry utilities: a tracing layer that captures error events into a bounded queue

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*; // brings SubscriberExt::with into scope

#[derive(Debug, Clone)]
pub struct UiErrorEvent {
	pub ts_unix_ms: u128,
	pub level: Level,
	pub target: String,
	pub message: String,
}

#[derive(Clone)]
pub struct UiErrorQueueHandle {
	queue: Arc<Mutex<VecDeque<UiErrorEvent>>>,
}

impl UiErrorQueueHandle {
	pub fn last(&self) -> Option<UiErrorEvent> {
		self.queue.lock().ok().and_then(|q| q.back().cloned())
	}
	#[allow(dead_code)]
	pub fn take_all(&self) -> Vec<UiErrorEvent> {
		if let Ok(mut q) = self.queue.lock() {
			let v: Vec<_> = q.drain(..).collect();
			v
		} else {
			Vec::new()
		}
	}
}

pub struct UiErrorLayer {
	queue: Arc<Mutex<VecDeque<UiErrorEvent>>>,
	capacity: usize,
	min_level: Level,
}

impl UiErrorLayer {
	pub fn new(capacity: usize) -> (Self, UiErrorQueueHandle) {
		let queue = Arc::new(Mutex::new(VecDeque::with_capacity(capacity)));
		let handle = UiErrorQueueHandle {
			queue: queue.clone(),
		};
		(
			Self {
				queue,
				capacity,
				min_level: Level::ERROR,
			},
			handle,
		)
	}
}

pub fn with_min_level(capacity: usize, min_level: Level) -> (UiErrorLayer, UiErrorQueueHandle) {
	let queue = Arc::new(Mutex::new(VecDeque::with_capacity(capacity)));
	let handle = UiErrorQueueHandle {
		queue: queue.clone(),
	};
	(
		UiErrorLayer {
			queue,
			capacity,
			min_level,
		},
		handle,
	)
}

struct MsgVisitor {
	message: Option<String>,
	fields: Vec<(String, String)>,
}

impl MsgVisitor {
	fn new() -> Self {
		Self {
			message: None,
			fields: Vec::new(),
		}
	}
}

impl tracing::field::Visit for MsgVisitor {
	fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
		let v = format!("{:?}", value);
		if field.name() == "message" {
			self.message = Some(v);
		} else {
			self.fields.push((field.name().to_string(), v));
		}
	}
	fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
		if field.name() == "message" {
			self.message = Some(value.to_string());
		} else {
			self.fields
				.push((field.name().to_string(), value.to_string()));
		}
	}
}

impl<S> Layer<S> for UiErrorLayer
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
	fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
		let meta = event.metadata();
		if meta.level() <= &self.min_level {
			let mut v = MsgVisitor::new();
			event.record(&mut v);
			let message = v.message.unwrap_or_else(|| {
				v.fields
					.iter()
					.map(|(k, val)| format!("{}={}", k, val))
					.collect::<Vec<_>>()
					.join(" ")
			});
			let now = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.unwrap_or_default();
			let item = UiErrorEvent {
				ts_unix_ms: (now.as_millis()),
				level: *meta.level(),
				target: meta.target().to_string(),
				message,
			};
			if let Ok(mut q) = self.queue.lock() {
				if q.len() >= self.capacity {
					let _ = q.pop_front();
				}
				q.push_back(item);
			}
		}
	}
}

/// Attempt to install the UI error layer atop the default registry. If a subscriber is
/// already installed, this will no-op and just return a handle with a live queue only if
/// installation succeeded; otherwise it will still return a handle but it won't receive events.
pub fn install_ui_error_layer(capacity: usize) -> UiErrorQueueHandle {
	let (layer, handle) = UiErrorLayer::new(capacity);
	// Try to install if not already set; ignore error if a subscriber exists.
	let _ = tracing_subscriber::registry().with(layer).try_init();
	handle
}
