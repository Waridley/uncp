//! Telemetry utilities: a tracing layer that captures error events into a bounded queue

use left_right::{Absorb, ReadHandleFactory};
use ringbuf::traits::{Consumer, Observer, RingBuffer};
use smallvec::SmallVec;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::Sender;
use std::time::SystemTime;
use tracing::level_filters::LevelFilter;
use tracing::{Event, Level, Subscriber, warn};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

pub struct Rb(ringbuf::HeapRb<UiLogEvent>);

impl Rb {
	pub fn new(capacity: usize) -> Self {
		Self(ringbuf::HeapRb::new(capacity))
	}
}

impl Deref for Rb {
	type Target = ringbuf::HeapRb<UiLogEvent>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Rb {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl Clone for Rb {
	fn clone(&self) -> Self {
		let mut new = Self::new(self.0.capacity().get());
		new.push_iter_overwrite(self.iter().cloned());
		new
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelToggles {
	pub error: bool,
	pub warn: bool,
	pub info: bool,
	pub debug: bool,
	pub trace: bool,
}

impl Default for LevelToggles {
	fn default() -> Self {
		Self::from_level(Level::INFO)
	}
}

impl LevelToggles {
	pub fn from_level(level: Level) -> Self {
		Self {
			error: level >= Level::ERROR,
			warn: level >= Level::WARN,
			info: level >= Level::INFO,
			debug: level >= Level::DEBUG,
			trace: level >= Level::TRACE,
		}
	}

	pub fn from_level_filter(level: LevelFilter) -> Self {
		Self {
			error: level < LevelFilter::ERROR,
			warn: level < LevelFilter::WARN,
			info: level < LevelFilter::INFO,
			debug: level < LevelFilter::DEBUG,
			trace: level < LevelFilter::TRACE,
		}
	}

	pub fn toggle(&mut self, level: Level) {
		match level {
			Level::ERROR => self.error = !self.error,
			Level::WARN => self.warn = !self.warn,
			Level::INFO => self.info = !self.info,
			Level::DEBUG => self.debug = !self.debug,
			Level::TRACE => self.trace = !self.trace,
		}
	}

	pub fn set(&mut self, level: Level, enabled: bool) {
		match level {
			Level::ERROR => self.error = enabled,
			Level::WARN => self.warn = enabled,
			Level::INFO => self.info = enabled,
			Level::DEBUG => self.debug = enabled,
			Level::TRACE => self.trace = enabled,
		}
	}

	pub fn enable(&mut self, level: Level) {
		self.set(level, true);
	}

	pub fn disable(&mut self, level: Level) {
		self.set(level, false);
	}

	pub fn should_show(&self, level: Level) -> bool {
		match level {
			Level::ERROR => self.error,
			Level::WARN => self.warn,
			Level::INFO => self.info,
			Level::DEBUG => self.debug,
			Level::TRACE => self.trace,
		}
	}

	pub fn filter<'e>(&self, event: Event<'e>) -> Option<Event<'e>> {
		self.should_show(*event.metadata().level()).then_some(event)
	}
}

impl From<Level> for LevelToggles {
	fn from(value: Level) -> Self {
		Self::from_level(value)
	}
}

impl From<LevelFilter> for LevelToggles {
	fn from(value: LevelFilter) -> Self {
		Self::from_level_filter(value)
	}
}

type EventBufReader = ReadHandleFactory<Rb>;
type EventBufWriter = left_right::WriteHandle<Rb, EventBufOp>;

enum EventBufOp {
	Push(UiLogEvent),
}

impl Absorb<EventBufOp> for Rb {
	fn absorb_first(&mut self, operation: &mut EventBufOp, _other: &Self) {
		match operation {
			EventBufOp::Push(event) => {
				self.push_overwrite(event.clone());
			}
		}
	}

	fn sync_with(&mut self, first: &Self) {
		self.clear();
		self.push_iter_overwrite(first.iter().cloned());
	}
}

#[derive(Debug, Clone)]
struct EventBufReaders {
	pub trace: EventBufReader,
	pub debug: EventBufReader,
	pub info: EventBufReader,
	pub warn: EventBufReader,
	pub error: EventBufReader,
}

struct EventBufWriters {
	trace: EventBufWriter,
	debug: EventBufWriter,
	info: EventBufWriter,
	warn: EventBufWriter,
	error: EventBufWriter,
}

fn new_event_bufs(capacity: usize) -> (EventBufWriters, EventBufReaders) {
	let (trace_w, trace_r) = left_right::new_from_empty::<_, EventBufOp>(Rb::new(capacity));
	let (debug_w, debug_r) = left_right::new_from_empty::<_, EventBufOp>(Rb::new(capacity));
	let (info_w, info_r) = left_right::new_from_empty::<_, EventBufOp>(Rb::new(capacity));
	let (warn_w, warn_r) = left_right::new_from_empty::<_, EventBufOp>(Rb::new(capacity));
	let (error_w, error_r) = left_right::new_from_empty::<_, EventBufOp>(Rb::new(capacity));
	(
		EventBufWriters {
			trace: trace_w,
			debug: debug_w,
			info: info_w,
			warn: warn_w,
			error: error_w,
		},
		EventBufReaders {
			trace: trace_r.factory(),
			debug: debug_r.factory(),
			info: info_r.factory(),
			warn: warn_r.factory(),
			error: error_r.factory(),
		},
	)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UiLogEvent {
	pub t: SystemTime,
	pub level: Level,
	pub target: String,
	pub message: String,
	pub fields: Vec<(&'static str, String)>,
}

#[derive(Debug, Clone)]
pub struct UiLogQueueHandle {
	readers: EventBufReaders,
}

impl UiLogQueueHandle {
	pub fn capacity(&self, level: Level) -> usize {
		self.readers.capacity(level)
	}
}

#[derive(Debug)]
pub struct UiLogLayer {
	/// Sends the events to the intermediary thread to eventually be written to the UI buffers.
	tx: Sender<UiLogEvent>,
}

impl EventBufReaders {
	pub fn collect_into_filtered<T: Extend<UiLogEvent>>(
		&self,
		levels: LevelToggles,
		destination: &mut T,
	) {
		let bufs = [
			levels.trace.then(|| self.trace.handle()),
			levels.debug.then(|| self.debug.handle()),
			levels.info.then(|| self.info.handle()),
			levels.warn.then(|| self.warn.handle()),
			levels.error.then(|| self.error.handle()),
		];

		let bufs = bufs
			.iter()
			.filter_map(|h| h.as_ref().and_then(|h| h.enter()))
			.collect::<SmallVec<[_; 5]>>();

		let mut bufs = bufs
			.iter()
			.map(|h| {
				let mut iter = h.iter().peekable();
				let next = iter.next();
				(iter, next)
			})
			.collect::<SmallVec<[_; 5]>>();

		destination.extend(std::iter::from_fn(move || {
			bufs.iter_mut()
				.filter(|(_, next)| next.is_some())
				.min_by_key(|(_iter, next)| next.map(|ev| ev.t).unwrap_or_else(SystemTime::now)) // now should be later than any previous events
				.map(|(iter, next)| {
					std::mem::replace(next, iter.next())
						.expect("`next` was the minimum, it must exist")
				})
				.cloned()
		}));
	}

	pub fn capacity(&self, level: Level) -> usize {
		match level {
			Level::TRACE => self
				.trace
				.handle()
				.enter()
				.map(|buf| buf.capacity().get())
				.unwrap_or(0),
			Level::DEBUG => self
				.debug
				.handle()
				.enter()
				.map(|buf| buf.capacity().get())
				.unwrap_or(0),
			Level::INFO => self
				.info
				.handle()
				.enter()
				.map(|buf| buf.capacity().get())
				.unwrap_or(0),
			Level::WARN => self
				.warn
				.handle()
				.enter()
				.map(|buf| buf.capacity().get())
				.unwrap_or(0),
			Level::ERROR => self
				.error
				.handle()
				.enter()
				.map(|buf| buf.capacity().get())
				.unwrap_or(0),
		}
	}
}

impl UiLogQueueHandle {
	pub fn collect_into_filtered<T: Extend<UiLogEvent>>(
		&self,
		levels: LevelToggles,
		destination: &mut T,
	) {
		self.readers.collect_into_filtered(levels, destination)
	}

	pub fn with_last_error<F, U>(&self, f: F) -> Option<U>
	where
		F: FnOnce(&UiLogEvent) -> U,
	{
		let handle = self.readers.error.handle();
		handle.enter().as_ref().and_then(|h| h.last()).map(f)
	}
}

impl UiLogLayer {
	pub fn new(capacity: usize) -> (Self, UiLogQueueHandle) {
		let (tx, rx) = std::sync::mpsc::channel::<UiLogEvent>();
		let (mut w, r) = new_event_bufs(capacity);
		std::thread::spawn(move || {
			loop {
				for event in rx.try_iter() {
					match event.level {
						Level::TRACE => {
							w.trace.append(EventBufOp::Push(event));
						}
						Level::DEBUG => {
							w.debug.append(EventBufOp::Push(event));
						}
						Level::INFO => {
							w.info.append(EventBufOp::Push(event));
						}
						Level::WARN => {
							w.warn.append(EventBufOp::Push(event));
						}
						Level::ERROR => {
							w.error.append(EventBufOp::Push(event));
						}
					}
				}
				w.trace.publish();
				w.debug.publish();
				w.info.publish();
				w.warn.publish();
				w.error.publish();
				std::thread::yield_now();
			}
		});
		let handle = UiLogQueueHandle { readers: r };
		(UiLogLayer { tx }, handle)
	}
}

struct MsgVisitor {
	message: Option<String>,
	fields: Vec<(&'static str, String)>,
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
	fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
		if field.name() == "message" {
			self.message = Some(value.to_string());
		} else {
			self.fields.push((field.name(), value.to_string()));
		}
	}

	fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
		let v = format!("{:?}", value);
		if field.name() == "message" {
			self.message = Some(v);
		} else {
			self.fields.push((field.name(), v));
		}
	}
}

impl<S> Layer<S> for UiLogLayer
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
	fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
		let meta = event.metadata();
		if ctx.enabled(meta) {
			let mut v = MsgVisitor::new();
			event.record(&mut v);
			let message = v.message.unwrap_or_default();
			let item = UiLogEvent {
				t: SystemTime::now(),
				level: *meta.level(),
				target: meta.target().to_string(),
				message,
				fields: v.fields,
			};
			if self.tx.send(item).is_err() {
				// don't try logging when logging is what failed
			}
		}
	}
}

#[derive(Debug, Clone, Default)]
pub struct LogDedup {
	list: Vec<DedupedEvent>,
}

impl LogDedup {
	pub fn new() -> Self {
		Self { list: Vec::new() }
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			list: Vec::with_capacity(capacity),
		}
	}

	pub fn clear(&mut self) {
		self.list.clear();
	}

	pub fn len(&self) -> usize {
		self.list.len()
	}

	pub fn is_empty(&self) -> bool {
		self.list.is_empty()
	}

	pub fn iter(&self) -> impl Iterator<Item = DedupedEvent> {
		self.list.iter().cloned()
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupedEvent {
	pub level: Level,
	pub target: String,
	pub message: String,
	pub times: SmallVec<[EventInstance; 1]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventInstance {
	pub t: SystemTime,
	pub fields: Vec<(&'static str, String)>,
}

impl EventInstance {
	pub fn new(t: SystemTime, fields: impl Into<Vec<(&'static str, String)>>) -> Self {
		Self {
			t,
			fields: fields.into(),
		}
	}
}

impl<Fields: Into<Vec<(&'static str, String)>>> From<(SystemTime, Fields)> for EventInstance {
	fn from(value: (SystemTime, Fields)) -> Self {
		Self::new(value.0, value.1)
	}
}

impl PartialEq<DedupedEvent> for UiLogEvent {
	fn eq(&self, other: &DedupedEvent) -> bool {
		// Ignore time and fields for compactness.
		// Fields might be incorrect to ignore, can be changed or made togglable if it becomes a problem
		self.level == other.level && self.target == other.target && self.message == other.message
	}
}

impl From<UiLogEvent> for DedupedEvent {
	fn from(value: UiLogEvent) -> Self {
		Self {
			level: value.level,
			target: value.target,
			message: value.message,
			times: SmallVec::from([(value.t, value.fields).into()]),
		}
	}
}

impl Extend<UiLogEvent> for LogDedup {
	fn extend<T: IntoIterator<Item = UiLogEvent>>(&mut self, iter: T) {
		for ev in iter {
			let dedup = self.list.last().map(|last| ev == *last).unwrap_or(false);
			if dedup {
				self.list
					.last_mut()
					.unwrap()
					.times
					.push((ev.t, ev.fields).into());
			} else {
				self.list.push(ev.into());
			}
		}
	}
}

/// Attempt to install the UI log layer atop the default registry. If a subscriber is
/// already installed, this will no-op and just return a handle with a live queue only if
/// installation succeeded; otherwise it will still return a handle but it won't receive events.
pub fn install_ui_log_layer(capacity: usize) -> UiLogQueueHandle {
	let (layer, handle) = UiLogLayer::new(capacity);
	if let Err(e) = tracing_subscriber::registry().with(layer).try_init() {
		warn!("Failed to install UI log layer: {e}")
	}
	handle
}
