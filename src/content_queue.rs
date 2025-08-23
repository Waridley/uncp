//! In-memory content load request queue with simple de-duplication.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use crate::paths::DirEntryId;

#[derive(Default)]
pub struct ContentLoadQueue {
	/// FIFO of pending requests
	queue: VecDeque<DirEntryId>,
	/// Set to avoid enqueuing duplicates
	seen: HashSet<(usize, usize)>,
}

impl ContentLoadQueue {
	pub fn new() -> Arc<std::sync::Mutex<Self>> {
		Arc::new(std::sync::Mutex::new(Self::default()))
	}

	pub fn len(&self) -> usize {
		self.queue.len()
	}
	pub fn is_empty(&self) -> bool {
		self.queue.is_empty()
	}

	/// Enqueue a single id if not already present.
	pub fn enqueue(&mut self, id: DirEntryId) {
		let key = id.raw_parts();
		if self.seen.insert(key) {
			self.queue.push_back(id);
		}
	}

	/// Enqueue many ids.
	pub fn enqueue_many(&mut self, ids: impl IntoIterator<Item = DirEntryId>) {
		for id in ids {
			self.enqueue(id);
		}
	}

	/// Snapshot the current queue as a vector (does not clear). Useful for systems that
	/// want to reprioritize items using external metadata.
	pub fn snapshot(&self) -> Vec<DirEntryId> {
		self.queue.iter().copied().collect()
	}

	/// Remove a specific id from the queue and seen set.
	pub fn remove(&mut self, id: &DirEntryId) {
		let key = id.raw_parts();
		if self.seen.remove(&key) {
			if let Some(pos) = self.queue.iter().position(|x| x == id) {
				self.queue.remove(pos);
			}
		}
	}

	/// Pop from the front (fallback if no prioritization is applied).
	pub fn pop_front(&mut self) -> Option<DirEntryId> {
		if let Some(id) = self.queue.pop_front() {
			let _ = self.seen.remove(&id.raw_parts());
			Some(id)
		} else {
			None
		}
	}
}
