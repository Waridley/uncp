//! Shared, thread-safe data pool for systems

use std::sync::{Arc, RwLock, Mutex, atomic::{AtomicBool, Ordering}};

use crate::{data::ScanState, memory::MemoryManager, relations::RelationStore};
use crate::content_queue::ContentLoadQueue;
use crate::events::EventBus;

#[derive(Clone)]
pub struct DataPool {
    pub state: Arc<RwLock<ScanState>>,
    pub relations: Arc<RwLock<RelationStore>>,
    pub memory: Arc<RwLock<MemoryManager>>,
    pub bus: Arc<EventBus>,
    pub load_queue: Arc<Mutex<ContentLoadQueue>>,
    pub running: Arc<AtomicBool>,
}

impl DataPool {
    pub fn new(state: ScanState, relations: RelationStore, memory: MemoryManager, bus: Arc<EventBus>, load_queue: Arc<Mutex<ContentLoadQueue>>) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            relations: Arc::new(RwLock::new(relations)),
            memory: Arc::new(RwLock::new(memory)),
            bus,
            load_queue,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn from_arcs(state: Arc<RwLock<ScanState>>, relations: Arc<RwLock<RelationStore>>, memory: Arc<RwLock<MemoryManager>>, bus: Arc<EventBus>, load_queue: Arc<Mutex<ContentLoadQueue>>) -> Self {
        Self { state, relations, memory, bus, load_queue, running: Arc::new(AtomicBool::new(true)) }
    }

    pub fn stop(&self) { self.running.store(false, Ordering::Relaxed); }
    pub fn is_running(&self) -> bool { self.running.load(Ordering::Relaxed) }
}

