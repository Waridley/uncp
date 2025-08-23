//! Event-to-queue adapter system: consumes RequestContentLoad events and enqueues them.

use async_trait::async_trait;
use std::sync::Arc;
use tracing::info;

use crate::content_queue::ContentLoadQueue;
use crate::error::SystemResult;
use crate::events::{EventBus, SystemEvent};
use crate::pool::DataPool;
use crate::systems::{System, SystemRunner};

pub struct RequestToQueueAdapter {
    pub bus: Arc<EventBus>,
    pub queue: std::sync::Arc<std::sync::Mutex<ContentLoadQueue>>, // redundant with pool.load_queue but fine for DI
}

impl RequestToQueueAdapter {
    pub fn new(bus: Arc<EventBus>, queue: std::sync::Arc<std::sync::Mutex<ContentLoadQueue>>) -> Self {
        Self { bus, queue }
    }
}

#[async_trait]
impl SystemRunner for RequestToQueueAdapter {
    async fn run(&self, pool: Arc<DataPool>) -> SystemResult<()> {
        let rx = self.bus.subscribe();
        loop {
            if !pool.is_running() { break; }
            let mut count = 0usize;
            while let Ok(ev) = rx.try_recv() {
                if let SystemEvent::RequestContentLoad { path } = ev {
                    let mut q = self.queue.lock().unwrap();
                    q.enqueue(path);
                    count += 1;
                }
            }
            if count > 0 { info!("RequestToQueueAdapter enqueued {} items", count); }
            smol::future::yield_now().await;
        }
        Ok(())
    }

    fn name(&self) -> &'static str { "RequestToQueueAdapter" }
}

impl System for RequestToQueueAdapter {
    fn required_columns(&self) -> &[&'static str] { &[] }
    fn optional_columns(&self) -> &[&'static str] { &[] }
    fn description(&self) -> &'static str { "Converts RequestContentLoad events into queue entries" }
}
