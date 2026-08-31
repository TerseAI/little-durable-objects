use std::sync::Mutex;

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use super::{ActorTelemetry, ActorTelemetryEvent};

#[derive(Default)]
pub(crate) struct LocalActorTelemetry {
    events: Mutex<Vec<ActorTelemetryEvent>>,
}

impl LocalActorTelemetry {
    pub(crate) fn events(&self) -> Result<Vec<ActorTelemetryEvent>> {
        Ok(self
            .events
            .lock()
            .map_err(|_| anyhow!("local actor telemetry lock poisoned"))?
            .clone())
    }
}

#[async_trait]
impl ActorTelemetry for LocalActorTelemetry {
    fn publish(&self, event: ActorTelemetryEvent) {
        if let Ok(mut events) = self.events.lock() {
            events.push(event);
        }
    }
}
