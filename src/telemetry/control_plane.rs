use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::warn;

use crate::control_plane::ControlPlaneClient;

use super::{ActorTelemetry, ActorTelemetryEvent};

const QUEUE_CAPACITY: usize = 10_000;
pub(crate) const MAX_FORWARDED_BATCH_EVENTS: usize = 100;
const FLUSH_EVERY: Duration = Duration::from_secs(1);

/// Credential-free telemetry used inside actor-host sandboxes. Events cross the already
/// authenticated control-plane connection; only the trusted control plane holds the
/// PostHog API key and it replaces every event's namespace scope from the JWT.
pub(crate) struct ControlPlaneActorTelemetry {
    sender: mpsc::Sender<ActorTelemetryEvent>,
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    dropped: Arc<AtomicU64>,
}

impl ControlPlaneActorTelemetry {
    pub(crate) fn new(client: ControlPlaneClient) -> Self {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker = tokio::spawn(run_worker(client, receiver, shutdown_rx, dropped.clone()));
        Self {
            sender,
            shutdown,
            worker: Mutex::new(Some(worker)),
            dropped,
        }
    }
}

#[async_trait]
impl ActorTelemetry for ControlPlaneActorTelemetry {
    fn publish(&self, event: ActorTelemetryEvent) {
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dropped_events(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    async fn shutdown(&self, timeout: Duration) -> Result<()> {
        let _ = self.shutdown.send(true);
        let worker = self
            .worker
            .lock()
            .map_err(|_| anyhow::anyhow!("control-plane telemetry worker lock poisoned"))?
            .take();
        if let Some(mut worker) = worker {
            match tokio::time::timeout(timeout, &mut worker).await {
                Ok(result) => result.context("control-plane telemetry worker failed")?,
                Err(_) => {
                    worker.abort();
                    let _ = worker.await;
                }
            }
        }
        Ok(())
    }
}

async fn run_worker(
    client: ControlPlaneClient,
    mut receiver: mpsc::Receiver<ActorTelemetryEvent>,
    mut shutdown: watch::Receiver<bool>,
    dropped: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(FLUSH_EVERY);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut batch = Vec::with_capacity(MAX_FORWARDED_BATCH_EVENTS);
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    drain_and_flush(&client, &mut receiver, &mut batch, &dropped).await;
                    return;
                }
            }
            event = receiver.recv() => match event {
                Some(event) => {
                    batch.push(event);
                    if batch.len() >= MAX_FORWARDED_BATCH_EVENTS {
                        flush(&client, &mut batch, &dropped).await;
                    }
                }
                None => {
                    drain_and_flush(&client, &mut receiver, &mut batch, &dropped).await;
                    return;
                }
            },
            _ = interval.tick() => flush(&client, &mut batch, &dropped).await,
        }
    }
}

async fn drain_and_flush(
    client: &ControlPlaneClient,
    receiver: &mut mpsc::Receiver<ActorTelemetryEvent>,
    batch: &mut Vec<ActorTelemetryEvent>,
    dropped: &AtomicU64,
) {
    loop {
        while batch.len() < MAX_FORWARDED_BATCH_EVENTS {
            match receiver.try_recv() {
                Ok(event) => batch.push(event),
                Err(_) => break,
            }
        }
        if batch.is_empty() {
            return;
        }
        flush(client, batch, dropped).await;
    }
}

async fn flush(
    client: &ControlPlaneClient,
    batch: &mut Vec<ActorTelemetryEvent>,
    dropped: &AtomicU64,
) {
    if batch.is_empty() {
        return;
    }
    let events = std::mem::take(batch);
    let event_count = events.len();
    if let Err(error) = client.publish_telemetry_batch(events).await {
        dropped.fetch_add(event_count as u64, Ordering::Relaxed);
        warn!(
            event_count,
            error = %format!("{error:#}"),
            "could not forward actor telemetry batch"
        );
    }
}
