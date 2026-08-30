use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use reqwest::{Client, Url};
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::warn;

use super::{ActorTelemetry, ActorTelemetryEvent};

const QUEUE_CAPACITY: usize = 10_000;
const MAX_BATCH_EVENTS: usize = 100;
const FLUSH_EVERY: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct PostHogActorTelemetry {
    sender: mpsc::Sender<ActorTelemetryEvent>,
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    dropped: Arc<AtomicU64>,
}

struct PostHogDelivery {
    client: Client,
    endpoint: Url,
    api_key: String,
    environment: String,
    region: String,
    dropped: Arc<AtomicU64>,
}

impl PostHogActorTelemetry {
    pub(super) fn new(
        api_key: String,
        host: String,
        environment: String,
        region: String,
    ) -> Result<Self> {
        ensure!(!api_key.is_empty(), "PostHog API key must not be empty");
        let endpoint = Url::parse(&format!("{}/batch/", host.trim_end_matches('/')))
            .context("POSTHOG_HOST must be a valid URL")?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https"),
            "POSTHOG_HOST must use HTTP or HTTPS"
        );
        let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let dropped = Arc::new(AtomicU64::new(0));
        let delivery = PostHogDelivery {
            client,
            endpoint,
            api_key,
            environment,
            region,
            dropped: dropped.clone(),
        };
        let worker = tokio::spawn(run_worker(delivery, receiver, shutdown_rx));
        Ok(Self {
            sender,
            shutdown,
            worker: Mutex::new(Some(worker)),
            dropped,
        })
    }
}

#[async_trait]
impl ActorTelemetry for PostHogActorTelemetry {
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
            .map_err(|_| anyhow::anyhow!("PostHog telemetry worker lock poisoned"))?
            .take();
        if let Some(mut worker) = worker {
            match tokio::time::timeout(timeout, &mut worker).await {
                Ok(result) => result.context("PostHog telemetry worker failed")?,
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
    delivery: PostHogDelivery,
    mut receiver: mpsc::Receiver<ActorTelemetryEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(FLUSH_EVERY);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut batch = Vec::with_capacity(MAX_BATCH_EVENTS);
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    delivery.drain_and_flush(&mut receiver, &mut batch).await;
                    return;
                }
            }
            event = receiver.recv() => match event {
                Some(event) => {
                    batch.push(event);
                    if batch.len() >= MAX_BATCH_EVENTS {
                        delivery.flush(&mut batch).await;
                    }
                }
                None => {
                    delivery.drain_and_flush(&mut receiver, &mut batch).await;
                    return;
                }
            },
            _ = interval.tick() => {
                delivery.flush(&mut batch).await;
            }
        }
    }
}

impl PostHogDelivery {
    async fn drain_and_flush(
        &self,
        receiver: &mut mpsc::Receiver<ActorTelemetryEvent>,
        batch: &mut Vec<ActorTelemetryEvent>,
    ) {
        loop {
            while batch.len() < MAX_BATCH_EVENTS {
                match receiver.try_recv() {
                    Ok(event) => batch.push(event),
                    Err(_) => break,
                }
            }
            if batch.is_empty() {
                return;
            }
            self.flush(batch).await;
        }
    }

    async fn flush(&self, batch: &mut Vec<ActorTelemetryEvent>) {
        if batch.is_empty() {
            return;
        }
        let events = std::mem::take(batch);
        let event_count = events.len();
        let payloads = events
            .iter()
            .map(|event| posthog_event(event, &self.environment, &self.region))
            .collect::<Result<Vec<_>>>();
        let payloads = match payloads {
            Ok(payloads) => payloads,
            Err(error) => {
                self.dropped
                    .fetch_add(event_count as u64, Ordering::Relaxed);
                warn!(error = %format!("{error:#}"), "could not serialize actor telemetry batch");
                return;
            }
        };
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&json!({ "api_key": self.api_key, "batch": payloads }))
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => {
                self.dropped
                    .fetch_add(event_count as u64, Ordering::Relaxed);
                warn!(status = %response.status(), "PostHog rejected actor telemetry batch")
            }
            Err(error) => {
                self.dropped
                    .fetch_add(event_count as u64, Ordering::Relaxed);
                warn!(error = %error, "could not deliver actor telemetry batch to PostHog")
            }
        }
    }
}

fn posthog_event(event: &ActorTelemetryEvent, environment: &str, region: &str) -> Result<Value> {
    let mut properties = event.posthog_properties(environment, region)?;
    let distinct_id = event
        .scope()
        .namespace_id
        .as_ref()
        .map(|namespace| format!("durable-object-namespace:{namespace}"))
        .unwrap_or_else(|| format!("actor-system:{}", event.role().as_str()));
    properties.insert("distinct_id".into(), distinct_id.into());
    properties.insert("$process_person_profile".into(), false.into());
    Ok(json!({
        "event": event.name(),
        "properties": properties,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{
        ActorExecutionKind, ActorExecutionTelemetry, ActorSystemRole, ActorTelemetryScope,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn serializes_namespace_events_without_creating_people() -> Result<()> {
        let event = execution_event();

        let payload = posthog_event(&event, "test", "local")?;
        assert_eq!(payload["event"], "actor_execution_finished");
        assert_eq!(
            payload["properties"]["distinct_id"],
            "durable-object-namespace:namespace-1"
        );
        assert_eq!(payload["properties"]["$process_person_profile"], false);
        assert!(payload["properties"].get("$groups").is_none());
        Ok(())
    }

    #[test]
    fn derives_the_system_identity_from_the_event_role() -> Result<()> {
        let mut event = execution_event();
        let ActorTelemetryEvent::ActorExecutionFinished(execution) = &mut event else {
            unreachable!("execution_event returned another event type")
        };
        execution.scope = ActorTelemetryScope::default();

        let payload = posthog_event(&event, "test", "local")?;

        assert_eq!(payload["properties"]["distinct_id"], "actor-system:host");
        Ok(())
    }

    #[tokio::test]
    async fn posts_a_queued_batch_during_bounded_shutdown() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let host = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let body = loop {
                let mut chunk = [0_u8; 4096];
                let read = stream.read(&mut chunk).await?;
                anyhow::ensure!(read > 0, "PostHog test request ended before its body");
                request.extend_from_slice(&chunk[..read]);
                let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = std::str::from_utf8(&request[..header_end])?;
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>())
                    })
                    .transpose()?
                    .context("PostHog test request omitted Content-Length")?;
                let body_start = header_end + 4;
                if request.len() < body_start + content_length {
                    continue;
                }
                break request[body_start..body_start + content_length].to_vec();
            };
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await?;
            Result::<Vec<u8>>::Ok(body)
        });
        let telemetry =
            PostHogActorTelemetry::new("test-api-key".into(), host, "test".into(), "local".into())?;
        telemetry.publish(execution_event());
        telemetry.shutdown(Duration::from_secs(2)).await?;

        let body: Value = serde_json::from_slice(&server.await??)?;
        assert_eq!(body["api_key"], "test-api-key");
        assert_eq!(body["batch"][0]["event"], "actor_execution_finished");
        assert_eq!(telemetry.dropped_events(), 0);
        Ok(())
    }

    fn execution_event() -> ActorTelemetryEvent {
        ActorTelemetryEvent::ActorExecutionFinished(ActorExecutionTelemetry {
            scope: ActorTelemetryScope {
                namespace_id: Some("namespace-1".into()),
            },
            role: ActorSystemRole::Host,
            total_ms: 12.5,
            queue_wait_ms: 1.0,
            actor_ready_ms: 2.0,
            executor_ms: 3.0,
            capture_ms: 2.0,
            publish_ms: 3.0,
            checkpoint_ms: 1.5,
            cold_start: false,
            state_changed: true,
            receipt_replay: false,
            execution_kind: ActorExecutionKind::HotWrite,
            success: true,
            outcome: "completed".into(),
            failure_class: None,
            error_code: None,
            actor_type: "Counter".into(),
            method: "increment".into(),
        })
    }
}
