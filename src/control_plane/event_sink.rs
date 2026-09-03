use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::Serialize;

use crate::actor::{ActorKey, ActorSocketMessage};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SocketMessageEvent {
    pub event_id: String,
    pub namespace_id: String,
    pub actor_type: String,
    pub actor_id: String,
    pub trigger_id: Option<String>,
    pub connection_id: String,
    pub message: ActorSocketMessage,
}

impl SocketMessageEvent {
    pub(crate) fn new(
        actor: &ActorKey,
        trigger_id: Option<String>,
        connection_id: &str,
        message: &ActorSocketMessage,
    ) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            namespace_id: actor.namespace_id.clone(),
            actor_type: actor.actor_type.clone(),
            actor_id: actor.actor_id.clone(),
            trigger_id,
            connection_id: connection_id.to_owned(),
            message: message.clone(),
        }
    }
}

#[async_trait]
pub(crate) trait SocketMessageEventSink: Send + Sync {
    async fn deliver(&self, event: SocketMessageEvent) -> Result<()>;
}

pub(crate) struct HttpSocketMessageEventSink {
    client: reqwest::Client,
    url: String,
    token: String,
}

impl HttpSocketMessageEventSink {
    pub(crate) fn new(url: String, token: String) -> Result<Self> {
        let parsed = reqwest::Url::parse(&url).context("socket event sink URL is invalid")?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "socket event sink URL must use HTTP or HTTPS"
        );
        ensure!(
            !token.is_empty(),
            "socket event sink token must not be empty"
        );
        Ok(Self {
            client: reqwest::Client::new(),
            url,
            token,
        })
    }
}

#[async_trait]
impl SocketMessageEventSink for HttpSocketMessageEventSink {
    async fn deliver(&self, event: SocketMessageEvent) -> Result<()> {
        self.client
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&event)
            .send()
            .await?
            .error_for_status()
            .context("socket event sink rejected the event")?;
        Ok(())
    }
}
