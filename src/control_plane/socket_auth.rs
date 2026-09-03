use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::actor::ActorKey;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SocketAuthorizationRequest {
    pub trigger_id: String,
    pub actor_id: String,
    pub credential: String,
}

#[derive(Clone, Debug)]
pub(crate) struct SocketAuthorization {
    pub actor: ActorKey,
    pub storage_region: String,
    pub metadata: Value,
    pub expires_at: i64,
}

#[derive(Debug)]
pub(crate) enum SocketAuthorizationError {
    Rejected,
    Unavailable(anyhow::Error),
}

impl std::fmt::Display for SocketAuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected => formatter.write_str("socket credential was rejected"),
            Self::Unavailable(error) => {
                write!(formatter, "socket authorization is unavailable: {error:#}")
            }
        }
    }
}

impl std::error::Error for SocketAuthorizationError {}

#[async_trait]
pub(crate) trait SocketAuthenticator: Send + Sync {
    async fn authorize(
        &self,
        request: SocketAuthorizationRequest,
    ) -> std::result::Result<SocketAuthorization, SocketAuthorizationError>;
}

pub(crate) struct HttpSocketAuthenticator {
    client: reqwest::Client,
    url: String,
    token: String,
}

impl HttpSocketAuthenticator {
    pub(crate) fn new(url: String, token: String) -> Result<Self> {
        let parsed = reqwest::Url::parse(&url).context("socket authorization URL is invalid")?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "socket authorization URL must use HTTP or HTTPS"
        );
        ensure!(
            !token.is_empty(),
            "socket authorization token must not be empty"
        );
        Ok(Self {
            client: reqwest::Client::new(),
            url,
            token,
        })
    }
}

#[async_trait]
impl SocketAuthenticator for HttpSocketAuthenticator {
    async fn authorize(
        &self,
        request: SocketAuthorizationRequest,
    ) -> std::result::Result<SocketAuthorization, SocketAuthorizationError> {
        let response = self
            .client
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&request)
            .send()
            .await
            .map_err(|error| SocketAuthorizationError::Unavailable(error.into()))?;
        if matches!(response.status().as_u16(), 401 | 403 | 404) {
            return Err(SocketAuthorizationError::Rejected);
        }
        let response = response
            .error_for_status()
            .map_err(|error| SocketAuthorizationError::Unavailable(error.into()))?
            .json::<SocketAuthorizationResponse>()
            .await
            .map_err(|error| SocketAuthorizationError::Unavailable(error.into()))?;
        response
            .into_authorization(&request.actor_id)
            .map_err(SocketAuthorizationError::Unavailable)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SocketAuthorizationResponse {
    namespace_id: String,
    actor_type: String,
    actor_id: String,
    storage_region: String,
    metadata: Value,
    expires_at: i64,
}

impl SocketAuthorizationResponse {
    fn into_authorization(self, requested_actor_id: &str) -> Result<SocketAuthorization> {
        ensure!(
            self.actor_id == requested_actor_id,
            "socket authorization changed the requested actor ID"
        );
        ensure!(
            !self.storage_region.is_empty(),
            "socket authorization omitted its storage region"
        );
        ensure!(
            self.expires_at > unix_seconds()?,
            "socket authorization has expired"
        );
        let actor = ActorKey {
            namespace_id: self.namespace_id,
            actor_type: self.actor_type,
            actor_id: self.actor_id,
        };
        actor.validate()?;
        Ok(SocketAuthorization {
            actor,
            storage_region: self.storage_region,
            metadata: self.metadata,
            expires_at: self.expires_at,
        })
    }
}

fn unix_seconds() -> Result<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?)
}
