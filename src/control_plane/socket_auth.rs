use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::actor::{ActorKey, validate_socket_metadata};

use super::CONTROL_PLANE_REQUEST_TIMEOUT;

const MAX_SOCKET_AUTHORIZATION_RESPONSE_BYTES: usize = 128 * 1024;

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
            client: reqwest::Client::builder()
                .timeout(CONTROL_PLANE_REQUEST_TIMEOUT)
                .build()
                .context("build socket authorization HTTP client")?,
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
            .map_err(|error| SocketAuthorizationError::Unavailable(error.into()))?;
        let response = read_authorization_response(response)
            .await
            .map_err(SocketAuthorizationError::Unavailable)?;
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
        validate_socket_metadata(&self.metadata)?;
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

async fn read_authorization_response(
    mut response: reqwest::Response,
) -> Result<SocketAuthorizationResponse> {
    if let Some(content_length) = response.content_length() {
        ensure!(
            content_length <= MAX_SOCKET_AUTHORIZATION_RESPONSE_BYTES as u64,
            "socket authorization response exceeds {MAX_SOCKET_AUTHORIZATION_RESPONSE_BYTES} bytes"
        );
    }
    let mut document = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(MAX_SOCKET_AUTHORIZATION_RESPONSE_BYTES as u64) as usize,
    );
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            document.len().saturating_add(chunk.len()) <= MAX_SOCKET_AUTHORIZATION_RESPONSE_BYTES,
            "socket authorization response exceeds {MAX_SOCKET_AUTHORIZATION_RESPONSE_BYTES} bytes"
        );
        document.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&document).context("decode socket authorization response")
}

#[cfg(test)]
mod tests {
    use axum::{Router, routing::post};
    use serde_json::json;

    use super::*;

    #[test]
    fn rejects_oversized_authorized_socket_metadata() {
        let response = SocketAuthorizationResponse {
            namespace_id: "project-1".into(),
            actor_type: "ChatRoom".into(),
            actor_id: "room-1".into(),
            storage_region: "north-america-east".into(),
            metadata: json!({ "data": "x".repeat(64 * 1024) }),
            expires_at: i64::MAX,
        };

        let error = response
            .into_authorization("room-1")
            .expect_err("oversized metadata should fail");
        assert!(error.to_string().contains("metadata"));
    }

    #[tokio::test]
    async fn rejects_oversized_authorization_response_while_reading() -> Result<()> {
        let app = Router::new().route("/", post(|| async { vec![b'x'; 128 * 1024 + 1] }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(axum::serve(listener, app).into_future());
        let authenticator = HttpSocketAuthenticator::new(
            format!("http://{address}"),
            "authorization-token".into(),
        )?;

        let error = authenticator
            .authorize(SocketAuthorizationRequest {
                trigger_id: "trigger-1".into(),
                actor_id: "room-1".into(),
                credential: "socket-credential".into(),
            })
            .await
            .expect_err("oversized authorization response should fail");
        server.abort();
        assert!(error.to_string().contains("exceeds"));
        Ok(())
    }
}
