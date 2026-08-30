//! Actor-host credential exchange and refresh.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    control_plane::{ActorJwtVerifier, ControlPlaneClient},
    host::{ActorProcessRole, HostId},
};

const MAX_REFRESH_JITTER: Duration = Duration::from_secs(5);
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
const TOKEN_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct HostCredentialIssuer {
    client: Client,
    endpoint: Url,
    credential: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HostCredentialRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "processId")]
    pub host_id: Option<HostId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(rename = "processRole")]
    pub process_role: ActorProcessRole,
    #[serde(rename = "storageRegion")]
    pub region: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_revision: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct IssuedHostCredentials {
    pub namespace_id: String,
    #[serde(rename = "processId")]
    pub host_id: HostId,
    pub session_id: String,
    #[serde(rename = "processRole")]
    pub process_role: ActorProcessRole,
    #[serde(rename = "storageRegion")]
    pub region: String,
    pub code_revision: Option<String>,
    #[serde(rename = "authorityToken")]
    pub control_plane_token: String,
    pub expires_at_ms: i64,
    pub public_keys: std::collections::HashMap<String, String>,
}

impl IssuedHostCredentials {
    pub fn host_request(&self) -> HostCredentialRequest {
        HostCredentialRequest {
            host_id: Some(self.host_id.clone()),
            session_id: Some(self.session_id.clone()),
            process_role: self.process_role,
            region: self.region.clone(),
            code_revision: self.code_revision.clone(),
        }
    }

    pub fn public_keys_json(&self) -> Result<String> {
        serde_json::to_string(&self.public_keys).context("encode actor JWT public keys")
    }
}

impl HostCredentialIssuer {
    pub fn new(endpoint: &str, credential: String) -> Result<Self> {
        ensure!(
            !credential.is_empty() && credential.trim() == credential,
            "DURABLE_OBJECT_CREDENTIAL must be non-empty without surrounding whitespace"
        );
        let endpoint = Url::parse(endpoint)
            .context("DURABLE_OBJECT_CREDENTIALS_URL must be a valid HTTP or HTTPS URL")?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https"),
            "DURABLE_OBJECT_CREDENTIALS_URL must use HTTP or HTTPS"
        );
        Ok(Self {
            client: Client::builder()
                .connect_timeout(TOKEN_CONNECT_TIMEOUT)
                .timeout(TOKEN_REQUEST_TIMEOUT)
                .build()
                .context("configure actor token HTTP client")?,
            endpoint,
            credential,
        })
    }

    pub async fn issue(
        &self,
        credential_request: &HostCredentialRequest,
    ) -> Result<IssuedHostCredentials> {
        let http_request = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.credential)
            .json(credential_request);
        let response = http_request
            .send()
            .await
            .context("request credentials from the configured issuer")?
            .error_for_status()
            .context("credential issuer rejected the host request")?
            .json::<IssuedHostCredentials>()
            .await
            .context("decode actor token response")?;
        ensure!(
            !response.control_plane_token.is_empty()
                && response.control_plane_token.trim() == response.control_plane_token,
            "credential issuer returned invalid actor credentials"
        );
        ensure!(
            !response.namespace_id.is_empty()
                && !response.host_id.as_str().is_empty()
                && uuid::Uuid::parse_str(&response.session_id).is_ok()
                && !response.public_keys.is_empty(),
            "credential issuer returned an invalid actor host identity"
        );
        if let (Some(host_id), Some(session_id)) =
            (&credential_request.host_id, &credential_request.session_id)
        {
            ensure!(
                response.host_id == *host_id && response.session_id == *session_id,
                "credential issuer changed the actor host identity during refresh"
            );
        }
        ensure!(
            response.process_role == credential_request.process_role
                && response.region == credential_request.region
                && response.code_revision == credential_request.code_revision,
            "credential issuer changed the actor host placement identity"
        );
        ensure!(
            response.expires_at_ms > unix_millis()?,
            "credential issuer returned an expired actor token"
        );
        Ok(response)
    }

    pub async fn refresh(
        self,
        control_plane: Arc<ControlPlaneClient>,
        invocation_auth: ActorJwtVerifier,
        request: HostCredentialRequest,
        mut expires_at_ms: i64,
    ) -> Result<()> {
        let jitter_ms = refresh_jitter_ms();
        loop {
            let now_ms = unix_millis()?;
            let remaining_ms = expires_at_ms.saturating_sub(now_ms);
            let half_lifetime_ms = remaining_ms / 2;
            let bounded_jitter_ms = jitter_ms.min(half_lifetime_ms / 2);
            let refresh_at_ms = now_ms
                .saturating_add(half_lifetime_ms)
                .saturating_sub(bounded_jitter_ms);
            sleep_until_ms(refresh_at_ms).await?;

            let mut retry_delay = INITIAL_RETRY_DELAY;
            loop {
                match self.issue(&request).await {
                    Ok(issued) => {
                        ensure!(
                            issued.expires_at_ms > expires_at_ms,
                            "credential issuer did not advance the actor token expiration"
                        );
                        let public_keys = issued.public_keys_json()?;
                        invocation_auth.replace_public_keys(public_keys)?;
                        control_plane.replace_token(&issued.control_plane_token)?;
                        expires_at_ms = issued.expires_at_ms;
                        info!(
                            token_expires_at_ms = expires_at_ms,
                            "refreshed actor control-plane token"
                        );
                        break;
                    }
                    Err(error) => {
                        let now_ms = unix_millis()?;
                        if now_ms >= expires_at_ms {
                            return Err(error).context(
                                "actor token expired before the credential issuer could refresh it",
                            );
                        }
                        let remaining = Duration::from_millis(
                            u64::try_from(expires_at_ms - now_ms).unwrap_or(u64::MAX),
                        );
                        let delay = retry_delay.min(remaining);
                        warn!(
                            error = %format!("{error:#}"),
                            retry_in_ms = delay.as_millis(),
                            token_expires_at_ms = expires_at_ms,
                            "failed to refresh actor control-plane token"
                        );
                        tokio::time::sleep(delay).await;
                        retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
                    }
                }
            }
        }
    }
}

async fn sleep_until_ms(timestamp_ms: i64) -> Result<()> {
    let now_ms = unix_millis()?;
    if timestamp_ms > now_ms {
        tokio::time::sleep(Duration::from_millis(
            u64::try_from(timestamp_ms - now_ms).unwrap_or(u64::MAX),
        ))
        .await;
    }
    Ok(())
}

fn refresh_jitter_ms() -> i64 {
    let bytes = uuid::Uuid::new_v4().into_bytes();
    let random = u64::from_le_bytes(bytes[..8].try_into().expect("UUID prefix is eight bytes"));
    let max_millis = u64::try_from(MAX_REFRESH_JITTER.as_millis()).unwrap_or(u64::MAX);
    i64::try_from(random % max_millis.max(1)).unwrap_or_default()
}

fn unix_millis() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_millis()).context("system clock exceeds supported JWT range")
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    #[tokio::test]
    async fn exchanges_a_bootstrap_credential_for_an_actor_token() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let expires_at_ms = unix_millis()? + 60_000;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = vec![0; 4096];
            let read = stream.read(&mut request).await?;
            let request = std::str::from_utf8(&request[..read])?;
            ensure!(request.starts_with("POST /sdk/actor-token HTTP/1.1"));
            ensure!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer bootstrap_credential")
            );
            ensure!(request.contains(r#""processRole":"host""#));
            ensure!(request.contains(r#""storageRegion":"default""#));
            let body = format!(
                r#"{{"namespaceId":"namespace-1","processId":"host.v1.namespace-1.00000000-0000-4000-8000-000000000001","sessionId":"00000000-0000-4000-8000-000000000002","processRole":"host","storageRegion":"default","authorityToken":"signed.authority.jwt","expiresAtMs":{} ,"publicKeys":{{"primary":"AA"}}}}"#,
                expires_at_ms
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await?;
            Ok::<_, anyhow::Error>(())
        });

        let issuer = HostCredentialIssuer::new(
            &format!("http://{address}/sdk/actor-token"),
            "bootstrap_credential".into(),
        )?;
        let issued = issuer
            .issue(&HostCredentialRequest {
                host_id: None,
                session_id: None,
                process_role: ActorProcessRole::Host,
                region: "default".into(),
                code_revision: None,
            })
            .await?;
        assert_eq!(issued.control_plane_token, "signed.authority.jwt");
        assert_eq!(issued.expires_at_ms, expires_at_ms);
        server.await??;
        Ok(())
    }
}
