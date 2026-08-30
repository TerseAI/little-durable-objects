//! Sandbox-provider boundary owned by the durable-object control plane.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};

use crate::host::HostId;

const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Provider-neutral request to start or reuse a regional actor host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureHostRequest {
    pub namespace_id: String,
    pub principal_id: String,
    pub credential_id: String,
    pub code_revision: String,
    pub canonical_region: String,
}

/// A running host returned by a sandbox provider.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostHandle {
    pub host_id: HostId,
    pub route: String,
    pub canonical_region: String,
    pub cache_source: CacheSource,
}

/// Describes how a host populated its local disposable SQLite cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheSource {
    Volume,
    #[default]
    DurableStorage,
}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle>;
}

/// Protocol client for adapters implemented outside this Rust process. The first
/// adapter is Modal/TypeScript; future adapters can implement the same endpoint.
pub struct HttpSandboxProvider {
    client: Client,
    endpoint: Url,
    credential: String,
}

impl HttpSandboxProvider {
    pub fn new(endpoint: impl AsRef<str>, credential: String) -> Result<Self> {
        let endpoint = Url::parse(endpoint.as_ref())
            .context("DURABLE_OBJECT_SANDBOX_PROVIDER_URL must be a valid URL")?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https"),
            "DURABLE_OBJECT_SANDBOX_PROVIDER_URL must use HTTP or HTTPS"
        );
        ensure!(
            !credential.is_empty() && credential.trim() == credential,
            "DURABLE_OBJECT_SANDBOX_PROVIDER_TOKEN must be non-empty without surrounding whitespace"
        );
        Ok(Self {
            client: Client::builder()
                .connect_timeout(PROVIDER_CONNECT_TIMEOUT)
                .timeout(PROVIDER_REQUEST_TIMEOUT)
                .build()
                .context("configure sandbox-provider client")?,
            endpoint,
            credential,
        })
    }
}

#[async_trait]
impl SandboxProvider for HttpSandboxProvider {
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.credential)
            .json(request)
            .send()
            .await
            .context("request a host from the configured sandbox provider")?
            .error_for_status()
            .context("sandbox provider rejected the host request")?
            .json::<ActorHostHandle>()
            .await
            .context("decode sandbox-provider response")?;
        ensure!(
            response.canonical_region == request.canonical_region,
            "sandbox provider returned a host in the wrong canonical region"
        );
        ensure!(
            !response.host_id.as_str().is_empty() && !response.route.is_empty(),
            "sandbox provider returned an invalid host"
        );
        Ok(response)
    }
}
