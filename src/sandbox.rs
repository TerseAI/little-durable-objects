use std::{collections::HashMap, process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, process::Command};

use crate::host::HostId;

const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureHostRequest {
    pub namespace_id: String,
    pub code_revision: String,
    pub canonical_region: String,
    pub host_id: HostId,
    pub session_id: String,
    pub host_token: String,
    pub jwt_public_keys: String,
    pub control_plane_url: String,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
    pub actor_idle_timeout_ms: u64,
    pub host_idle_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostHandle {
    pub host_id: HostId,
    pub route: String,
    pub canonical_region: String,
    pub provisioning: Option<ActorHostProvisioning>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostProvisioning {
    pub provider: String,
    pub resource_id: String,
    pub reused: bool,
    pub resource_lookup_ms: u64,
    pub existing_lookup_ms: u64,
    pub create_ms: u64,
    pub placement_ms: u64,
    pub tunnel_ms: u64,
    pub ready_ms: u64,
    pub metadata_ms: u64,
    pub total_ms: u64,
}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle>;
}

#[derive(Clone)]
pub struct HostSandboxRuntimeConfig {
    pub control_plane_url: String,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub actor_idle_timeout_ms: u64,
    pub host_idle_timeout_ms: u64,
}

pub struct CommandSandboxProvider {
    provider_name: String,
    command: String,
    environment: HashMap<String, String>,
}

impl CommandSandboxProvider {
    pub fn new(
        provider_name: String,
        command: String,
        mut environment: HashMap<String, String>,
    ) -> Result<Self> {
        ensure!(
            !provider_name.is_empty() && provider_name.trim() == provider_name,
            "sandbox provider name must be non-empty without surrounding whitespace"
        );
        ensure!(
            !command.is_empty() && command.trim() == command,
            "DURABLE_OBJECT_SANDBOX_COMMAND must be non-empty without surrounding whitespace"
        );
        if let Ok(path) = std::env::var("PATH") {
            environment.entry("PATH".into()).or_insert(path);
        }
        Ok(Self {
            provider_name,
            command,
            environment,
        })
    }
}

#[async_trait]
impl SandboxProvider for CommandSandboxProvider {
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        let response: ActorHostHandle = self.execute("ensure_host", request).await?;
        ensure!(
            response.canonical_region == request.canonical_region,
            "{} sandbox command returned a host in the wrong canonical region",
            self.provider_name
        );
        ensure!(
            !response.host_id.as_str().is_empty() && !response.route.is_empty(),
            "{} sandbox command returned an invalid host",
            self.provider_name
        );
        Ok(response)
    }
}

impl CommandSandboxProvider {
    async fn execute<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<Reply> {
        let mut child = Command::new(&self.command)
            .env_clear()
            .envs(&self.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "start {} sandbox command {:?}",
                    self.provider_name, self.command
                )
            })?;
        let document = serde_json::to_vec(&ProviderCommand { operation, request })?;
        let mut stdin = child
            .stdin
            .take()
            .with_context(|| format!("open {} sandbox command stdin", self.provider_name))?;
        stdin
            .write_all(&document)
            .await
            .with_context(|| format!("write {} sandbox command request", self.provider_name))?;
        stdin
            .shutdown()
            .await
            .with_context(|| format!("close {} sandbox command stdin", self.provider_name))?;
        drop(stdin);
        let output = tokio::time::timeout(PROVIDER_REQUEST_TIMEOUT, child.wait_with_output())
            .await
            .with_context(|| format!("{} sandbox command timed out", self.provider_name))??;
        ensure!(
            output.status.success(),
            "{} sandbox command failed with {}: {}",
            self.provider_name,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
        serde_json::from_slice(&output.stdout)
            .with_context(|| format!("decode {} sandbox command response", self.provider_name))
    }
}

#[derive(Serialize)]
struct ProviderCommand<'a, Request> {
    operation: &'a str,
    request: &'a Request,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_provider_provisioning_timings() {
        let handle: ActorHostHandle = serde_json::from_value(serde_json::json!({
            "hostId": "host.v1.namespace.revision.session",
            "route": "https://host.example.com",
            "canonicalRegion": "north-america-east",
            "provisioning": {
                "provider": "modal",
                "resourceId": "sb-actor",
                "reused": false,
                "resourceLookupMs": 12,
                "existingLookupMs": 34,
                "createMs": 56,
                "placementMs": 78,
                "tunnelMs": 90,
                "readyMs": 123,
                "metadataMs": 4,
                "totalMs": 397
            }
        }))
        .expect("actor host handle");

        let provisioning = handle.provisioning.expect("provisioning timings");
        assert_eq!(provisioning.resource_id, "sb-actor");
        assert_eq!(provisioning.create_ms, 56);
        assert_eq!(provisioning.total_ms, 397);
    }

    #[test]
    fn rejects_ambiguous_command_configuration() {
        assert!(CommandSandboxProvider::new("".into(), "modal".into(), HashMap::new()).is_err());
        assert!(CommandSandboxProvider::new("modal".into(), "".into(), HashMap::new()).is_err());
    }
}
