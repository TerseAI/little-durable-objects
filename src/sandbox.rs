use std::{
    collections::HashMap,
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};

use crate::host::HostId;

const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_PROVIDER_OUTPUT_BYTES: usize = 1024 * 1024;

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
    pub started_at_ms: u64,
    pub input_parsed_at_ms: Option<u64>,
    pub sdk_loaded_at_ms: Option<u64>,
    pub resources_resolved_at_ms: Option<u64>,
    pub existing_host_checked_at_ms: Option<u64>,
    pub sandbox_scheduled_at_ms: Option<u64>,
    pub host_ready_observed_at_ms: Option<u64>,
    pub route_read_at_ms: Option<u64>,
    pub metadata_written_at_ms: Option<u64>,
    pub completed_at_ms: u64,
    #[serde(default)]
    pub command_spawned_at_ms: Option<u64>,
    #[serde(default)]
    pub request_written_at_ms: Option<u64>,
    #[serde(default)]
    pub process_completed_at_ms: Option<u64>,
    #[serde(default)]
    pub response_decoded_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WarmImageRequest {
    pub namespace_id: String,
    pub code_revision: String,
    pub canonical_region: String,
    pub image_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageWarmup {
    pub provider: String,
    pub resource_id: String,
    pub total_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminateHostsRequest {
    pub namespace_id: String,
    pub code_revision: String,
    pub canonical_regions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostTermination {
    pub provider: String,
    pub resource_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicHostRouteRequest {
    pub namespace_id: String,
    pub code_revision: String,
    pub canonical_region: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicHostRoute {
    pub route: String,
}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle>;
    async fn public_host_route(&self, request: &PublicHostRouteRequest) -> Result<PublicHostRoute>;
    async fn warm_image(&self, request: &WarmImageRequest) -> Result<ImageWarmup>;
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination>;
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
        let (mut response, command): (ActorHostHandle, _) =
            self.execute_timed("ensure_host", request).await?;
        if let Some(provisioning) = &mut response.provisioning {
            provisioning.command_spawned_at_ms = command.spawned_at_ms;
            provisioning.request_written_at_ms = command.request_written_at_ms;
            provisioning.process_completed_at_ms = command.process_completed_at_ms;
            provisioning.response_decoded_at_ms = command.response_decoded_at_ms;
        }
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

    async fn public_host_route(&self, request: &PublicHostRouteRequest) -> Result<PublicHostRoute> {
        let response: PublicHostRoute = self.execute("public_host_route", request).await?;
        ensure!(
            !response.route.is_empty(),
            "{} sandbox command returned an invalid public host route",
            self.provider_name
        );
        Ok(response)
    }

    async fn warm_image(&self, request: &WarmImageRequest) -> Result<ImageWarmup> {
        self.execute("warm_image", request).await
    }

    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        self.execute("terminate_hosts", request).await
    }
}

impl CommandSandboxProvider {
    async fn execute<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<Reply> {
        Ok(self.execute_timed(operation, request).await?.0)
    }

    async fn execute_timed<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<(Reply, ProviderCommandTimings)> {
        let started_at = Instant::now();
        let mut timings = ProviderCommandTimings::default();
        match self
            .execute_timed_inner(operation, request, started_at, &mut timings)
            .await
        {
            Ok(response) => Ok((response, timings)),
            Err(source) => Err(ProviderCommandFailure { source, timings }.into()),
        }
    }

    async fn execute_timed_inner<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
        started_at: Instant,
        timings: &mut ProviderCommandTimings,
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
        timings.spawned_at_ms = Some(elapsed_ms(started_at));
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
        timings.request_written_at_ms = Some(elapsed_ms(started_at));
        drop(stdin);
        let stdout = child
            .stdout
            .take()
            .with_context(|| format!("open {} sandbox command stdout", self.provider_name))?;
        let stderr = child
            .stderr
            .take()
            .with_context(|| format!("open {} sandbox command stderr", self.provider_name))?;
        let execution = async {
            let wait = async {
                child
                    .wait()
                    .await
                    .with_context(|| format!("wait for {} sandbox command", self.provider_name))
            };
            tokio::try_join!(
                wait,
                read_bounded_output(stdout, MAX_PROVIDER_OUTPUT_BYTES, "stdout"),
                read_bounded_output(stderr, MAX_PROVIDER_OUTPUT_BYTES, "stderr"),
            )
        };
        let (status, stdout, stderr) = tokio::time::timeout(PROVIDER_REQUEST_TIMEOUT, execution)
            .await
            .with_context(|| format!("{} sandbox command timed out", self.provider_name))??;
        timings.process_completed_at_ms = Some(elapsed_ms(started_at));
        ensure!(
            status.success(),
            "{} sandbox command failed with {}: {}",
            self.provider_name,
            status,
            String::from_utf8_lossy(&stderr).trim()
        );
        let response = serde_json::from_slice(&stdout)
            .with_context(|| format!("decode {} sandbox command response", self.provider_name))?;
        timings.response_decoded_at_ms = Some(elapsed_ms(started_at));
        Ok(response)
    }
}

#[derive(Debug, Default)]
struct ProviderCommandTimings {
    spawned_at_ms: Option<u64>,
    request_written_at_ms: Option<u64>,
    process_completed_at_ms: Option<u64>,
    response_decoded_at_ms: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct ProviderCommandFailure {
    source: anyhow::Error,
    timings: ProviderCommandTimings,
}

impl ProviderCommandFailure {
    pub(crate) fn spawned_at_ms(&self) -> Option<u64> {
        self.timings.spawned_at_ms
    }

    pub(crate) fn request_written_at_ms(&self) -> Option<u64> {
        self.timings.request_written_at_ms
    }

    pub(crate) fn process_completed_at_ms(&self) -> Option<u64> {
        self.timings.process_completed_at_ms
    }

    pub(crate) fn response_decoded_at_ms(&self) -> Option<u64> {
        self.timings.response_decoded_at_ms
    }
}

impl std::fmt::Display for ProviderCommandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for ProviderCommandFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

async fn read_bounded_output(
    reader: impl AsyncRead + Unpin,
    max_bytes: usize,
    label: &str,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    reader
        .take((max_bytes + 1) as u64)
        .read_to_end(&mut output)
        .await
        .with_context(|| format!("read sandbox command {label}"))?;
    ensure!(
        output.len() <= max_bytes,
        "sandbox command {label} exceeds {max_bytes} bytes"
    );
    Ok(output)
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
                "startedAtMs": 0,
                "resourcesResolvedAtMs": 12,
                "existingHostCheckedAtMs": 34,
                "sandboxScheduledAtMs": 56,
                "hostReadyObservedAtMs": 123,
                "routeReadAtMs": 125,
                "metadataWrittenAtMs": 129,
                "completedAtMs": 130
            }
        }))
        .expect("actor host handle");

        let provisioning = handle.provisioning.expect("provisioning timings");
        assert_eq!(provisioning.resource_id, "sb-actor");
        assert_eq!(provisioning.sandbox_scheduled_at_ms, Some(56));
        assert_eq!(provisioning.completed_at_ms, 130);
    }

    #[test]
    fn rejects_ambiguous_command_configuration() {
        assert!(CommandSandboxProvider::new("".into(), "modal".into(), HashMap::new()).is_err());
        assert!(CommandSandboxProvider::new("modal".into(), "".into(), HashMap::new()).is_err());
    }

    #[tokio::test]
    async fn provider_output_is_bounded_while_reading() {
        let error = read_bounded_output(tokio::io::repeat(b'x'), 32, "stdout")
            .await
            .expect_err("unbounded provider output should fail");
        assert!(error.to_string().contains("stdout exceeds 32 bytes"));
    }
}
