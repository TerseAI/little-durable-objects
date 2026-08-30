use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Channel, Endpoint},
};

use crate::{
    actor_state::ActorStorageKey,
    durability::{
        ActorDurabilityStore, ActorManifest, CapturedActorChanges, OwnershipClaimResult,
        RecoveryData, VersionedActorManifest,
    },
    grpc::proto::actor_control_plane_service_client::ActorControlPlaneServiceClient,
    host::HostId,
    host_leases::{HostLease, HostLeaseRequest, HostLeaseStatus, HostLeaseStore},
    telemetry::{
        ActorSystemRole, ActorTelemetry, ActorTelemetryEvent, ActorTelemetryScope,
        ControlPlaneOperation, ControlPlaneRequestTelemetry, elapsed_ms, noop_actor_telemetry,
    },
};

use super::{
    CONTROL_PLANE_REQUEST_TIMEOUT, MAX_CONTROL_PLANE_MESSAGE_BYTES,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_reply, encode_command},
};

const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ControlPlaneClient {
    client: ActorControlPlaneServiceClient<Channel>,
    credentials: ControlPlaneCredentials,
    telemetry: Arc<RwLock<Arc<dyn ActorTelemetry>>>,
    telemetry_scope: Arc<RwLock<ActorTelemetryScope>>,
}

#[derive(Clone)]
pub struct ControlPlaneCredentials {
    authorization: Arc<RwLock<MetadataValue<tonic::metadata::Ascii>>>,
}

impl ControlPlaneCredentials {
    pub fn new(token: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            authorization: Arc::new(RwLock::new(bearer_authorization(token.as_ref())?)),
        })
    }

    pub fn replace(&self, token: &str) -> Result<()> {
        let authorization = bearer_authorization(token)?;
        *self
            .authorization
            .write()
            .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))? = authorization;
        Ok(())
    }

    pub(crate) fn current(&self) -> Result<MetadataValue<tonic::metadata::Ascii>> {
        Ok(self
            .authorization
            .read()
            .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))?
            .clone())
    }
}

impl ControlPlaneClient {
    pub async fn connect(endpoint: impl Into<String>, token: impl AsRef<str>) -> Result<Self> {
        let credentials = ControlPlaneCredentials::new(token)?;
        let channel = Endpoint::from_shared(endpoint.into())
            .context("parse actor control-plane endpoint")?
            .connect_timeout(CONTROL_PLANE_CONNECT_TIMEOUT)
            .timeout(CONTROL_PLANE_REQUEST_TIMEOUT)
            .connect()
            .await
            .context("connect to actor control plane")?;
        let client = ActorControlPlaneServiceClient::new(channel)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES);
        Ok(Self {
            client,
            credentials,
            telemetry: Arc::new(RwLock::new(noop_actor_telemetry())),
            telemetry_scope: Arc::new(RwLock::new(ActorTelemetryScope::default())),
        })
    }

    pub fn set_telemetry(
        &self,
        scope: ActorTelemetryScope,
        telemetry: Arc<dyn ActorTelemetry>,
    ) -> Result<()> {
        *self
            .telemetry
            .write()
            .map_err(|_| anyhow::anyhow!("actor telemetry lock poisoned"))? = telemetry;
        *self
            .telemetry_scope
            .write()
            .map_err(|_| anyhow::anyhow!("actor telemetry scope lock poisoned"))? = scope;
        Ok(())
    }

    pub(crate) fn telemetry_transport(&self) -> Self {
        Self {
            client: self.client.clone(),
            credentials: self.credentials.clone(),
            telemetry: Arc::new(RwLock::new(noop_actor_telemetry())),
            telemetry_scope: Arc::new(RwLock::new(ActorTelemetryScope::default())),
        }
    }

    pub fn replace_token(&self, token: &str) -> Result<()> {
        self.credentials.replace(token)
    }

    async fn execute(&self, command: ControlPlaneCommand) -> Result<ControlPlaneCommandReply> {
        let telemetry = command
            .control_plane_operation()
            .map(|operation| (operation, std::time::Instant::now()));
        let result = async {
            let request = encode_command(command)?;
            let mut client = self.client.clone();
            let response = client
                .execute(self.authenticated_request(request)?)
                .await
                .context("execute actor control-plane command")?
                .into_inner();
            decode_reply(response).context("decode actor control-plane command reply")
        }
        .await;
        if let Some((operation, started)) = telemetry {
            self.publish_control_plane_result(operation, started, &result);
        }
        result
    }

    pub(crate) async fn publish_telemetry_batch(
        &self,
        events: Vec<ActorTelemetryEvent>,
    ) -> Result<()> {
        match self
            .execute(ControlPlaneCommand::TelemetryBatch { events })
            .await?
        {
            ControlPlaneCommandReply::Unit => Ok(()),
            reply => anyhow::bail!("unexpected telemetry-batch reply: {reply:?}"),
        }
    }

    fn publish_control_plane_result<T>(
        &self,
        operation: ControlPlaneOperation,
        started: std::time::Instant,
        result: &Result<T>,
    ) {
        let (grpc_code, timed_out) = result
            .as_ref()
            .err()
            .and_then(tonic_status)
            .map(|status| {
                (
                    Some(format!("{:?}", status.code()).to_lowercase()),
                    status.code() == tonic::Code::DeadlineExceeded,
                )
            })
            .unwrap_or((None, false));
        let scope = self
            .telemetry_scope
            .read()
            .map(|scope| scope.clone())
            .unwrap_or_default();
        let event =
            ActorTelemetryEvent::ControlPlaneRequestFinished(ControlPlaneRequestTelemetry {
                scope,
                role: ActorSystemRole::Host,
                operation,
                total_ms: elapsed_ms(started),
                success: result.is_ok(),
                timed_out,
                grpc_code,
            });
        if let Ok(telemetry) = self.telemetry.read() {
            telemetry.publish(event);
        }
    }

    fn authenticated_request<T>(&self, value: T) -> Result<Request<T>> {
        let mut request = Request::new(value);
        request.set_timeout(CONTROL_PLANE_REQUEST_TIMEOUT);
        request
            .metadata_mut()
            .insert("authorization", self.credentials.current()?);
        Ok(request)
    }
}

fn tonic_status(error: &anyhow::Error) -> Option<&tonic::Status> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>())
}

fn bearer_authorization(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    ensure!(
        !token.is_empty() && token.trim() == token,
        "actor token must be non-empty without surrounding whitespace"
    );
    format!("Bearer {token}")
        .parse()
        .context("actor token is not valid gRPC metadata")
}

#[async_trait]
impl HostLeaseStore for ControlPlaneClient {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        match self
            .execute(ControlPlaneCommand::RegisterLease {
                request: request.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Lease { lease } => Ok(lease),
            reply => anyhow::bail!("unexpected register-lease reply: {reply:?}"),
        }
    }

    async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus> {
        match self
            .execute(ControlPlaneCommand::GetLeaseStatus {
                host_id: id.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::LeaseStatus { status } => Ok(status),
            reply => anyhow::bail!("unexpected lease-status reply: {reply:?}"),
        }
    }

    async fn unregister(&self, id: &HostId, _session_id: &str) -> Result<()> {
        match self
            .execute(ControlPlaneCommand::UnregisterLease {
                host_id: id.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Unit => Ok(()),
            reply => anyhow::bail!("unexpected unregister-lease reply: {reply:?}"),
        }
    }
}

#[async_trait]
impl ActorDurabilityStore for ControlPlaneClient {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        match self
            .execute(ControlPlaneCommand::GetManifest {
                storage_key: object.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Manifest { manifest } => Ok(manifest),
            reply => anyhow::bail!("unexpected manifest reply: {reply:?}"),
        }
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        node: &HostId,
    ) -> Result<OwnershipClaimResult> {
        match self
            .execute(ControlPlaneCommand::Claim {
                storage_key: object.clone(),
                expected: expected.cloned(),
                host_id: node.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Claim { result } => Ok(result),
            reply => anyhow::bail!("unexpected claim reply: {reply:?}"),
        }
    }

    async fn publish(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<VersionedActorManifest> {
        match self
            .execute(ControlPlaneCommand::Publish {
                storage_key: object.clone(),
                current: current.clone(),
                captured: captured.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Published { manifest } => Ok(manifest),
            reply => anyhow::bail!("unexpected publish reply: {reply:?}"),
        }
    }

    async fn recovery(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<RecoveryData> {
        match self
            .execute(ControlPlaneCommand::Recovery {
                storage_key: object.clone(),
                manifest: manifest.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Recovery { recovery } => Ok(recovery),
            reply => anyhow::bail!("unexpected recovery reply: {reply:?}"),
        }
    }
}
