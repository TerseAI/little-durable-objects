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
    actor::ActorKey,
    grpc::proto::actor_control_plane_service_client::ActorControlPlaneServiceClient,
    host::HostId,
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
};

use super::{
    CONTROL_PLANE_REQUEST_TIMEOUT, MAX_CONTROL_PLANE_MESSAGE_BYTES,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_reply, encode_command},
};

const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ControlPlaneClient {
    client: ActorControlPlaneServiceClient<Channel>,
    authorization: Arc<RwLock<MetadataValue<tonic::metadata::Ascii>>>,
}

impl ControlPlaneClient {
    pub async fn connect(endpoint: impl Into<String>, token: impl AsRef<str>) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.into())
            .context("parse actor control-plane endpoint")?
            .connect_timeout(CONTROL_PLANE_CONNECT_TIMEOUT)
            .timeout(CONTROL_PLANE_REQUEST_TIMEOUT)
            .connect()
            .await
            .context("connect to actor control plane")?;
        Ok(Self {
            client: ActorControlPlaneServiceClient::new(channel)
                .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES),
            authorization: Arc::new(RwLock::new(bearer_authorization(token.as_ref())?)),
        })
    }

    pub async fn authorize_state_write(
        &self,
        actor: &ActorKey,
        host_id: &HostId,
        owner_epoch: u64,
        expected_generation: &str,
    ) -> Result<String> {
        match self
            .execute(ControlPlaneCommand::AuthorizeStateWrite {
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch,
                expected_generation: expected_generation.to_owned(),
            })
            .await?
        {
            ControlPlaneCommandReply::StateWriteUrl { url } => Ok(url),
            reply => anyhow::bail!("unexpected state-write authorization reply: {reply:?}"),
        }
    }
}

#[async_trait]
impl HostLeaseRegistry for ControlPlaneClient {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        match self
            .execute(ControlPlaneCommand::RegisterLease {
                request: request.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Lease {
                lease,
                replacement_token,
            } => {
                if let Some(token) = replacement_token {
                    self.replace_token(&token)?;
                }
                Ok(lease)
            }
            reply => anyhow::bail!("unexpected register-lease reply: {reply:?}"),
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

impl ControlPlaneClient {
    async fn execute(&self, command: ControlPlaneCommand) -> Result<ControlPlaneCommandReply> {
        let mut request = Request::new(encode_command(command)?);
        request.set_timeout(CONTROL_PLANE_REQUEST_TIMEOUT);
        request.metadata_mut().insert(
            "authorization",
            self.authorization
                .read()
                .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))?
                .clone(),
        );
        let reply = self
            .client
            .clone()
            .execute(request)
            .await
            .context("execute actor control-plane command")?
            .into_inner();
        decode_reply(reply).context("decode actor control-plane reply")
    }

    fn replace_token(&self, token: &str) -> Result<()> {
        *self
            .authorization
            .write()
            .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))? =
            bearer_authorization(token)?;
        Ok(())
    }
}

fn bearer_authorization(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    ensure!(
        !token.is_empty() && token.trim() == token,
        "actor token is invalid"
    );
    format!("Bearer {token}")
        .parse()
        .context("actor token is not valid gRPC metadata")
}
