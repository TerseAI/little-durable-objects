use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{error, warn};

use super::proto::{
    HostInvokeActorRequest, InvokeActorReply,
    actor_host_service_server::{ActorHostService, ActorHostServiceServer},
};
use crate::{
    actor::{
        ActorExecutionResult, ActorInvocation, ActorInvocationFailure,
        MAX_ACTOR_EXECUTOR_MESSAGE_BYTES,
    },
    control_plane::{ActorJwtVerifier, ActorPrincipal},
    host::{ActorHost, ActorProcessRole, HostId},
};

pub(crate) struct ActorHostGrpcService {
    host_id: HostId,
    host: Arc<ActorHost>,
    invocation_auth: ActorJwtVerifier,
}

impl ActorHostGrpcService {
    pub(crate) fn new(host: Arc<ActorHost>, auth: ActorJwtVerifier) -> Self {
        Self {
            host_id: host.id().clone(),
            host,
            invocation_auth: auth,
        }
    }

    pub(crate) fn into_service(self) -> ActorHostServiceServer<Self> {
        ActorHostServiceServer::new(self)
            .max_decoding_message_size(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
    }
}

#[tonic::async_trait]
impl ActorHostService for ActorHostGrpcService {
    async fn invoke(
        &self,
        request: Request<HostInvokeActorRequest>,
    ) -> Result<Response<InvokeActorReply>, Status> {
        let invocation = self.authorize_invocation(request).await?;
        self.invoke_detached(invocation).await
    }
}

impl ActorHostGrpcService {
    async fn authorize_invocation(
        &self,
        request: Request<HostInvokeActorRequest>,
    ) -> Result<AuthorizedHostInvocation, Status> {
        let principal = self.invocation_auth.authenticate(&request).await?;
        let request = request.into_inner();
        let invocation: ActorInvocation = request
            .invocation
            .ok_or_else(|| Status::invalid_argument("actor invocation is required"))?
            .try_into()
            .map_err(|error| Status::invalid_argument(format!("{error:#}")))?;
        if !principal.scope.contains(&invocation.actor) {
            return Err(Status::permission_denied(
                "actor invocation crossed namespace scope",
            ));
        }
        if request.owner_epoch == 0
            || (request.state_version == 0) != request.state_read_url.is_empty()
        {
            return Err(Status::invalid_argument(
                "actor ownership capability is incomplete",
            ));
        }
        validate_invocation_principal(
            &principal,
            &self.host_id,
            &invocation.actor,
            request.owner_epoch,
            request.state_version,
            &request.state_read_url,
        )?;
        Ok(AuthorizedHostInvocation {
            invocation,
            owner_epoch: request.owner_epoch,
            state_version: request.state_version,
            state_read_url: request.state_read_url,
        })
    }

    async fn invoke_detached(
        &self,
        request: AuthorizedHostInvocation,
    ) -> Result<Response<InvokeActorReply>, Status> {
        // The task is deliberately detached so a disconnected caller never
        // cancels an accepted actor method or releases its actor gate early.
        let host = self.host.clone();
        let request_id = request.invocation.request_id.clone();
        let task_request_id = request_id.clone();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = match host
                .invoke_actor(
                    request.invocation,
                    request.owner_epoch,
                    request.state_version,
                    request.state_read_url,
                )
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    warn!(request_id = task_request_id, error = %format!("{error:#}"), "actor invocation failed before execution");
                    ActorExecutionResult::Failed {
                        failure: ActorInvocationFailure {
                            code: "unavailable".into(),
                            message: "actor could not start because its state was unavailable"
                                .into(),
                        },
                    }
                }
            };
            let _ = reply_tx.send(InvokeActorReply::from(result));
        });

        match reply_rx.await {
            Ok(reply) => Ok(Response::new(reply)),
            Err(error) => {
                error!(request_id, error = %error, "actor invocation task stopped without a reply");
                Err(Status::internal("actor invocation task stopped"))
            }
        }
    }
}

fn validate_invocation_principal(
    principal: &ActorPrincipal,
    host_id: &HostId,
    actor: &crate::actor::ActorKey,
    owner_epoch: u64,
    state_version: u64,
    state_read_url: &str,
) -> Result<(), Status> {
    if principal.process_role != ActorProcessRole::Host || principal.host_id != *host_id {
        return Err(Status::permission_denied(
            "actor invocation credential is not for this host",
        ));
    }
    if let Some(capability) = &principal.invocation
        && (capability.actor != *actor
            || capability.host_id != *host_id
            || capability.owner_epoch != owner_epoch
            || capability.state_version != state_version
            || capability.state_read_url != state_read_url)
    {
        return Err(Status::permission_denied(
            "actor invocation does not match its direct capability",
        ));
    }
    Ok(())
}

struct AuthorizedHostInvocation {
    invocation: ActorInvocation,
    owner_epoch: u64,
    state_version: u64,
    state_read_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        actor::{ActorKey, ActorScope},
        control_plane::{ActorInvocationCapability, ActorPrincipal},
    };

    #[test]
    fn direct_capability_is_bound_to_the_actor_epoch_and_state_url() {
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        };
        let host_id = HostId::new("host.v1.project-1.revision-1.host-1");
        let principal = ActorPrincipal {
            scope: ActorScope {
                namespace_id: "project-1".into(),
            },
            host_id: host_id.clone(),
            session_id: "00000000-0000-4000-8000-000000000001".into(),
            process_role: ActorProcessRole::Host,
            region: "north-america-east".into(),
            private_routing: false,
            code_revision: Some("revision-1".into()),
            expires_at: i64::MAX,
            invocation: Some(ActorInvocationCapability {
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch: 3,
                state_version: 1,
                state_read_url: "https://storage.example.com/state".into(),
            }),
        };

        assert!(
            validate_invocation_principal(
                &principal,
                &host_id,
                &actor,
                3,
                1,
                "https://storage.example.com/state"
            )
            .is_ok()
        );
        assert!(
            validate_invocation_principal(
                &principal,
                &host_id,
                &actor,
                4,
                1,
                "https://storage.example.com/state"
            )
            .is_err()
        );
        assert!(
            validate_invocation_principal(
                &principal,
                &host_id,
                &actor,
                3,
                1,
                "https://storage.example.com/other"
            )
            .is_err()
        );
    }
}
