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
    control_plane::ActorJwtVerifier,
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
        let principal = self.invocation_auth.authenticate(&request).await?;
        if principal.process_role != ActorProcessRole::Host || principal.host_id != self.host_id {
            return Err(Status::permission_denied(
                "actor invocation credential is not for this host",
            ));
        }
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
        if request.owner_epoch == 0 || request.state_read_url.is_empty() {
            return Err(Status::invalid_argument(
                "actor ownership capability is incomplete",
            ));
        }

        // The task is deliberately detached so a disconnected caller never
        // cancels an accepted actor method or releases its actor gate early.
        let host = self.host.clone();
        let request_id = invocation.request_id.clone();
        let task_request_id = request_id.clone();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = match host
                .invoke_actor(invocation, request.owner_epoch, request.state_read_url)
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
