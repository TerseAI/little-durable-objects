use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, error, warn};

use super::proto::{
    InvokeActorReply, InvokeActorRequest,
    actor_host_service_server::{ActorHostService, ActorHostServiceServer},
};
use crate::{
    actor::{ActorInvocation, MAX_ACTOR_EXECUTOR_MESSAGE_BYTES},
    control_plane::ActorJwtVerifier,
    host::{ActorHost, ActorProcessRole},
};

pub(crate) struct ActorHostGrpcService {
    host: Arc<ActorHost>,
    invocation_auth: ActorJwtVerifier,
}

impl ActorHostGrpcService {
    pub(crate) fn new(host: Arc<ActorHost>, auth: ActorJwtVerifier) -> Self {
        Self {
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
    #[tracing::instrument(name = "actor.host.invoke", skip(self, request))]
    async fn invoke(
        &self,
        request: Request<InvokeActorRequest>,
    ) -> Result<Response<InvokeActorReply>, Status> {
        let principal = self.invocation_auth.authenticate(&request).await?;
        if principal.process_role != ActorProcessRole::Workflow {
            return Err(Status::permission_denied(
                "actor invocation credential has the wrong process role",
            ));
        }
        let invocation: ActorInvocation = request.into_inner().try_into().map_err(|error| {
            warn!(
                error = %format!("{error:#}"),
                "rejected invalid actor invocation"
            );
            Status::invalid_argument(format!("{error:#}"))
        })?;
        if !principal.scope.contains(&invocation.actor) {
            return Err(Status::permission_denied(
                "actor invocation crossed namespace scope",
            ));
        }

        let request_id = invocation.request_id.clone();
        let reply = self
            .host
            .invoke_actor(invocation)
            .await
            .map_err(|execution_error| {
                error!(
                    request_id,
                    caller_host_id = %principal.host_id,
                    caller_session_id = %principal.session_id,
                    error = %format!("{execution_error:#}"),
                    "actor invocation failed"
                );
                Status::internal(format!("{execution_error:#}"))
            })?;
        debug!(
            request_id,
            caller_host_id = %principal.host_id,
            "actor invocation completed"
        );
        Ok(Response::new(reply.into()))
    }
}
