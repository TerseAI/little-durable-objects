//! Checked conversion between generated protobuf messages and the core command protocol.

use anyhow::{Context, Result};

use super::proto;
use crate::actor::{ActorExecutionResult, ActorInvocation, ActorKey};

impl TryFrom<proto::InvokeActorRequest> for ActorInvocation {
    type Error = anyhow::Error;

    fn try_from(invocation: proto::InvokeActorRequest) -> Result<Self> {
        let actor = invocation.actor.context("actor key is required")?.into();
        let invocation = Self {
            request_id: invocation.request_id,
            actor,
            method: invocation.method,
            args: serde_json::from_slice(&invocation.args_json)
                .context("actor arguments must be a JSON array")?,
            timeout_ms: invocation.timeout_ms,
        };
        invocation.validate()?;
        Ok(invocation)
    }
}

impl From<proto::ActorKey> for ActorKey {
    fn from(actor: proto::ActorKey) -> Self {
        Self {
            namespace_id: actor.namespace_id,
            actor_type: actor.actor_type,
            actor_id: actor.actor_id,
        }
    }
}

impl From<ActorExecutionResult> for proto::InvokeActorReply {
    fn from(reply: ActorExecutionResult) -> Self {
        use proto::invoke_actor_reply::Result;

        let result = match reply {
            ActorExecutionResult::Completed { result } => {
                Result::Completed(proto::ActorCompleted {
                    result_json: serde_json::to_vec(&result)
                        .expect("validated JSON actor result must serialize"),
                })
            }
            ActorExecutionResult::Failed { failure } => Result::Failed(proto::ActorFailed {
                code: failure.code,
                message: failure.message,
            }),
            ActorExecutionResult::Reroute => Result::Reroute(proto::Reroute {}),
        };
        Self {
            result: Some(result),
        }
    }
}
