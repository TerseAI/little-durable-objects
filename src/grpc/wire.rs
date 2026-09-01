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
        };
        invocation.validate()?;
        Ok(invocation)
    }
}

impl From<ActorInvocation> for proto::InvokeActorRequest {
    fn from(invocation: ActorInvocation) -> Self {
        Self {
            request_id: invocation.request_id,
            actor: Some(proto::ActorKey::from(invocation.actor)),
            method: invocation.method,
            args_json: serde_json::to_vec(&invocation.args)
                .expect("validated JSON actor arguments must serialize"),
        }
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

impl From<ActorKey> for proto::ActorKey {
    fn from(actor: ActorKey) -> Self {
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
            ActorExecutionResult::HostUnavailable => Result::Failed(proto::ActorFailed {
                code: "unavailable".into(),
                message: "actor host is draining".into(),
            }),
        };
        Self {
            result: Some(result),
        }
    }
}

impl TryFrom<proto::InvokeActorReply> for ActorExecutionResult {
    type Error = anyhow::Error;

    fn try_from(reply: proto::InvokeActorReply) -> Result<Self> {
        use proto::invoke_actor_reply::Result as WireResult;

        match reply.result.context("actor reply omitted its result")? {
            WireResult::Completed(completed) => Ok(Self::Completed {
                result: serde_json::from_slice(&completed.result_json)
                    .context("actor result is not valid JSON")?,
            }),
            WireResult::Failed(failed) => Ok(Self::Failed {
                failure: crate::actor::ActorInvocationFailure {
                    code: failed.code,
                    message: failed.message,
                },
            }),
            WireResult::Reroute(_) => Ok(Self::Reroute),
        }
    }
}
