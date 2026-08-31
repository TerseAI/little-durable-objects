use super::*;
use crate::actor::{ActorExecutionResult, ActorInvocation, ActorInvocationFailure, ActorKey};
use anyhow::Result;
use prost::Message;
use serde_json::json;

fn invocation() -> ActorInvocation {
    ActorInvocation {
        request_id: "request-1".into(),
        actor: ActorKey {
            namespace_id: "namespace-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        },
        method: "increment".into(),
        args: vec![json!(3)],
        timeout_ms: 30_000,
    }
}

#[test]
fn protobuf_round_trips_an_actor_invocation() -> Result<()> {
    let invocation = invocation();
    let encoded = proto::InvokeActorRequest {
        request_id: invocation.request_id.clone(),
        actor: Some(proto::ActorKey {
            namespace_id: invocation.actor.namespace_id.clone(),
            actor_type: invocation.actor.actor_type.clone(),
            actor_id: invocation.actor.actor_id.clone(),
        }),
        method: invocation.method.clone(),
        args_json: serde_json::to_vec(&invocation.args)?,
        timeout_ms: invocation.timeout_ms,
    }
    .encode_to_vec();
    let decoded = proto::InvokeActorRequest::decode(encoded.as_slice())?;

    assert_eq!(ActorInvocation::try_from(decoded)?, invocation);
    Ok(())
}

#[test]
fn protobuf_encodes_every_actor_result() -> Result<()> {
    let results = [
        ActorExecutionResult::Completed {
            result: json!({ "count": 3 }),
        },
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure {
                code: "method_failed".into(),
                message: "the method rejected".into(),
            },
        },
        ActorExecutionResult::Reroute,
        ActorExecutionResult::HostUnavailable,
    ];

    for result in results {
        assert!(proto::InvokeActorReply::from(result).result.is_some());
    }

    Ok(())
}

#[test]
fn protobuf_rejects_a_malformed_invocation() {
    let malformed = proto::InvokeActorRequest {
        request_id: "request-1".into(),
        actor: Some(proto::ActorKey {
            namespace_id: "namespace-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        }),
        method: "increment".into(),
        args_json: br#"{"not":"an array"}"#.to_vec(),
        timeout_ms: 30_000,
    };
    assert!(ActorInvocation::try_from(malformed).is_err());
}
