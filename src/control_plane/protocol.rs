use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    grpc::proto::{ControlPlaneReply, ControlPlaneRequest},
    host::HostId,
    host_leases::{HostLease, HostLeaseRequest},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommand {
    RegisterLease {
        request: HostLeaseRequest,
    },
    UnregisterLease {
        host_id: HostId,
    },
    AuthorizeStateWrite {
        actor: ActorKey,
        host_id: HostId,
        owner_epoch: u64,
        expected_generation: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommandReply {
    Unit,
    Lease {
        lease: HostLease,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replacement_token: Option<String>,
    },
    StateWriteUrl {
        url: String,
    },
}

pub(crate) fn encode_command(command: ControlPlaneCommand) -> Result<ControlPlaneRequest> {
    Ok(ControlPlaneRequest {
        command_json: serde_json::to_vec(&command)?,
    })
}

pub(crate) fn decode_command(request: ControlPlaneRequest) -> Result<ControlPlaneCommand> {
    Ok(serde_json::from_slice(&request.command_json)?)
}

pub(crate) fn encode_reply(reply: ControlPlaneCommandReply) -> Result<ControlPlaneReply> {
    Ok(ControlPlaneReply {
        reply_json: serde_json::to_vec(&reply)?,
    })
}

pub(crate) fn decode_reply(reply: ControlPlaneReply) -> Result<ControlPlaneCommandReply> {
    Ok(serde_json::from_slice(&reply.reply_json)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_capabilities_are_plain_json_messages() -> Result<()> {
        let encoded = encode_command(ControlPlaneCommand::AuthorizeStateWrite {
            actor: ActorKey {
                namespace_id: "project-1".into(),
                actor_type: "counter".into(),
                actor_id: "counter-1".into(),
            },
            host_id: HostId::new("host.v1.project-1.revision-1.host-1"),
            owner_epoch: 3,
            expected_generation: "100".into(),
        })?;
        let json = String::from_utf8(encoded.command_json)?;
        assert!(json.contains("authorize_state_write"));
        assert!(json.contains("\"expected_generation\":\"100\""));
        Ok(())
    }
}
