use std::mem;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    actor_state::ActorStorageKey,
    durability::{
        ActorManifest, CapturedActorChanges, OwnershipClaimResult, RecoveryData,
        VersionedActorManifest,
    },
    grpc::proto::{ControlPlaneReply, ControlPlaneRequest},
    host::HostId,
    host_leases::{HostLease, HostLeaseRequest, HostLeaseStatus},
    ltx::LtxSegment,
    telemetry::{ActorTelemetryEvent, ControlPlaneOperation},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommand {
    RegisterLease {
        request: HostLeaseRequest,
    },
    GetLeaseStatus {
        host_id: HostId,
    },
    UnregisterLease {
        host_id: HostId,
    },
    GetManifest {
        #[serde(rename = "object")]
        storage_key: ActorStorageKey,
    },
    Claim {
        #[serde(rename = "object")]
        storage_key: ActorStorageKey,
        expected: Option<VersionedActorManifest>,
        host_id: HostId,
    },
    Publish {
        #[serde(rename = "object")]
        storage_key: ActorStorageKey,
        current: VersionedActorManifest,
        captured: CapturedActorChanges,
    },
    Recovery {
        #[serde(rename = "object")]
        storage_key: ActorStorageKey,
        manifest: ActorManifest,
    },
    TelemetryBatch {
        events: Vec<ActorTelemetryEvent>,
    },
}

impl ControlPlaneCommand {
    pub(crate) fn control_plane_operation(&self) -> Option<ControlPlaneOperation> {
        match self {
            Self::RegisterLease { .. } => Some(ControlPlaneOperation::RegisterLease),
            Self::GetLeaseStatus { .. } => Some(ControlPlaneOperation::GetLeaseStatus),
            Self::UnregisterLease { .. } => Some(ControlPlaneOperation::UnregisterLease),
            Self::GetManifest { .. } => Some(ControlPlaneOperation::GetManifest),
            Self::Claim { .. } => Some(ControlPlaneOperation::Claim),
            Self::Publish { .. } => Some(ControlPlaneOperation::Publish),
            Self::Recovery { .. } => Some(ControlPlaneOperation::Recovery),
            Self::TelemetryBatch { .. } => None,
        }
    }
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
    LeaseStatus {
        status: HostLeaseStatus,
    },
    Manifest {
        manifest: Option<VersionedActorManifest>,
    },
    Claim {
        result: OwnershipClaimResult,
    },
    Published {
        manifest: VersionedActorManifest,
    },
    Recovery {
        recovery: RecoveryData,
    },
}

pub(crate) fn encode_command(mut command: ControlPlaneCommand) -> Result<ControlPlaneRequest> {
    let mut binary_payloads = Vec::new();
    if let ControlPlaneCommand::Publish { captured, .. } = &mut command {
        detach_segment_payloads(captured.segments_mut(), &mut binary_payloads);
    }
    Ok(ControlPlaneRequest {
        command_json: serde_json::to_vec(&command)?,
        binary_payloads,
    })
}

pub(crate) fn decode_command(request: ControlPlaneRequest) -> Result<ControlPlaneCommand> {
    let mut command: ControlPlaneCommand = serde_json::from_slice(&request.command_json)?;
    let mut binary_payloads = request.binary_payloads.into_iter();
    match &mut command {
        ControlPlaneCommand::Publish { captured, .. } => {
            attach_segment_payloads(captured.segments_mut(), &mut binary_payloads)?;
        }
        _ => {
            ensure!(
                binary_payloads.next().is_none(),
                "control-plane command has unexpected binary payloads"
            );
        }
    }
    ensure!(
        binary_payloads.next().is_none(),
        "control-plane command has excess binary payloads"
    );
    Ok(command)
}

pub(crate) fn encode_reply(mut reply: ControlPlaneCommandReply) -> Result<ControlPlaneReply> {
    let mut binary_payloads = Vec::new();
    if let ControlPlaneCommandReply::Recovery { recovery } = &mut reply {
        if let Some(checkpoint) = &mut recovery.checkpoint {
            binary_payloads.push(mem::take(&mut checkpoint.bytes));
        }
        detach_segment_payloads(&mut recovery.segments, &mut binary_payloads);
    }
    Ok(ControlPlaneReply {
        reply_json: serde_json::to_vec(&reply)?,
        binary_payloads,
    })
}

pub(crate) fn decode_reply(reply: ControlPlaneReply) -> Result<ControlPlaneCommandReply> {
    let mut decoded: ControlPlaneCommandReply = serde_json::from_slice(&reply.reply_json)?;
    let mut binary_payloads = reply.binary_payloads.into_iter();
    match &mut decoded {
        ControlPlaneCommandReply::Recovery { recovery } => {
            if let Some(checkpoint) = &mut recovery.checkpoint {
                ensure!(
                    checkpoint.bytes.is_empty(),
                    "control-plane checkpoint metadata contains inline bytes"
                );
                checkpoint.bytes = binary_payloads
                    .next()
                    .context("control-plane reply omitted checkpoint bytes")?;
            }
            attach_segment_payloads(&mut recovery.segments, &mut binary_payloads)?;
        }
        _ => {
            ensure!(
                binary_payloads.next().is_none(),
                "control-plane reply has unexpected binary payloads"
            );
        }
    }
    ensure!(
        binary_payloads.next().is_none(),
        "control-plane reply has excess binary payloads"
    );
    Ok(decoded)
}

fn detach_segment_payloads(segments: &mut [LtxSegment], binary_payloads: &mut Vec<Vec<u8>>) {
    binary_payloads.extend(
        segments
            .iter_mut()
            .map(|segment| mem::take(&mut segment.bytes)),
    );
}

fn attach_segment_payloads(
    segments: &mut [LtxSegment],
    binary_payloads: &mut impl Iterator<Item = Vec<u8>>,
) -> Result<()> {
    for segment in segments {
        ensure!(
            segment.bytes.is_empty(),
            "control-plane LTX metadata contains inline bytes"
        );
        segment.bytes = binary_payloads
            .next()
            .context("control-plane message omitted LTX bytes")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{
        durability::{
            ActorDurabilityStore, CheckpointMetadata, CommitPosition, LocalActorStore,
            RecoveredCheckpoint,
        },
        host::HostId,
    };

    fn segment(bytes: &[u8]) -> LtxSegment {
        LtxSegment {
            min_txid: 1,
            max_txid: 1,
            page_size: 4_096,
            commit: 1,
            post_apply_checksum: 1,
            pre_apply_checksum: None,
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn telemetry_batches_do_not_emit_control_plane_request_telemetry() {
        let command = ControlPlaneCommand::TelemetryBatch { events: Vec::new() };

        assert_eq!(command.control_plane_operation(), None);
    }

    #[tokio::test]
    async fn publish_binary_payloads_are_an_invisible_codec_detail() -> Result<()> {
        let root = TempDir::new()?;
        let store = LocalActorStore::new(root.path());
        let object = ActorStorageKey::new("object-1");
        let current = match store.claim(&object, None, &HostId::new("node-1")).await? {
            OwnershipClaimResult::Acquired(manifest) => manifest,
            result => anyhow::bail!("test claim failed: {result:?}"),
        };

        let encoded = encode_command(ControlPlaneCommand::Publish {
            storage_key: object,
            current,
            captured: CapturedActorChanges::new(vec![segment(b"ltx")]),
        })?;
        assert_eq!(encoded.binary_payloads, vec![b"ltx".to_vec()]);
        let command_json = String::from_utf8(encoded.command_json.clone())?;
        assert!(command_json.contains("\"type\":\"publish\""));
        assert!(command_json.contains("\"object\":\"object-1\""));
        assert!(!command_json.contains("storage_key"));

        let ControlPlaneCommand::Publish { captured, .. } = decode_command(encoded)? else {
            anyhow::bail!("decoded command was not publish")
        };
        assert_eq!(captured.segments()[0].bytes, b"ltx");
        Ok(())
    }

    #[test]
    fn recovery_binary_payloads_are_an_invisible_codec_detail() -> Result<()> {
        let checkpoint_bytes = b"sqlite".to_vec();
        let encoded = encode_reply(ControlPlaneCommandReply::Recovery {
            recovery: RecoveryData {
                checkpoint: Some(RecoveredCheckpoint {
                    metadata: CheckpointMetadata {
                        through: CommitPosition {
                            epoch: 1,
                            log_generation: 0,
                            max_txid: 1,
                        },
                        object_key: "checkpoint".into(),
                        byte_len: checkpoint_bytes.len() as u64,
                        crc32c: 0,
                        page_size: 4_096,
                        post_apply_checksum: 1,
                    },
                    bytes: checkpoint_bytes.clone(),
                }),
                segments: vec![segment(b"tail")],
            },
        })?;
        assert_eq!(
            encoded.binary_payloads,
            vec![checkpoint_bytes.clone(), b"tail".to_vec()]
        );

        let ControlPlaneCommandReply::Recovery { recovery } = decode_reply(encoded)? else {
            anyhow::bail!("decoded reply was not recovery")
        };
        assert_eq!(recovery.checkpoint.unwrap().bytes, checkpoint_bytes);
        assert_eq!(recovery.segments[0].bytes, b"tail");
        Ok(())
    }
}
