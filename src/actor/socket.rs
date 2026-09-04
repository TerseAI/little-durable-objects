use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;

use super::executor_connection::{ActorSocketEffect, ActorSocketMessage};

pub(crate) const MAX_SOCKET_METADATA_BYTES: usize = 64 * 1024;
pub(crate) const MAX_SOCKET_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

const MAX_SOCKET_CONNECTION_ID_BYTES: usize = 128;
const MAX_SOCKET_TAGS: usize = 128;
const MAX_SOCKET_TAG_CHARACTERS: usize = 256;
const MAX_SOCKET_TAG_BYTES: usize = 8 * 1024;
const MAX_SOCKET_CLOSE_REASON_BYTES: usize = 123;

pub(crate) fn validate_socket_metadata(metadata: &Value) -> Result<()> {
    ensure!(
        serde_json::to_vec(metadata)?.len() <= MAX_SOCKET_METADATA_BYTES,
        "socket metadata exceeds {MAX_SOCKET_METADATA_BYTES} bytes"
    );
    Ok(())
}

pub(crate) fn validate_socket_effects(effects: &[ActorSocketEffect]) -> Result<()> {
    for effect in effects {
        validate_socket_effect(effect)?;
    }
    Ok(())
}

fn validate_socket_effect(effect: &ActorSocketEffect) -> Result<()> {
    match effect {
        ActorSocketEffect::Send {
            connection_id,
            message,
        } => {
            validate_connection_id(connection_id)?;
            validate_socket_message(message)
        }
        ActorSocketEffect::Broadcast {
            message,
            except_connection_ids,
            tags,
        } => {
            ensure!(
                except_connection_ids.len() <= MAX_SOCKET_TAGS,
                "socket broadcast exclusions exceed {MAX_SOCKET_TAGS} entries"
            );
            for connection_id in except_connection_ids {
                validate_connection_id(connection_id)?;
            }
            validate_socket_tags(tags)?;
            validate_socket_message(message)
        }
        ActorSocketEffect::Close {
            connection_id,
            code,
            reason,
        }
        | ActorSocketEffect::Reject {
            connection_id,
            code,
            reason,
        } => {
            validate_connection_id(connection_id)?;
            validate_close(*code, reason)
        }
        ActorSocketEffect::SetMetadata {
            connection_id,
            metadata,
        } => {
            validate_connection_id(connection_id)?;
            validate_socket_metadata(metadata)
        }
        ActorSocketEffect::SetTags {
            connection_id,
            tags,
        } => {
            validate_connection_id(connection_id)?;
            validate_socket_tags(tags)
        }
    }
}

fn validate_socket_message(message: &ActorSocketMessage) -> Result<()> {
    match message {
        ActorSocketMessage::Text { data } => ensure!(
            data.len() <= MAX_SOCKET_MESSAGE_BYTES,
            "socket message exceeds {MAX_SOCKET_MESSAGE_BYTES} bytes"
        ),
        ActorSocketMessage::Binary { data } => {
            let max_encoded_bytes = MAX_SOCKET_MESSAGE_BYTES.div_ceil(3) * 4;
            ensure!(
                data.len() <= max_encoded_bytes,
                "socket message exceeds {MAX_SOCKET_MESSAGE_BYTES} decoded bytes"
            );
            let decoded = STANDARD
                .decode(data)
                .context("socket binary message is not valid base64")?;
            ensure!(
                decoded.len() <= MAX_SOCKET_MESSAGE_BYTES,
                "socket message exceeds {MAX_SOCKET_MESSAGE_BYTES} decoded bytes"
            );
        }
    }
    Ok(())
}

fn validate_connection_id(connection_id: &str) -> Result<()> {
    ensure!(!connection_id.is_empty(), "socket connection ID is empty");
    ensure!(
        connection_id.len() <= MAX_SOCKET_CONNECTION_ID_BYTES,
        "socket connection ID exceeds {MAX_SOCKET_CONNECTION_ID_BYTES} bytes"
    );
    Ok(())
}

fn validate_socket_tags(tags: &[String]) -> Result<()> {
    ensure!(
        tags.len() <= MAX_SOCKET_TAGS,
        "socket tags exceed {MAX_SOCKET_TAGS} entries"
    );
    let mut total_bytes = 0usize;
    for tag in tags {
        ensure!(!tag.is_empty(), "socket tag is empty");
        ensure!(
            tag.chars().count() <= MAX_SOCKET_TAG_CHARACTERS,
            "socket tag exceeds {MAX_SOCKET_TAG_CHARACTERS} characters"
        );
        total_bytes = total_bytes.saturating_add(tag.len());
    }
    ensure!(
        total_bytes <= MAX_SOCKET_TAG_BYTES,
        "socket tags exceed {MAX_SOCKET_TAG_BYTES} bytes"
    );
    Ok(())
}

fn validate_close(code: u16, reason: &str) -> Result<()> {
    ensure!(
        code == 1000 || (3000..=4999).contains(&code),
        "socket close code must be 1000 or between 3000 and 4999"
    );
    ensure!(
        reason.len() <= MAX_SOCKET_CLOSE_REASON_BYTES,
        "socket close reason exceeds {MAX_SOCKET_CLOSE_REASON_BYTES} bytes"
    );
    Ok(())
}
