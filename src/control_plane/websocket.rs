use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};
use tracing::warn;

use crate::{
    actor::{
        ActorKey, ActorSocketConnection, ActorSocketEffect, ActorSocketEvent,
        ActorSocketInvocation, ActorSocketMessage, MAX_SOCKET_MESSAGE_BYTES,
        MAX_SOCKET_METADATA_BYTES, validate_socket_effects,
    },
    control_plane::ActorPrincipal,
};

use super::service::ControlPlaneService;
use super::socket_auth::{SocketAuthorizationError, SocketAuthorizationRequest};

const MAX_CONNECTIONS_PER_ACTOR: usize = 128;
const MAX_SOCKET_EFFECTS_REQUEST_BYTES: usize = 32 * 1024 * 1024;
const SOCKET_INITIALIZATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
struct SocketServerState {
    service: ControlPlaneService,
    registry: SocketRegistry,
}

#[derive(Clone, Default)]
pub(crate) struct SocketRegistry {
    entries: Arc<RwLock<HashMap<ActorKey, HashMap<String, RegisteredSocket>>>>,
}

#[derive(Clone)]
struct RegisteredSocket {
    connection: ActorSocketConnection,
    outbound: mpsc::UnboundedSender<OutboundMessage>,
    open: bool,
    trigger_id: Option<String>,
}

struct SocketAccess {
    actor: ActorKey,
    principal: ActorPrincipal,
    metadata: Option<Value>,
    trigger_id: Option<String>,
}

enum OutboundMessage {
    Message(ActorSocketMessage),
    Close { code: u16, reason: String },
}

pub(crate) fn router(service: ControlPlaneService, registry: SocketRegistry) -> Router {
    Router::new()
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/websocket",
            get(connect_workflow),
        )
        .route("/v1/socket/{trigger_id}/{actor_id}", get(connect_external))
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/socket-effects",
            post(apply_effects),
        )
        .layer(DefaultBodyLimit::max(MAX_SOCKET_EFFECTS_REQUEST_BYTES))
        .with_state(SocketServerState { service, registry })
}

async fn apply_effects(
    State(state): State<SocketServerState>,
    Path((namespace_id, actor_type, actor_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(request): Json<ApplyEffectsRequest>,
) -> Result<StatusCode, SocketApiError> {
    let actor = ActorKey {
        namespace_id,
        actor_type,
        actor_id,
    };
    actor.validate().map_err(SocketApiError::bad_request)?;
    authorize_workflow(&state, &headers, &actor)?;
    validate_socket_effects(&request.effects).map_err(SocketApiError::bad_request)?;
    state.registry.apply(&actor, request.effects).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn connect_workflow(
    State(state): State<SocketServerState>,
    Path((namespace_id, actor_type, actor_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, SocketApiError> {
    let actor = ActorKey {
        namespace_id,
        actor_type,
        actor_id,
    };
    actor.validate().map_err(SocketApiError::bad_request)?;
    let principal = authorize_workflow(&state, &headers, &actor)?;
    upgrade_socket(
        upgrade,
        state,
        SocketAccess {
            actor,
            principal,
            metadata: None,
            trigger_id: None,
        },
    )
}

async fn connect_external(
    State(state): State<SocketServerState>,
    Path((trigger_id, actor_id)): Path<(String, String)>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, SocketApiError> {
    let credential = external_credential(&headers)
        .ok_or_else(|| SocketApiError::unauthorized("socket credential is required"))?;
    let authorization = state
        .service
        .authorize_socket(SocketAuthorizationRequest {
            trigger_id: trigger_id.clone(),
            actor_id,
            credential,
        })
        .await
        .map_err(SocketApiError::from_authorization)?;
    let principal = ActorPrincipal::for_external_socket(
        &authorization.actor,
        authorization.storage_region,
        authorization.expires_at,
    );
    upgrade_socket(
        upgrade.protocols(["terse-do"]),
        state,
        SocketAccess {
            actor: authorization.actor,
            principal,
            metadata: Some(authorization.metadata),
            trigger_id: Some(trigger_id),
        },
    )
}

fn upgrade_socket(
    upgrade: WebSocketUpgrade,
    state: SocketServerState,
    access: SocketAccess,
) -> Result<Response, SocketApiError> {
    Ok(upgrade
        .max_frame_size(MAX_SOCKET_MESSAGE_BYTES)
        .max_message_size(MAX_SOCKET_MESSAGE_BYTES)
        .on_upgrade(move |socket| run_connection(socket, state, access))
        .into_response())
}

async fn run_connection(mut socket: WebSocket, state: SocketServerState, access: SocketAccess) {
    let actor = access.actor;
    let principal = access.principal;
    let metadata = match access.metadata {
        Some(metadata) => metadata,
        None => {
            let Some(metadata) = receive_metadata(&mut socket).await else {
                let _ =
                    close_socket(&mut socket, 1002, "socket metadata was not initialized").await;
                return;
            };
            metadata
        }
    };
    let connection = ActorSocketConnection {
        id: uuid::Uuid::new_v4().to_string(),
        metadata,
        tags: Vec::new(),
    };
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
    if !state
        .registry
        .insert(&actor, connection.clone(), outbound_tx, access.trigger_id)
        .await
    {
        let _ = close_socket(&mut socket, 1013, "actor connection limit reached").await;
        return;
    }

    if !dispatch(
        &state,
        &actor,
        &principal,
        ActorSocketEvent::Connect {
            connection: connection.clone(),
        },
        false,
    )
    .await
    {
        state.registry.remove(&actor, &connection.id).await;
        let _ = close_socket(&mut socket, 1011, "actor rejected the connection").await;
        return;
    }

    let mut disconnect = (1006, String::new(), false);
    loop {
        tokio::select! {
            inbound = socket.recv() => {
                match inbound {
                    Some(Ok(Message::Text(data))) => {
                        if !dispatch(&state, &actor, &principal, ActorSocketEvent::Message {
                            connection_id: connection.id.clone(),
                            message: ActorSocketMessage::Text { data: data.to_string() },
                        }, true).await {
                            disconnect = (1011, "actor socket handler failed".into(), false);
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        if !dispatch(&state, &actor, &principal, ActorSocketEvent::Message {
                            connection_id: connection.id.clone(),
                            message: ActorSocketMessage::Binary { data: STANDARD.encode(data) },
                        }, true).await {
                            disconnect = (1011, "actor socket handler failed".into(), false);
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() { break; }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        disconnect = frame.map_or((1000, String::new(), true), |frame| (frame.code, frame.reason.to_string(), true));
                        break;
                    }
                    Some(Err(_)) | None => break,
                }
            }
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(OutboundMessage::Message(message)) => {
                        let Some(message) = websocket_message(message) else {
                            disconnect = (1011, "actor produced an invalid socket message".into(), false);
                            break;
                        };
                        if socket.send(message).await.is_err() { break; }
                    }
                    Some(OutboundMessage::Close { code, reason }) => {
                        disconnect = (code, reason.clone(), true);
                        let _ = close_socket(&mut socket, code, &reason).await;
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    let _ = dispatch(
        &state,
        &actor,
        &principal,
        ActorSocketEvent::Disconnect {
            connection: connection.clone(),
            code: disconnect.0,
            reason: disconnect.1,
            was_clean: disconnect.2,
        },
        false,
    )
    .await;
    state.registry.remove(&actor, &connection.id).await;
}

async fn dispatch(
    state: &SocketServerState,
    actor: &ActorKey,
    principal: &ActorPrincipal,
    event: ActorSocketEvent,
    trigger_message: bool,
) -> bool {
    let (event, connections) = state.registry.prepare_event(actor, event).await;
    let connecting = match &event {
        ActorSocketEvent::Connect { connection } => Some(connection.id.clone()),
        _ => None,
    };
    let message_connection_id = match &event {
        ActorSocketEvent::Message { connection_id, .. } => Some(connection_id.clone()),
        _ => None,
    };
    let delivered_event = event.clone();
    let invocation = ActorSocketInvocation {
        request_id: uuid::Uuid::new_v4().to_string(),
        actor: actor.clone(),
        event,
        connections,
        state: None,
    };
    match state
        .service
        .dispatch_socket_event(principal, invocation)
        .await
    {
        Ok(effects) => {
            if let Err(error) = validate_socket_effects(&effects) {
                warn!(actor = %actor.storage_key(), error = %format!("{error:#}"), "actor returned invalid socket effects");
                return false;
            }
            state.registry.apply(actor, effects).await;
            if let Some(connection_id) = connecting {
                state.registry.activate(actor, &connection_id).await;
            }
            if trigger_message {
                let trigger_id = state
                    .registry
                    .trigger_id(actor, message_connection_id.as_deref())
                    .await;
                state
                    .service
                    .deliver_socket_message_event(actor, trigger_id, &delivered_event);
            }
            true
        }
        Err(error) => {
            warn!(actor = %actor.storage_key(), error = %format!("{error:#}"), "actor socket event failed");
            false
        }
    }
}

impl SocketRegistry {
    async fn insert(
        &self,
        actor: &ActorKey,
        connection: ActorSocketConnection,
        outbound: mpsc::UnboundedSender<OutboundMessage>,
        trigger_id: Option<String>,
    ) -> bool {
        let mut entries = self.entries.write().await;
        let connections = entries.entry(actor.clone()).or_default();
        if connections.len() >= MAX_CONNECTIONS_PER_ACTOR {
            return false;
        }
        connections.insert(
            connection.id.clone(),
            RegisteredSocket {
                connection,
                outbound,
                open: false,
                trigger_id,
            },
        );
        true
    }

    async fn remove(&self, actor: &ActorKey, connection_id: &str) -> Option<ActorSocketConnection> {
        let mut entries = self.entries.write().await;
        let connections = entries.get_mut(actor)?;
        let removed = connections
            .remove(connection_id)
            .map(|entry| entry.connection);
        if connections.is_empty() {
            entries.remove(actor);
        }
        removed
    }

    async fn connections(&self, actor: &ActorKey) -> Vec<ActorSocketConnection> {
        self.entries
            .read()
            .await
            .get(actor)
            .map(|entries| {
                entries
                    .values()
                    .filter(|entry| entry.open)
                    .map(|entry| entry.connection.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn trigger_id(&self, actor: &ActorKey, connection_id: Option<&str>) -> Option<String> {
        let connection_id = connection_id?;
        self.entries
            .read()
            .await
            .get(actor)
            .and_then(|connections| connections.get(connection_id))
            .and_then(|entry| entry.trigger_id.clone())
    }

    async fn prepare_event(
        &self,
        actor: &ActorKey,
        event: ActorSocketEvent,
    ) -> (ActorSocketEvent, Vec<ActorSocketConnection>) {
        match event {
            ActorSocketEvent::Connect { connection } => {
                let mut connections = self.connections(actor).await;
                connections.push(connection.clone());
                (ActorSocketEvent::Connect { connection }, connections)
            }
            ActorSocketEvent::Message {
                connection_id,
                message,
            } => (
                ActorSocketEvent::Message {
                    connection_id,
                    message,
                },
                self.connections(actor).await,
            ),
            ActorSocketEvent::Disconnect {
                connection,
                code,
                reason,
                was_clean,
            } => {
                let connection = self
                    .remove(actor, &connection.id)
                    .await
                    .unwrap_or(connection);
                (
                    ActorSocketEvent::Disconnect {
                        connection,
                        code,
                        reason,
                        was_clean,
                    },
                    self.connections(actor).await,
                )
            }
        }
    }

    async fn apply(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) {
        for effect in effects {
            self.apply_one(actor, effect).await;
        }
    }

    async fn apply_one(&self, actor: &ActorKey, effect: ActorSocketEffect) {
        match effect {
            ActorSocketEffect::Broadcast {
                message,
                except_connection_ids,
                tags,
            } => {
                let recipients = self
                    .entries
                    .read()
                    .await
                    .get(actor)
                    .map(|connections| {
                        connections
                            .values()
                            .filter(|entry| {
                                entry.open
                                    && !except_connection_ids.contains(&entry.connection.id)
                                    && tags.iter().all(|tag| entry.connection.tags.contains(tag))
                            })
                            .map(|entry| entry.outbound.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for sender in recipients {
                    let _ = sender.send(OutboundMessage::Message(message.clone()));
                }
            }
            ActorSocketEffect::SetMetadata {
                connection_id,
                metadata,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.metadata = metadata;
                }
            }
            ActorSocketEffect::SetTags {
                connection_id,
                tags,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.tags = tags;
                }
            }
            ActorSocketEffect::Send {
                connection_id,
                message,
            } => {
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Message(message));
                }
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
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Close { code, reason });
                }
            }
        }
    }

    async fn sender(
        &self,
        actor: &ActorKey,
        connection_id: &str,
    ) -> Option<mpsc::UnboundedSender<OutboundMessage>> {
        self.entries
            .read()
            .await
            .get(actor)
            .and_then(|connections| connections.get(connection_id))
            .map(|entry| entry.outbound.clone())
    }

    async fn activate(&self, actor: &ActorKey, connection_id: &str) {
        if let Some(entry) = self
            .entries
            .write()
            .await
            .get_mut(actor)
            .and_then(|connections| connections.get_mut(connection_id))
        {
            entry.open = true;
        }
    }
}

fn authorize_workflow(
    state: &SocketServerState,
    headers: &HeaderMap,
    actor: &ActorKey,
) -> Result<ActorPrincipal, SocketApiError> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| SocketApiError::unauthorized("actor token is required"))?
        .to_str()
        .map_err(|_| SocketApiError::unauthorized("actor token is invalid"))?;
    let principal = state
        .service
        .authenticate_workflow(authorization)
        .map_err(|_| SocketApiError::unauthorized("actor token was rejected"))?;
    if principal.process_role != crate::host::ActorProcessRole::Workflow
        || !principal.scope.contains(actor)
    {
        return Err(SocketApiError::forbidden(
            "workflow token is not valid for this actor",
        ));
    }
    Ok(principal)
}

fn external_credential(headers: &HeaderMap) -> Option<String> {
    if let Some(authorization) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        && let Some(credential) = authorization
            .strip_prefix("Bearer ")
            .filter(|value| !value.is_empty())
    {
        return Some(credential.to_owned());
    }
    headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .split(',')
                .map(str::trim)
                .find_map(|protocol| protocol.strip_prefix("terse-ticket."))
        })
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

async fn receive_metadata(socket: &mut WebSocket) -> Option<Value> {
    let message = tokio::time::timeout(SOCKET_INITIALIZATION_TIMEOUT, socket.recv())
        .await
        .ok()??
        .ok()?;
    let Message::Text(document) = message else {
        return None;
    };
    if document.len() > MAX_SOCKET_METADATA_BYTES + 128 {
        return None;
    }
    let initialization = serde_json::from_str::<SocketInitialization>(&document).ok()?;
    crate::actor::validate_socket_metadata(&initialization.metadata)
        .ok()
        .map(|()| initialization.metadata)
}

fn websocket_message(message: ActorSocketMessage) -> Option<Message> {
    match message {
        ActorSocketMessage::Text { data } => Some(Message::Text(data.into())),
        ActorSocketMessage::Binary { data } => STANDARD
            .decode(data)
            .ok()
            .map(|data| Message::Binary(data.into())),
    }
}

async fn close_socket(socket: &mut WebSocket, code: u16, reason: &str) -> Result<(), axum::Error> {
    socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.to_owned().into(),
        })))
        .await
}

#[derive(Deserialize)]
struct SocketInitialization {
    #[serde(rename = "type")]
    _message_type: SocketInitializationType,
    metadata: Value,
}

#[derive(Deserialize)]
struct ApplyEffectsRequest {
    effects: Vec<ActorSocketEffect>,
}

#[derive(Deserialize)]
enum SocketInitializationType {
    #[serde(rename = "initialize")]
    Initialize,
}

struct SocketApiError {
    status: StatusCode,
    message: &'static str,
}

impl SocketApiError {
    fn bad_request(_error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: "invalid socket request",
        }
    }

    fn unauthorized(message: &'static str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message,
        }
    }

    fn forbidden(message: &'static str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message,
        }
    }

    fn from_authorization(error: SocketAuthorizationError) -> Self {
        match error {
            SocketAuthorizationError::Rejected => {
                Self::unauthorized("socket credential was rejected")
            }
            SocketAuthorizationError::Unavailable(_) => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "socket authorization is unavailable",
            },
        }
    }
}

impl IntoResponse for SocketApiError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use serde_json::json;

    use super::*;

    #[test]
    fn external_credentials_support_server_headers_and_browser_protocols() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer terse_socket_secret"),
        );
        assert_eq!(
            external_credential(&headers).as_deref(),
            Some("terse_socket_secret")
        );

        headers.remove(header::AUTHORIZATION);
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("terse-do, terse-ticket.ticket-value"),
        );
        assert_eq!(
            external_credential(&headers).as_deref(),
            Some("ticket-value")
        );
    }

    #[test]
    fn rejects_socket_effects_that_bypass_sdk_invariants() {
        let invalid = [
            ActorSocketEffect::Close {
                connection_id: "socket-1".into(),
                code: 1001,
                reason: String::new(),
            },
            ActorSocketEffect::SetTags {
                connection_id: "socket-1".into(),
                tags: vec!["x".repeat(257)],
            },
            ActorSocketEffect::SetMetadata {
                connection_id: "socket-1".into(),
                metadata: json!({ "data": "x".repeat(64 * 1024) }),
            },
            ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: ActorSocketMessage::Binary {
                    data: "not base64".into(),
                },
            },
        ];

        for effect in invalid {
            assert!(crate::actor::validate_socket_effects(&[effect]).is_err());
        }
    }

    #[tokio::test]
    async fn registry_retains_metadata_tags_and_outbound_messages() {
        let registry = SocketRegistry::default();
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "ChatRoom".into(),
            actor_id: "room-1".into(),
        };
        let (outbound, mut messages) = mpsc::unbounded_channel();
        assert!(
            registry
                .insert(
                    &actor,
                    ActorSocketConnection {
                        id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1" }),
                        tags: Vec::new(),
                    },
                    outbound,
                    None,
                )
                .await
        );
        registry.activate(&actor, "socket-1").await;

        registry
            .apply(
                &actor,
                vec![
                    ActorSocketEffect::SetMetadata {
                        connection_id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1", "ready": true }),
                    },
                    ActorSocketEffect::SetTags {
                        connection_id: "socket-1".into(),
                        tags: vec!["member".into()],
                    },
                    ActorSocketEffect::Send {
                        connection_id: "socket-1".into(),
                        message: ActorSocketMessage::Text {
                            data: "hello".into(),
                        },
                    },
                    ActorSocketEffect::Broadcast {
                        message: ActorSocketMessage::Text {
                            data: "everyone".into(),
                        },
                        except_connection_ids: Vec::new(),
                        tags: vec!["member".into()],
                    },
                ],
            )
            .await;

        assert_eq!(
            registry.connections(&actor).await,
            vec![ActorSocketConnection {
                id: "socket-1".into(),
                metadata: json!({ "userId": "user-1", "ready": true }),
                tags: vec!["member".into()],
            }]
        );
        assert!(matches!(
            messages.recv().await,
            Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "hello"
        ));
        assert!(matches!(
            messages.recv().await,
            Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "everyone"
        ));
    }
}
