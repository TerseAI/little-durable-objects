use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, unix::OwnedWriteHalf},
    sync::{Mutex as AsyncMutex, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::{ActorInvocationFailure, ActorKey};

const ACTOR_EXECUTOR_PROTOCOL_VERSION: u32 = 12;
pub(crate) const MAX_ACTOR_EXECUTOR_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub struct ActorMethodInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub method: String,
    pub args: Vec<Value>,
    pub state: Option<Value>,
    pub connections: Vec<ActorSocketConnection>,
}

#[derive(Debug, Serialize)]
pub struct ActorMethodEviction {
    pub actor: ActorKey,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorSocketConnection {
    pub id: String,
    pub metadata: Value,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketMessage {
    Text { data: String },
    Binary { data: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEvent {
    Connect {
        connection: ActorSocketConnection,
    },
    Message {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Disconnect {
        connection: ActorSocketConnection,
        code: u16,
        reason: String,
        was_clean: bool,
    },
}

#[derive(Debug, Serialize)]
pub struct ActorSocketInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub event: ActorSocketEvent,
    pub connections: Vec<ActorSocketConnection>,
    pub state: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEffect {
    Send {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Broadcast {
        message: ActorSocketMessage,
        except_connection_ids: Vec<String>,
        tags: Vec<String>,
    },
    Close {
        connection_id: String,
        code: u16,
        reason: String,
    },
    Reject {
        connection_id: String,
        code: u16,
        reason: String,
    },
    SetMetadata {
        connection_id: String,
        metadata: Value,
    },
    SetTags {
        connection_id: String,
        tags: Vec<String>,
    },
}

#[derive(Debug, PartialEq)]
pub enum ActorMethodOutcome {
    Completed {
        result: Value,
        state: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed(ActorInvocationFailure),
}

#[derive(Debug, PartialEq)]
pub enum ActorSocketOutcome {
    Handled {
        state: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed(ActorInvocationFailure),
}

#[async_trait]
pub trait ActorExecutor: Send + Sync {
    fn supports(&self, actor_type: &str) -> bool;

    async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome>;

    async fn handle_socket(
        &self,
        _invocation: ActorSocketInvocation,
    ) -> Result<ActorSocketOutcome> {
        Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
            code: "socket_not_supported".into(),
            message: "actor executor does not support sockets".into(),
        }))
    }

    async fn evict(&self, _eviction: ActorMethodEviction) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct ActorExecutorListener {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl ActorExecutorListener {
    pub(crate) async fn bind(socket_path: impl Into<PathBuf>) -> Result<Self> {
        let socket_path = socket_path.into();
        prepare_socket_path(&socket_path).await?;
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create actor executor directory {}", parent.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind actor executor socket {}", socket_path.display()))?;
        Ok(Self {
            listener,
            socket_path,
        })
    }

    pub(crate) async fn accept(self) -> Result<ActorExecutorConnection> {
        let result = self.accept_connection().await;
        let cleanup = remove_socket(&self.socket_path).await;
        match (result, cleanup) {
            (Ok(connection), Ok(())) => Ok(connection),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn accept_connection(&self) -> Result<ActorExecutorConnection> {
        let (stream, _) =
            self.listener.accept().await.with_context(|| {
                format!("accept actor executor at {}", self.socket_path.display())
            })?;
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let attach = match read_client_message(&mut reader).await? {
            Some(ActorExecutorClientMessage::Attach {
                protocol,
                actor_types,
            }) => {
                ensure!(
                    protocol == ACTOR_EXECUTOR_PROTOCOL_VERSION,
                    "customer actor executor uses unsupported protocol version {protocol}"
                );
                ensure!(
                    !actor_types.is_empty(),
                    "customer actor executor did not advertise any actor types"
                );
                actor_types
            }
            Some(_) => {
                anyhow::bail!("first customer actor executor message must attach the process")
            }
            None => anyhow::bail!("customer actor executor disconnected before attaching"),
        };

        let executor = Arc::new(JsActorExecutor::new(writer, attach));
        let task = tokio::spawn(read_executor_messages(reader, executor.clone()));
        debug!(
            socket = %self.socket_path.display(),
            actor_types = ?executor.actor_types,
            "customer JavaScript process connected to actor executor"
        );
        Ok(ActorExecutorConnection { executor, task })
    }
}

pub(crate) struct ActorExecutorConnection {
    executor: Arc<JsActorExecutor>,
    task: JoinHandle<Result<()>>,
}

impl ActorExecutorConnection {
    pub(crate) fn executor(&self) -> Arc<dyn ActorExecutor> {
        self.executor.clone()
    }

    pub(crate) async fn mark_ready(&self) -> Result<()> {
        self.executor
            .send(&ActorExecutorServerMessage::Attached {
                protocol: ACTOR_EXECUTOR_PROTOCOL_VERSION,
            })
            .await?;
        info!(
            actor_types = ?self.executor.actor_types,
            "customer JavaScript process attached to actor executor"
        );
        Ok(())
    }

    pub(crate) async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        tokio::select! {
            result = &mut self.task => {
                match result {
                    Ok(result) => result,
                    Err(error) => Err(error.into()),
                }
            }
            _ = shutdown.cancelled() => {
                self.executor.close().await;
                self.task.abort();
                let _ = (&mut self.task).await;
                Ok(())
            }
        }
    }
}

impl Drop for ActorExecutorConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct JsActorExecutor {
    actor_types: HashSet<String>,
    next_message_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<ExecutorReply>>>,
    writer: AsyncMutex<OwnedWriteHalf>,
}

#[async_trait]
impl ActorExecutor for JsActorExecutor {
    fn supports(&self, actor_type: &str) -> bool {
        self.actor_types.contains(actor_type)
    }

    async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome> {
        match self.exchange(ExecutorCommand::Invoke(invocation)).await? {
            ExecutorReply::Invoked {
                result,
                state,
                effects,
            } => Ok(ActorMethodOutcome::Completed {
                result,
                state,
                effects,
            }),
            ExecutorReply::Failed { code, message } => {
                Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            ExecutorReply::Evicted => {
                anyhow::bail!("actor executor returned eviction reply to invocation")
            }
            ExecutorReply::WebsocketHandled { .. } => {
                anyhow::bail!("actor executor returned socket reply to invocation")
            }
        }
    }

    async fn handle_socket(&self, invocation: ActorSocketInvocation) -> Result<ActorSocketOutcome> {
        match self
            .exchange(ExecutorCommand::WebsocketEvent(invocation))
            .await?
        {
            ExecutorReply::WebsocketHandled { state, effects } => {
                Ok(ActorSocketOutcome::Handled { state, effects })
            }
            ExecutorReply::Failed { code, message } => {
                Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            ExecutorReply::Invoked { .. } | ExecutorReply::Evicted => {
                anyhow::bail!("actor executor returned the wrong reply to socket event")
            }
        }
    }

    async fn evict(&self, eviction: ActorMethodEviction) -> Result<()> {
        match self.exchange(ExecutorCommand::Evict(eviction)).await? {
            ExecutorReply::Evicted => Ok(()),
            ExecutorReply::Failed { code, message } => {
                anyhow::bail!("actor executor rejected eviction ({code}): {message}")
            }
            ExecutorReply::Invoked { .. } => {
                anyhow::bail!("actor executor returned the wrong reply to eviction")
            }
            ExecutorReply::WebsocketHandled { .. } => {
                anyhow::bail!("actor executor returned socket reply to eviction")
            }
        }
    }
}

impl JsActorExecutor {
    fn new(writer: OwnedWriteHalf, actor_types: Vec<String>) -> Self {
        Self {
            actor_types: actor_types.into_iter().collect(),
            next_message_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            writer: AsyncMutex::new(writer),
        }
    }

    async fn exchange(&self, command: ExecutorCommand) -> Result<ExecutorReply> {
        let message_id = self.next_message_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| anyhow::anyhow!("actor executor pending-reply lock poisoned"))?
            .insert(message_id, reply_tx);

        let write_result = self
            .send(&ActorExecutorServerMessage::Command {
                message_id,
                command: Box::new(command),
            })
            .await;
        if let Err(error) = write_result {
            self.remove_pending(message_id)?;
            if let Some(limit) = error.downcast_ref::<ActorExecutorMessageTooLarge>() {
                return Ok(ExecutorReply::Failed {
                    code: "resource_exhausted".into(),
                    message: limit.to_string(),
                });
            }
            return Err(error.context("send command to customer actor executor"));
        }

        reply_rx
            .await
            .context("customer actor executor disconnected before replying")
    }

    async fn send(&self, message: &ActorExecutorServerMessage) -> Result<()> {
        write_server_message(&mut *self.writer.lock().await, message).await
    }

    fn deliver(&self, message_id: u64, reply: ExecutorReply) -> Result<()> {
        let sender = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("actor executor pending-reply lock poisoned"))?
            .remove(&message_id)
            .with_context(|| format!("actor executor replied to unknown message {message_id}"))?;
        let _ = sender.send(reply);
        Ok(())
    }

    fn remove_pending(&self, message_id: u64) -> Result<()> {
        self.pending
            .lock()
            .map_err(|_| anyhow::anyhow!("actor executor pending-reply lock poisoned"))?
            .remove(&message_id);
        Ok(())
    }

    async fn close(&self) {
        let _ = self.writer.lock().await.shutdown().await;
        self.disconnect();
    }

    fn disconnect(&self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.clear();
        }
    }
}

async fn read_executor_messages(
    mut reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    executor: Arc<JsActorExecutor>,
) -> Result<()> {
    let result = async {
        loop {
            match read_client_message(&mut reader).await? {
                None => {
                    anyhow::bail!("customer JavaScript actor executor disconnected")
                }
                Some(ActorExecutorClientMessage::Reply { message_id, reply }) => {
                    executor.deliver(message_id, reply)?;
                }
                Some(ActorExecutorClientMessage::Attach { .. }) => {
                    anyhow::bail!("customer actor executor attached more than once")
                }
            }
        }
    }
    .await;
    executor.disconnect();
    result
}

async fn read_client_message(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Option<ActorExecutorClientMessage>> {
    let mut document = Vec::new();
    let bytes = reader
        .take((MAX_ACTOR_EXECUTOR_MESSAGE_BYTES + 1) as u64)
        .read_until(b'\n', &mut document)
        .await?;
    if bytes == 0 {
        return Ok(None);
    }
    ensure!(
        bytes <= MAX_ACTOR_EXECUTOR_MESSAGE_BYTES,
        "customer actor executor message exceeds {MAX_ACTOR_EXECUTOR_MESSAGE_BYTES} bytes"
    );
    serde_json::from_slice(trim_ascii_end(&document))
        .map(Some)
        .context("decode customer actor executor message")
}

fn trim_ascii_end(mut document: &[u8]) -> &[u8] {
    while document.last().is_some_and(u8::is_ascii_whitespace) {
        document = &document[..document.len() - 1];
    }
    document
}

async fn write_server_message(
    writer: &mut OwnedWriteHalf,
    message: &ActorExecutorServerMessage,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_ACTOR_EXECUTOR_MESSAGE_BYTES {
        return Err(ActorExecutorMessageTooLarge.into());
    }
    writer.write_all(&bytes).await?;
    Ok(())
}

#[derive(Debug)]
struct ActorExecutorMessageTooLarge;

impl Display for ActorExecutorMessageTooLarge {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "actor executor command exceeds {MAX_ACTOR_EXECUTOR_MESSAGE_BYTES} bytes"
        )
    }
}

impl Error for ActorExecutorMessageTooLarge {}

async fn prepare_socket_path(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_socket(),
                "refusing to replace non-socket actor executor path {}",
                path.display()
            );
            tokio::fs::remove_file(path).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn remove_socket(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {
            debug!(socket = %path.display(), "actor executor socket removed");
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorServerMessage {
    Attached {
        protocol: u32,
    },
    Command {
        message_id: u64,
        command: Box<ExecutorCommand>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorClientMessage {
    Attach {
        protocol: u32,
        actor_types: Vec<String>,
    },
    Reply {
        message_id: u64,
        reply: ExecutorReply,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorCommand {
    Invoke(ActorMethodInvocation),
    WebsocketEvent(ActorSocketInvocation),
    Evict(ActorMethodEviction),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorReply {
    Invoked {
        result: Value,
        state: Value,
        #[serde(default)]
        effects: Vec<ActorSocketEffect>,
    },
    WebsocketHandled {
        state: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed {
        code: String,
        message: String,
    },
    Evicted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
        time::{Duration, timeout},
    };

    #[tokio::test]
    async fn one_javascript_executor_runs_until_host_shutdown() -> Result<()> {
        let root = TempDir::new_in("/tmp")?;
        let socket = root.path().join("actor-executor.sock");
        let host = ActorExecutorListener::bind(&socket).await?;
        let customer = tokio::spawn(run_incrementing_customer(socket.clone()));
        let connection = host.accept().await?;
        let executor = connection.executor();
        connection.mark_ready().await?;
        assert!(executor.supports("counter"));

        let shutdown = CancellationToken::new();
        let connection_task = tokio::spawn(connection.run(shutdown.clone()));
        let outcome = executor
            .invoke(ActorMethodInvocation {
                request_id: "request-1".into(),
                actor: ActorKey {
                    namespace_id: "namespace-1".into(),
                    actor_type: "counter".into(),
                    actor_id: "counter-1".into(),
                },
                method: "increment".into(),
                args: vec![json!(2)],
                state: None,
                connections: Vec::new(),
            })
            .await?;
        assert_eq!(
            outcome,
            ActorMethodOutcome::Completed {
                result: json!(2),
                state: json!({ "count": 2 }),
                effects: Vec::new(),
            }
        );
        let socket_outcome = executor
            .handle_socket(ActorSocketInvocation {
                request_id: "socket-request-1".into(),
                actor: ActorKey {
                    namespace_id: "namespace-1".into(),
                    actor_type: "counter".into(),
                    actor_id: "counter-1".into(),
                },
                event: ActorSocketEvent::Connect {
                    connection: ActorSocketConnection {
                        id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1" }),
                        tags: Vec::new(),
                    },
                },
                connections: vec![ActorSocketConnection {
                    id: "socket-1".into(),
                    metadata: json!({ "userId": "user-1" }),
                    tags: Vec::new(),
                }],
                state: Some(json!({ "count": 2 })),
            })
            .await?;
        assert_eq!(
            socket_outcome,
            ActorSocketOutcome::Handled {
                state: json!({ "count": 3 }),
                effects: vec![ActorSocketEffect::Send {
                    connection_id: "socket-1".into(),
                    message: ActorSocketMessage::Text {
                        data: "ready".into()
                    },
                }],
            }
        );
        shutdown.cancel();
        connection_task.await??;
        customer.await??;
        Ok(())
    }

    #[tokio::test]
    async fn oversized_commands_are_reported_as_resource_exhausted() -> Result<()> {
        let root = TempDir::new_in("/tmp")?;
        let socket = root.path().join("actor-executor.sock");
        let host = ActorExecutorListener::bind(&socket).await?;
        let customer = tokio::spawn(run_attached_customer(socket.clone()));
        let connection = host.accept().await?;
        let executor = connection.executor();
        connection.mark_ready().await?;

        let shutdown = CancellationToken::new();
        let connection_task = tokio::spawn(connection.run(shutdown.clone()));
        let outcome = executor
            .invoke(ActorMethodInvocation {
                request_id: "request-1".into(),
                actor: ActorKey {
                    namespace_id: "namespace-1".into(),
                    actor_type: "counter".into(),
                    actor_id: "counter-1".into(),
                },
                method: "accept".into(),
                args: vec![json!("x".repeat(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES))],
                state: None,
                connections: Vec::new(),
            })
            .await?;

        assert!(matches!(
            outcome,
            ActorMethodOutcome::Failed(ref failure) if failure.code == "resource_exhausted"
        ));
        shutdown.cancel();
        connection_task.await??;
        customer.await??;
        Ok(())
    }

    #[tokio::test]
    async fn oversized_client_messages_are_rejected_before_newline() -> Result<()> {
        let (host, mut customer) = UnixStream::pair()?;
        let (reader, _) = host.into_split();
        let mut reader = BufReader::new(reader);
        let customer = tokio::spawn(async move {
            let chunk = vec![b'x'; 64 * 1024];
            for _ in 0..=MAX_ACTOR_EXECUTOR_MESSAGE_BYTES / chunk.len() {
                customer.write_all(&chunk).await?;
            }
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        });

        let result = timeout(Duration::from_secs(5), read_client_message(&mut reader)).await;
        customer.abort();
        let error = result
            .context("oversized actor executor message was not rejected before newline")?
            .expect_err("oversized actor executor message should fail");
        assert!(error.to_string().contains("exceeds"));
        Ok(())
    }

    async fn run_incrementing_customer(socket: PathBuf) -> Result<()> {
        let stream = UnixStream::connect(socket).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        writer
            .write_all(b"{\"type\":\"attach\",\"protocol\":12,\"actor_types\":[\"counter\"]}\n")
            .await?;
        ensure!(
            read_json_line(&mut reader).await? == json!({ "type": "attached", "protocol": 12 })
        );

        let invocation = read_json_line(&mut reader).await?;
        let invocation_id = invocation["message_id"]
            .as_u64()
            .context("invocation message ID")?;
        ensure!(invocation["command"]["type"] == "invoke");
        ensure!(invocation["command"].get("timeout_ms").is_none());
        write_json_line(
            &mut writer,
            &json!({
                "type": "reply",
                "message_id": invocation_id,
                "reply": {
                    "type": "invoked",
                    "result": 2,
                    "state": { "count": 2 }
                }
            }),
        )
        .await?;

        let socket_event = read_json_line(&mut reader).await?;
        let socket_event_id = socket_event["message_id"]
            .as_u64()
            .context("socket event message ID")?;
        ensure!(socket_event["command"]["type"] == "websocket_event");
        ensure!(socket_event["command"]["event"]["type"] == "connect");
        write_json_line(
            &mut writer,
            &json!({
                "type": "reply",
                "message_id": socket_event_id,
                "reply": {
                    "type": "websocket_handled",
                    "state": { "count": 3 },
                    "effects": [{
                        "type": "send",
                        "connection_id": "socket-1",
                        "message": { "type": "text", "data": "ready" }
                    }]
                }
            }),
        )
        .await?;

        let mut trailing = String::new();
        ensure!(
            reader.read_line(&mut trailing).await? == 0,
            "expected Rust host to close the actor executor"
        );
        Ok(())
    }

    async fn run_attached_customer(socket: PathBuf) -> Result<()> {
        let stream = UnixStream::connect(socket).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        writer
            .write_all(b"{\"type\":\"attach\",\"protocol\":12,\"actor_types\":[\"counter\"]}\n")
            .await?;
        ensure!(
            read_json_line(&mut reader).await? == json!({ "type": "attached", "protocol": 12 })
        );
        let mut trailing = String::new();
        ensure!(
            reader.read_line(&mut trailing).await? == 0,
            "oversized command reached the customer actor executor"
        );
        Ok(())
    }

    async fn read_json_line<R>(reader: &mut R) -> Result<Value>
    where
        R: tokio::io::AsyncBufRead + Unpin,
    {
        let mut line = String::new();
        ensure!(reader.read_line(&mut line).await? > 0, "expected JSON line");
        Ok(serde_json::from_str(line.trim_end())?)
    }

    async fn write_json_line<W>(writer: &mut W, value: &Value) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        writer
            .write_all(serde_json::to_string(value)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        Ok(())
    }
}
