//! Persistent local connection between the Rust host and customer JavaScript.

use std::{
    collections::{HashMap, HashSet},
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
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, unix::OwnedWriteHalf},
    sync::{Mutex as AsyncMutex, oneshot, watch},
    task::JoinHandle,
};
use tracing::{debug, info};

use super::{ActorInvocationFailure, ActorKey};

const ACTOR_EXECUTOR_PROTOCOL_VERSION: u32 = 8;
pub(crate) const MAX_ACTOR_EXECUTOR_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub struct ActorMethodInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub method: String,
    pub args: Vec<Value>,
    pub state: Option<Value>,
    pub timeout_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct ActorMethodCancellation {
    pub request_id: String,
    pub actor: ActorKey,
}

#[derive(Debug, PartialEq)]
pub enum ActorMethodOutcome {
    Completed { result: Value, state: Value },
    Failed(ActorInvocationFailure),
}

#[async_trait]
pub trait ActorExecutor: Send + Sync {
    fn supports(&self, actor_type: &str) -> bool;

    async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome>;

    async fn cancel(&self, cancellation: ActorMethodCancellation) -> Result<()>;
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

    pub(crate) async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        tokio::select! {
            result = &mut self.task => {
                match result {
                    Ok(result) => result,
                    Err(error) => Err(error.into()),
                }
            }
            _ = wait_for_shutdown(&mut shutdown) => {
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

#[async_trait]
impl ActorExecutor for JsActorExecutor {
    fn supports(&self, actor_type: &str) -> bool {
        self.actor_types.contains(actor_type)
    }

    async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome> {
        match self.exchange(ExecutorCommand::Invoke(invocation)).await? {
            ExecutorReply::Invoked { result, state } => {
                Ok(ActorMethodOutcome::Completed { result, state })
            }
            ExecutorReply::Failed { code, message } => {
                Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            ExecutorReply::Cancelled => {
                anyhow::bail!("actor executor returned cancellation reply to invocation")
            }
        }
    }

    async fn cancel(&self, cancellation: ActorMethodCancellation) -> Result<()> {
        match self.exchange(ExecutorCommand::Cancel(cancellation)).await? {
            ExecutorReply::Cancelled => Ok(()),
            ExecutorReply::Failed { code, message } => {
                anyhow::bail!("actor executor rejected cancellation ({code}): {message}")
            }
            ExecutorReply::Invoked { .. } => {
                anyhow::bail!("actor executor returned invocation reply to cancellation")
            }
        }
    }
}

struct JsActorExecutor {
    actor_types: HashSet<String>,
    next_message_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<ExecutorReply>>>,
    writer: AsyncMutex<OwnedWriteHalf>,
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

    fn disconnect(&self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.clear();
        }
    }

    async fn close(&self) {
        let _ = self.writer.lock().await.shutdown().await;
        self.disconnect();
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
    let mut line = String::new();
    let bytes = reader.read_line(&mut line).await?;
    if bytes == 0 {
        return Ok(None);
    }
    ensure!(
        bytes <= MAX_ACTOR_EXECUTOR_MESSAGE_BYTES,
        "customer actor executor message exceeds {MAX_ACTOR_EXECUTOR_MESSAGE_BYTES} bytes"
    );
    serde_json::from_str(line.trim_end())
        .map(Some)
        .context("decode customer actor executor message")
}

async fn write_server_message(
    writer: &mut OwnedWriteHalf,
    message: &ActorExecutorServerMessage,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    ensure!(
        bytes.len() <= MAX_ACTOR_EXECUTOR_MESSAGE_BYTES,
        "actor executor command exceeds {MAX_ACTOR_EXECUTOR_MESSAGE_BYTES} bytes"
    );
    writer.write_all(&bytes).await?;
    Ok(())
}

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

async fn wait_for_shutdown(receiver: &mut watch::Receiver<bool>) {
    if !*receiver.borrow() {
        let _ = receiver.changed().await;
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
    Cancel(ActorMethodCancellation),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorReply {
    Invoked { result: Value, state: Value },
    Failed { code: String, message: String },
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
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

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let connection_task = tokio::spawn(connection.run(shutdown_rx));
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
                timeout_ms: 30_000,
            })
            .await?;
        assert_eq!(
            outcome,
            ActorMethodOutcome::Completed {
                result: json!(2),
                state: json!({ "count": 2 }),
            }
        );
        shutdown_tx.send(true)?;
        connection_task.await??;
        customer.await??;
        Ok(())
    }

    async fn run_incrementing_customer(socket: PathBuf) -> Result<()> {
        let stream = UnixStream::connect(socket).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        writer
            .write_all(b"{\"type\":\"attach\",\"protocol\":8,\"actor_types\":[\"counter\"]}\n")
            .await?;
        ensure!(read_json_line(&mut reader).await? == json!({ "type": "attached", "protocol": 8 }));

        let invocation = read_json_line(&mut reader).await?;
        let invocation_id = invocation["message_id"]
            .as_u64()
            .context("invocation message ID")?;
        ensure!(invocation["command"]["type"] == "invoke");
        ensure!(
            invocation["command"]["timeout_ms"]
                .as_u64()
                .is_some_and(|timeout| timeout > 0 && timeout <= 30_000)
        );
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

        let mut trailing = String::new();
        ensure!(
            reader.read_line(&mut trailing).await? == 0,
            "expected Rust host to close the actor executor"
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
