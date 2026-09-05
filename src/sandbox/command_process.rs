use std::{
    collections::HashMap,
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

use super::{MAX_PROVIDER_OUTPUT_BYTES, ProviderCommandTimings, elapsed_ms};

pub(super) fn pool(
    command: String,
    environment: HashMap<String, String>,
) -> Result<Pool<ProviderProcessManager>> {
    Ok(Pool::builder(ProviderProcessManager {
        command,
        environment,
    })
    .max_size(2)
    .runtime(deadpool::Runtime::Tokio1)
    .wait_timeout(Some(Duration::from_secs(120)))
    .build()?)
}

pub(super) async fn exchange<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
    pool: &Pool<ProviderProcessManager>,
    request: &Request,
    started_at: Instant,
    timings: &mut ProviderCommandTimings,
) -> Result<Reply> {
    let process = pool
        .get()
        .await
        .map_err(|error| anyhow::anyhow!("acquire sandbox provider process: {error}"))?;
    let mut lease = ProcessLease(Some(process));
    timings.spawned_at_ms = Some(elapsed_ms(started_at));
    lease
        .0
        .as_mut()
        .expect("provider process lease")
        .exchange(request, started_at, timings)
        .await
}

struct ProcessLease(Option<Object<ProviderProcessManager>>);

impl Drop for ProcessLease {
    fn drop(&mut self) {
        if let Some(process) = self.0.take()
            && process.busy
        {
            drop(Object::take(process));
        }
    }
}

pub(super) struct ProviderProcessManager {
    command: String,
    environment: HashMap<String, String>,
}

impl Manager for ProviderProcessManager {
    type Type = ProviderProcess;
    type Error = anyhow::Error;

    async fn create(&self) -> Result<ProviderProcess> {
        let mut child = Command::new(&self.command)
            .arg("--serve")
            .env_clear()
            .envs(&self.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("start persistent sandbox provider")?;
        let stdin = child.stdin.take().context("open provider stdin")?;
        let stdout = BufReader::new(child.stdout.take().context("open provider stdout")?);
        Ok(ProviderProcess {
            child,
            stdin,
            stdout,
            busy: false,
        })
    }

    async fn recycle(
        &self,
        process: &mut ProviderProcess,
        _: &Metrics,
    ) -> RecycleResult<anyhow::Error> {
        if process.busy
            || process
                .child
                .try_wait()
                .map_err(anyhow::Error::from)?
                .is_some()
        {
            return Err(RecycleError::message(
                "provider exchange was interrupted or process exited",
            ));
        }
        Ok(())
    }
}

pub(super) struct ProviderProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    busy: bool,
}

impl ProviderProcess {
    pub(super) async fn exchange<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &mut self,
        request: &Request,
        started_at: Instant,
        timings: &mut ProviderCommandTimings,
    ) -> Result<Reply> {
        let mut document = serde_json::to_vec(request)?;
        ensure!(
            document.len() <= MAX_PROVIDER_OUTPUT_BYTES,
            "sandbox provider command is too large"
        );
        document.push(b'\n');
        // Cancellation leaves this set, so the pool cannot reuse an ambiguous exchange.
        self.busy = true;
        self.stdin
            .write_all(&document)
            .await
            .context("write provider command")?;
        timings.request_written_at_ms = Some(elapsed_ms(started_at));
        let response = self.read_response().await?;
        timings.process_completed_at_ms = Some(elapsed_ms(started_at));
        let response: ProviderResponse<Reply> =
            serde_json::from_slice(&response).context("decode provider response")?;
        timings.response_decoded_at_ms = Some(elapsed_ms(started_at));
        self.busy = false;
        match response {
            ProviderResponse::Success { result } => Ok(result),
            ProviderResponse::Failure { error } => {
                anyhow::bail!("sandbox provider failed: {error}")
            }
        }
    }

    async fn read_response(&mut self) -> Result<Vec<u8>> {
        let mut response = Vec::new();
        (&mut self.stdout)
            .take((MAX_PROVIDER_OUTPUT_BYTES + 1) as u64)
            .read_until(b'\n', &mut response)
            .await?;
        ensure!(
            response.len() <= MAX_PROVIDER_OUTPUT_BYTES,
            "provider stdout exceeds {MAX_PROVIDER_OUTPUT_BYTES} bytes"
        );
        ensure!(
            response.last() == Some(&b'\n'),
            "provider disconnected before replying; outcome may be unknown"
        );
        Ok(response)
    }
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ProviderResponse<T> {
    Success { result: T },
    Failure { error: String },
}
