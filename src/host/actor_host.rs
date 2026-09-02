use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Notify, watch};
use tracing::{info, warn};

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure,
        ActorMethodEviction, ActorMethodInvocation, ActorMethodOutcome,
    },
    actor_state::{ActorExecutionAdmission, ActorExecutionLocks},
    control_plane::ControlPlaneClient,
    state_log::StateAppend,
    state_transport::{LoadedState, StateTransport, StateWrite},
};

use super::HostEndpoint;

#[async_trait]
pub(crate) trait StateWriteAuthority: Send + Sync {
    async fn authorize_state_write(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_generation: &str,
    ) -> Result<String>;
}

#[async_trait]
impl StateWriteAuthority for ControlPlaneClient {
    async fn authorize_state_write(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_generation: &str,
    ) -> Result<String> {
        ControlPlaneClient::authorize_state_write(
            self,
            actor,
            host_id,
            owner_epoch,
            expected_generation,
        )
        .await
    }
}

pub(crate) struct ActorHost {
    endpoint: HostEndpoint,
    namespace_id: String,
    executor: Arc<dyn ActorExecutor>,
    state_write_authority: Arc<dyn StateWriteAuthority>,
    state: Arc<dyn StateTransport>,
    executions: ActorExecutionLocks,
    accepting: AtomicBool,
    active: AtomicUsize,
    activity_tx: watch::Sender<usize>,
    idle: Notify,
}

impl ActorHost {
    pub(crate) fn new(
        endpoint: HostEndpoint,
        namespace_id: String,
        executor: Arc<dyn ActorExecutor>,
        state_write_authority: Arc<dyn StateWriteAuthority>,
        state: Arc<dyn StateTransport>,
    ) -> Self {
        let (activity_tx, _) = watch::channel(0);
        Self {
            endpoint,
            namespace_id,
            executor,
            state_write_authority,
            state,
            executions: ActorExecutionLocks::new(),
            accepting: AtomicBool::new(true),
            active: AtomicUsize::new(0),
            activity_tx,
            idle: Notify::new(),
        }
    }

    pub(crate) fn activity(&self) -> watch::Receiver<usize> {
        self.activity_tx.subscribe()
    }

    pub(crate) fn id(&self) -> &super::HostId {
        &self.endpoint.id
    }

    pub(crate) async fn invoke_actor(
        &self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        state_read_url: String,
    ) -> Result<ActorExecutionResult> {
        let started_at = Instant::now();
        let mut timings = InvocationTimings::default();
        let outcome = self
            .invoke_actor_once(&invocation, owner_epoch, &state_read_url, &mut timings)
            .await;
        self.log_invocation(&invocation, started_at, &timings, &outcome);
        outcome
    }

    async fn invoke_actor_once(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        state_read_url: &str,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        if let Some(result) = self.validate_invocation(invocation)? {
            return Ok(result);
        }
        let _activity = ActivityGuard::begin(self);
        let object = invocation.actor.storage_key();
        let queue_started_at = Instant::now();
        let _execution = match self.executions.admit(&object).await? {
            ActorExecutionAdmission::Acquired(guard) => guard,
            ActorExecutionAdmission::Full => return Ok(ActorExecutionResult::HostUnavailable),
        };
        timings.queue_ms = elapsed_ms(queue_started_at);
        if !self.accepting.load(Ordering::SeqCst) {
            return Ok(ActorExecutionResult::HostUnavailable);
        }
        let state_load_started_at = Instant::now();
        let loaded = self
            .load_owned_state(invocation, owner_epoch, state_read_url)
            .await;
        timings.state_load_ms = elapsed_ms(state_load_started_at);
        let mut loaded = match loaded? {
            OwnershipRead::Owned(loaded) => loaded,
            OwnershipRead::Reroute => return Ok(ActorExecutionResult::Reroute),
        };
        let execution_started_at = Instant::now();
        let executed = self.execute_method(invocation, &loaded).await;
        timings.actor_execution_ms = elapsed_ms(execution_started_at);
        let (result, next_state) = match executed {
            Ok(outcome) => outcome,
            Err(failure) => return Ok(failure),
        };
        let append = match loaded.log.append(owner_epoch, next_state) {
            Ok(append) => append,
            Err(error) => return Ok(self.state_failure(invocation, error).await),
        };
        if append == StateAppend::Unchanged {
            return Ok(ActorExecutionResult::Completed { result });
        }
        let state_publish_started_at = Instant::now();
        let published = self
            .publish_result(invocation, owner_epoch, loaded, result)
            .await;
        timings.state_publish_ms = elapsed_ms(state_publish_started_at);
        published
    }

    fn log_invocation(
        &self,
        invocation: &ActorInvocation,
        started_at: Instant,
        timings: &InvocationTimings,
        outcome: &Result<ActorExecutionResult>,
    ) {
        match outcome {
            Ok(result) => info!(
                event = "actor_host_invocation",
                request_id = %invocation.request_id,
                namespace_id = %invocation.actor.namespace_id,
                actor_type = %invocation.actor.actor_type,
                actor_id = %invocation.actor.actor_id,
                method = %invocation.method,
                host_id = %self.endpoint.id,
                queue_ms = timings.queue_ms,
                state_load_ms = timings.state_load_ms,
                actor_execution_ms = timings.actor_execution_ms,
                state_publish_ms = timings.state_publish_ms,
                total_ms = elapsed_ms(started_at),
                outcome = actor_execution_outcome(result),
                failure_code = actor_execution_failure_code(result).unwrap_or(""),
                "actor host invocation completed"
            ),
            Err(error) => warn!(
                event = "actor_host_invocation",
                request_id = %invocation.request_id,
                namespace_id = %invocation.actor.namespace_id,
                actor_type = %invocation.actor.actor_type,
                actor_id = %invocation.actor.actor_id,
                method = %invocation.method,
                host_id = %self.endpoint.id,
                queue_ms = timings.queue_ms,
                state_load_ms = timings.state_load_ms,
                actor_execution_ms = timings.actor_execution_ms,
                state_publish_ms = timings.state_publish_ms,
                total_ms = elapsed_ms(started_at),
                outcome = "host_error",
                error = %format!("{error:#}"),
                "actor host invocation failed"
            ),
        }
    }

    pub(crate) async fn drain(&self, timeout: Duration) -> Result<()> {
        self.accepting.store(false, Ordering::SeqCst);
        tokio::time::timeout(timeout, async {
            while self.active.load(Ordering::SeqCst) != 0 {
                self.idle.notified().await;
            }
        })
        .await
        .context("actor invocations did not drain before shutdown")?;
        Ok(())
    }

    fn validate_invocation(
        &self,
        invocation: &ActorInvocation,
    ) -> Result<Option<ActorExecutionResult>> {
        if !self.accepting.load(Ordering::SeqCst) {
            return Ok(Some(ActorExecutionResult::HostUnavailable));
        }
        invocation.validate()?;
        if invocation.actor.namespace_id != self.namespace_id {
            anyhow::bail!("actor invocation crossed the host namespace");
        }
        if !self.executor.supports(&invocation.actor.actor_type) {
            return Ok(Some(failed(
                "actor_error",
                "actor type is not loaded by this host",
            )));
        }
        Ok(None)
    }

    async fn load_owned_state(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        state_read_url: &str,
    ) -> Result<OwnershipRead> {
        let mut loaded = self
            .state
            .read(state_read_url)
            .await
            .context("load actor state")?;
        if has_newer_owner(&loaded, owner_epoch) {
            return Ok(OwnershipRead::Reroute);
        }
        if !loaded.log.claim(owner_epoch).map_err(state_error)? {
            return Ok(OwnershipRead::Owned(loaded));
        }
        if !self.publish_claim(invocation, owner_epoch, &loaded).await? {
            return Ok(OwnershipRead::Reroute);
        }
        loaded = self.state.read(state_read_url).await?;
        if has_current_owner(&loaded, owner_epoch) {
            Ok(OwnershipRead::Owned(loaded))
        } else {
            Ok(OwnershipRead::Reroute)
        }
    }

    async fn execute_method(
        &self,
        invocation: &ActorInvocation,
        loaded: &LoadedState,
    ) -> std::result::Result<(Value, Value), ActorExecutionResult> {
        let outcome = self
            .executor
            .invoke(ActorMethodInvocation {
                request_id: invocation.request_id.clone(),
                actor: invocation.actor.clone(),
                method: invocation.method.clone(),
                args: invocation.args.clone(),
                state: loaded.log.latest_state().cloned(),
            })
            .await;
        match outcome {
            Ok(ActorMethodOutcome::Completed { result, state }) => Ok((result, state)),
            Ok(ActorMethodOutcome::Failed(failure)) => {
                self.evict(&invocation.actor).await;
                Err(failed("actor_error", failure.message))
            }
            Err(error) => {
                self.evict(&invocation.actor).await;
                Err(failed(
                    "actor_error",
                    format!("actor executor failed: {error:#}"),
                ))
            }
        }
    }

    async fn state_failure(
        &self,
        invocation: &ActorInvocation,
        error: anyhow::Error,
    ) -> ActorExecutionResult {
        self.evict(&invocation.actor).await;
        failed("state_error", format!("{error:#}"))
    }

    async fn publish_result(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        loaded: LoadedState,
        result: Value,
    ) -> Result<ActorExecutionResult> {
        if let Err(error) = self.publish_state(invocation, owner_epoch, &loaded).await {
            self.evict(&invocation.actor).await;
            warn!(
                actor = %invocation.actor.storage_key(),
                error = %format!("{error:#}"),
                "actor completed but state publication could not be confirmed"
            );
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }
        Ok(ActorExecutionResult::Completed { result })
    }

    async fn publish_claim(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        loaded: &LoadedState,
    ) -> Result<bool> {
        match self
            .write_state(invocation, owner_epoch, loaded, "ownership_claim")
            .await
            .context("publish actor ownership claim")?
        {
            StateWrite::Written => Ok(true),
            StateWrite::GenerationMismatch => Ok(false),
        }
    }

    async fn publish_state(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        loaded: &LoadedState,
    ) -> Result<()> {
        match self
            .write_state(invocation, owner_epoch, loaded, "result")
            .await?
        {
            StateWrite::Written => Ok(()),
            StateWrite::GenerationMismatch => {
                anyhow::bail!("actor state generation changed during execution")
            }
        }
    }

    async fn write_state(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        loaded: &LoadedState,
        write_kind: &'static str,
    ) -> Result<StateWrite> {
        let started_at = Instant::now();
        let authorization_started_at = Instant::now();
        let write_url = match self
            .authorize_write(invocation, owner_epoch, &loaded.generation)
            .await
        {
            Ok(write_url) => write_url,
            Err(error) => {
                warn!(
                    event = "actor_state_write",
                    request_id = %invocation.request_id,
                    namespace_id = %invocation.actor.namespace_id,
                    actor_type = %invocation.actor.actor_type,
                    actor_id = %invocation.actor.actor_id,
                    host_id = %self.endpoint.id,
                    write_kind,
                    authorization_ms = elapsed_ms(authorization_started_at),
                    total_ms = elapsed_ms(started_at),
                    outcome = "authorization_failed",
                    error = %format!("{error:#}"),
                    "actor state write failed"
                );
                return Err(error);
            }
        };
        let authorization_ms = elapsed_ms(authorization_started_at);
        let encode_started_at = Instant::now();
        let bytes = loaded.log.encode()?;
        let encode_ms = elapsed_ms(encode_started_at);
        let storage_started_at = Instant::now();
        let outcome = self.state.write(&write_url, bytes).await;
        let storage_ms = elapsed_ms(storage_started_at);
        match &outcome {
            Ok(result) => info!(
                event = "actor_state_write",
                request_id = %invocation.request_id,
                namespace_id = %invocation.actor.namespace_id,
                actor_type = %invocation.actor.actor_type,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                write_kind,
                authorization_ms,
                encode_ms,
                storage_ms,
                total_ms = elapsed_ms(started_at),
                outcome = state_write_outcome(result),
                "actor state write completed"
            ),
            Err(error) => warn!(
                event = "actor_state_write",
                request_id = %invocation.request_id,
                namespace_id = %invocation.actor.namespace_id,
                actor_type = %invocation.actor.actor_type,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                write_kind,
                authorization_ms,
                encode_ms,
                storage_ms,
                total_ms = elapsed_ms(started_at),
                outcome = "storage_failed",
                error = %format!("{error:#}"),
                "actor state write failed"
            ),
        }
        outcome
    }

    async fn authorize_write(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        generation: &str,
    ) -> Result<String> {
        self.state_write_authority
            .authorize_state_write(
                &invocation.actor,
                &self.endpoint.id,
                owner_epoch,
                generation,
            )
            .await
    }

    async fn evict(&self, actor: &crate::actor::ActorKey) {
        if let Err(error) = self
            .executor
            .evict(ActorMethodEviction {
                actor: actor.clone(),
            })
            .await
        {
            warn!(error = %format!("{error:#}"), "failed to evict actor after invocation failure");
        }
    }
}

enum OwnershipRead {
    Owned(LoadedState),
    Reroute,
}

#[derive(Default)]
struct InvocationTimings {
    queue_ms: f64,
    state_load_ms: f64,
    actor_execution_ms: f64,
    state_publish_ms: f64,
}

fn actor_execution_outcome(result: &ActorExecutionResult) -> &'static str {
    match result {
        ActorExecutionResult::Completed { .. } => "completed",
        ActorExecutionResult::Failed { .. } => "failed",
        ActorExecutionResult::Reroute => "reroute",
        ActorExecutionResult::HostUnavailable => "host_unavailable",
    }
}

fn actor_execution_failure_code(result: &ActorExecutionResult) -> Option<&str> {
    match result {
        ActorExecutionResult::Failed { failure } => Some(&failure.code),
        _ => None,
    }
}

fn state_write_outcome(result: &StateWrite) -> &'static str {
    match result {
        StateWrite::Written => "written",
        StateWrite::GenerationMismatch => "generation_mismatch",
    }
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn has_newer_owner(loaded: &LoadedState, owner_epoch: u64) -> bool {
    loaded
        .log
        .latest()
        .is_some_and(|record| record.owner_epoch > owner_epoch)
}

fn has_current_owner(loaded: &LoadedState, owner_epoch: u64) -> bool {
    loaded
        .log
        .latest()
        .is_some_and(|record| record.owner_epoch == owner_epoch)
}

fn failed(code: impl Into<String>, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}

fn state_error(error: anyhow::Error) -> anyhow::Error {
    error.context("validate actor state log")
}

struct ActivityGuard<'a> {
    host: &'a ActorHost,
}

impl<'a> ActivityGuard<'a> {
    fn begin(host: &'a ActorHost) -> Self {
        let active = host.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = host.activity_tx.send(active);
        Self { host }
    }
}

impl Drop for ActivityGuard<'_> {
    fn drop(&mut self) {
        let active = self.host.active.fetch_sub(1, Ordering::SeqCst) - 1;
        let _ = self.host.activity_tx.send(active);
        if active == 0 {
            self.host.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::{actor::ActorKey, state_log::StateLog};

    struct FakeExecutor;

    #[async_trait]
    impl ActorExecutor for FakeExecutor {
        fn supports(&self, actor_type: &str) -> bool {
            actor_type == "Counter"
        }

        async fn invoke(&self, _invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome> {
            Ok(ActorMethodOutcome::Completed {
                result: json!({ "count": 1 }),
                state: json!({ "count": 1 }),
            })
        }
    }

    #[derive(Default)]
    struct FakeAuthority {
        generations: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl StateWriteAuthority for FakeAuthority {
        async fn authorize_state_write(
            &self,
            _actor: &ActorKey,
            _host_id: &super::super::HostId,
            _owner_epoch: u64,
            expected_generation: &str,
        ) -> Result<String> {
            self.generations
                .lock()
                .unwrap()
                .push(expected_generation.into());
            Ok("https://state.invalid/write".into())
        }
    }

    #[derive(Default)]
    struct FakeStateTransport {
        body: Mutex<Vec<u8>>,
        generation: Mutex<u64>,
    }

    #[async_trait]
    impl StateTransport for FakeStateTransport {
        async fn read(&self, _signed_url: &str) -> Result<LoadedState> {
            Ok(LoadedState {
                log: StateLog::decode(&self.body.lock().unwrap())?,
                generation: self.generation.lock().unwrap().to_string(),
            })
        }

        async fn write(&self, _signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
            *self.body.lock().unwrap() = bytes;
            *self.generation.lock().unwrap() += 1;
            Ok(StateWrite::Written)
        }
    }

    #[tokio::test]
    async fn actor_execution_uses_the_injected_state_write_authority() -> Result<()> {
        let authority = Arc::new(FakeAuthority::default());
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            Arc::new(FakeExecutor),
            authority.clone(),
            Arc::new(FakeStateTransport::default()),
        );

        let result = host
            .invoke_actor(
                ActorInvocation {
                    request_id: "request-1".into(),
                    actor: ActorKey {
                        namespace_id: "project-1".into(),
                        actor_type: "Counter".into(),
                        actor_id: "counter-1".into(),
                    },
                    method: "increment".into(),
                    args: Vec::new(),
                },
                1,
                "https://state.invalid/read".into(),
            )
            .await?;

        assert_eq!(
            result,
            ActorExecutionResult::Completed {
                result: json!({ "count": 1 })
            }
        );
        assert_eq!(*authority.generations.lock().unwrap(), ["0", "1"]);
        Ok(())
    }
}
