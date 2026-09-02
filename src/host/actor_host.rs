use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Notify, watch};
use tracing::{info, warn};

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure,
        ActorMethodEviction, ActorMethodInvocation, ActorMethodOutcome,
    },
    actor_state::{ActorExecutionAdmission, ActorExecutionLocks, ActorStorageKey},
    control_plane::ControlPlaneClient,
    state_log::StateSnapshot,
    state_transport::{LoadedState, StateTransport, StateWrite},
    storage_urls::StateWriteTicket,
};

use super::HostEndpoint;

const STATE_WRITE_TICKET_SAFETY: Duration = Duration::from_secs(5);

#[async_trait]
pub(crate) trait StateCommitAuthority: Send + Sync {
    async fn prepare_state_write(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<StateWriteTicket>;

    #[allow(clippy::too_many_arguments)]
    async fn commit_state(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
        state_object: &str,
        request_id: &str,
    ) -> Result<CommittedState>;
}

#[derive(Debug)]
pub(crate) struct CommittedState {
    state_version: u64,
    next_write: Option<StateWriteTicket>,
}

#[async_trait]
impl StateCommitAuthority for ControlPlaneClient {
    async fn prepare_state_write(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<StateWriteTicket> {
        ControlPlaneClient::prepare_state_write(self, actor, host_id, owner_epoch, expected_version)
            .await
    }

    async fn commit_state(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
        state_object: &str,
        request_id: &str,
    ) -> Result<CommittedState> {
        let (state_version, next_write) = ControlPlaneClient::commit_state(
            self,
            actor,
            host_id,
            owner_epoch,
            expected_version,
            state_object,
            request_id,
        )
        .await?;
        Ok(CommittedState {
            state_version,
            next_write,
        })
    }
}

pub(crate) struct ActorHost {
    endpoint: HostEndpoint,
    namespace_id: String,
    executor: Arc<dyn ActorExecutor>,
    commits: Arc<dyn StateCommitAuthority>,
    state: Arc<dyn StateTransport>,
    executions: ActorExecutionLocks,
    cached_state: Mutex<HashMap<ActorStorageKey, CachedActorState>>,
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
        commits: Arc<dyn StateCommitAuthority>,
        state: Arc<dyn StateTransport>,
    ) -> Self {
        let (activity_tx, _) = watch::channel(0);
        Self {
            endpoint,
            namespace_id,
            executor,
            commits,
            state,
            executions: ActorExecutionLocks::new(),
            cached_state: Mutex::new(HashMap::new()),
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
        state_version: u64,
        state_read_url: String,
    ) -> Result<ActorExecutionResult> {
        let started_at = Instant::now();
        let mut timings = InvocationTimings::default();
        let outcome = self
            .invoke_actor_once(
                &invocation,
                owner_epoch,
                state_version,
                &state_read_url,
                &mut timings,
            )
            .await;
        self.log_invocation(&invocation, started_at, &timings, &outcome);
        outcome
    }

    async fn invoke_actor_once(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        state_version: u64,
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
        let mut cached = self
            .take_or_load_state(&object, owner_epoch, state_version, state_read_url)
            .await?;
        timings.state_load_ms = elapsed_ms(state_load_started_at);
        if let Err(error) = self.finish_pending_commit(invocation, &mut cached).await {
            self.store_cached_state(object, cached)?;
            warn!(
                actor = %invocation.actor.storage_key(),
                error = %format!("{error:#}"),
                "pending actor state commit remains unresolved"
            );
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }
        if let Some(result) = cached.replay(&invocation.request_id) {
            self.store_cached_state(object, cached)?;
            return Ok(ActorExecutionResult::Completed { result });
        }

        let execution_started_at = Instant::now();
        let executed = self.execute_method(invocation, cached.state()).await;
        timings.actor_execution_ms = elapsed_ms(execution_started_at);
        let (result, next_state) = match executed {
            Ok(outcome) => outcome,
            Err(failure) => {
                self.store_cached_state(object, cached)?;
                return Ok(failure);
            }
        };
        if cached.state.as_ref() == Some(&next_state) {
            self.store_cached_state(object, cached)?;
            return Ok(ActorExecutionResult::Completed { result });
        }

        let state_publish_started_at = Instant::now();
        let published = self
            .publish_result(invocation, owner_epoch, &mut cached, result, next_state)
            .await;
        timings.state_publish_ms = elapsed_ms(state_publish_started_at);
        if published.is_err() {
            self.evict(&invocation.actor).await;
        }
        self.store_cached_state(object, cached)?;
        match published {
            Ok(result) => Ok(result),
            Err(error) => {
                warn!(
                    actor = %invocation.actor.storage_key(),
                    error = %format!("{error:#}"),
                    "actor completed but state publication could not be confirmed"
                );
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                })
            }
        }
    }

    async fn take_or_load_state(
        &self,
        object: &ActorStorageKey,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: &str,
    ) -> Result<CachedActorState> {
        if let Some(cached) = self.take_cached_state(object)?
            && cached.owner_epoch == owner_epoch
        {
            return Ok(cached);
        }
        if state_version == 0 {
            ensure!(
                state_read_url.is_empty(),
                "uninitialized actor has a state URL"
            );
            return Ok(CachedActorState::new(owner_epoch));
        }
        ensure!(
            !state_read_url.is_empty(),
            "initialized actor has no state URL"
        );
        let loaded = self
            .state
            .read(state_read_url)
            .await
            .context("load actor state")?;
        CachedActorState::from_loaded(owner_epoch, state_version, loaded)
    }

    fn take_cached_state(&self, object: &ActorStorageKey) -> Result<Option<CachedActorState>> {
        Ok(self
            .cached_state
            .lock()
            .map_err(|_| anyhow::anyhow!("actor state cache lock poisoned"))?
            .remove(object))
    }

    fn store_cached_state(&self, object: ActorStorageKey, state: CachedActorState) -> Result<()> {
        self.cached_state
            .lock()
            .map_err(|_| anyhow::anyhow!("actor state cache lock poisoned"))?
            .insert(object, state);
        Ok(())
    }

    async fn execute_method(
        &self,
        invocation: &ActorInvocation,
        state: Option<&Value>,
    ) -> std::result::Result<(Value, Value), ActorExecutionResult> {
        let outcome = self
            .executor
            .invoke(ActorMethodInvocation {
                request_id: invocation.request_id.clone(),
                actor: invocation.actor.clone(),
                method: invocation.method.clone(),
                args: invocation.args.clone(),
                state: state.cloned(),
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

    async fn publish_result(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut CachedActorState,
        result: Value,
        next_state: Value,
    ) -> Result<ActorExecutionResult> {
        let started_at = Instant::now();
        let next_version = cached
            .state_version
            .checked_add(1)
            .context("actor state version overflow")?;
        let prepare_started_at = Instant::now();
        let ticket = match cached.next_write.take() {
            Some(ticket)
                if ticket.state_version == next_version
                    && ticket.expires_at_ms
                        > unix_millis()?.saturating_add(i64::try_from(
                            STATE_WRITE_TICKET_SAFETY.as_millis(),
                        )?) =>
            {
                ticket
            }
            _ => {
                self.commits
                    .prepare_state_write(
                        &invocation.actor,
                        &self.endpoint.id,
                        owner_epoch,
                        cached.state_version,
                    )
                    .await?
            }
        };
        ensure!(
            ticket.state_version == next_version,
            "state write ticket has the wrong version"
        );
        let prepare_ms = elapsed_ms(prepare_started_at);
        let snapshot = StateSnapshot::new(
            next_version,
            owner_epoch,
            invocation.request_id.clone(),
            next_state,
            result.clone(),
        )?;
        let encode_started_at = Instant::now();
        let bytes = snapshot.encode()?;
        let encode_ms = elapsed_ms(encode_started_at);
        let storage_started_at = Instant::now();
        let write = self.state.write(&ticket.url, bytes).await?;
        let storage_ms = elapsed_ms(storage_started_at);
        ensure!(
            matches!(write, StateWrite::Written | StateWrite::AlreadyExists),
            "actor snapshot was not stored"
        );
        cached.pending = Some(PendingStateCommit { snapshot, ticket });
        let commit_started_at = Instant::now();
        self.finish_pending_commit(invocation, cached).await?;
        let commit_ms = elapsed_ms(commit_started_at);
        info!(
            event = "actor_state_write",
            request_id = %invocation.request_id,
            namespace_id = %invocation.actor.namespace_id,
            actor_type = %invocation.actor.actor_type,
            actor_id = %invocation.actor.actor_id,
            host_id = %self.endpoint.id,
            owner_epoch,
            state_version = next_version,
            prepare_ms,
            encode_ms,
            storage_ms,
            commit_ms,
            total_ms = elapsed_ms(started_at),
            outcome = "committed",
            "immutable actor state committed"
        );
        Ok(ActorExecutionResult::Completed { result })
    }

    async fn finish_pending_commit(
        &self,
        invocation: &ActorInvocation,
        cached: &mut CachedActorState,
    ) -> Result<()> {
        let Some(pending) = &cached.pending else {
            return Ok(());
        };
        let committed = self
            .commits
            .commit_state(
                &invocation.actor,
                &self.endpoint.id,
                cached.owner_epoch,
                cached.state_version,
                &pending.ticket.object_name,
                &pending.snapshot.request_id,
            )
            .await?;
        ensure!(
            committed.state_version == pending.snapshot.state_version,
            "control plane committed the wrong actor state version"
        );
        let pending = cached.pending.take().expect("pending commit checked above");
        cached.state_version = pending.snapshot.state_version;
        cached.state = Some(pending.snapshot.state);
        cached.last_request_id = Some(pending.snapshot.request_id);
        cached.last_result = Some(pending.snapshot.result);
        cached.next_write = committed.next_write;
        Ok(())
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

struct CachedActorState {
    owner_epoch: u64,
    state_version: u64,
    state: Option<Value>,
    last_request_id: Option<String>,
    last_result: Option<Value>,
    next_write: Option<StateWriteTicket>,
    pending: Option<PendingStateCommit>,
}

struct PendingStateCommit {
    snapshot: StateSnapshot,
    ticket: StateWriteTicket,
}

impl CachedActorState {
    fn new(owner_epoch: u64) -> Self {
        Self {
            owner_epoch,
            state_version: 0,
            state: None,
            last_request_id: None,
            last_result: None,
            next_write: None,
            pending: None,
        }
    }

    fn from_loaded(owner_epoch: u64, state_version: u64, loaded: LoadedState) -> Result<Self> {
        ensure!(
            loaded.snapshot.state_version == state_version,
            "actor snapshot version does not match its state head"
        );
        ensure!(
            loaded.snapshot.owner_epoch <= owner_epoch,
            "actor snapshot belongs to a newer owner epoch"
        );
        Ok(Self {
            owner_epoch,
            state_version,
            state: Some(loaded.snapshot.state),
            last_request_id: Some(loaded.snapshot.request_id),
            last_result: Some(loaded.snapshot.result),
            next_write: None,
            pending: None,
        })
    }

    fn state(&self) -> Option<&Value> {
        self.state.as_ref()
    }

    fn replay(&self, request_id: &str) -> Option<Value> {
        (self.last_request_id.as_deref() == Some(request_id))
            .then(|| self.last_result.clone())
            .flatten()
    }
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

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn unix_millis() -> Result<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis(),
    )
    .context("system clock exceeds supported state-write timestamp range")
}

fn failed(code: impl Into<String>, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::json;

    use super::*;
    use crate::actor::ActorKey;

    struct IncrementingExecutor {
        invocations: AtomicU64,
    }

    #[async_trait]
    impl ActorExecutor for IncrementingExecutor {
        fn supports(&self, actor_type: &str) -> bool {
            actor_type == "Counter"
        }

        async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome> {
            self.invocations.fetch_add(1, Ordering::Relaxed);
            let count = invocation
                .state
                .as_ref()
                .and_then(|state| state.get("count"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            Ok(ActorMethodOutcome::Completed {
                result: json!(count),
                state: json!({ "count": count }),
            })
        }
    }

    #[derive(Default)]
    struct FakeAuthority {
        preparations: Mutex<Vec<u64>>,
        commits: Mutex<Vec<u64>>,
        commit_failures: AtomicUsize,
    }

    #[async_trait]
    impl StateCommitAuthority for FakeAuthority {
        async fn prepare_state_write(
            &self,
            _actor: &ActorKey,
            _host_id: &super::super::HostId,
            _owner_epoch: u64,
            expected_version: u64,
        ) -> Result<StateWriteTicket> {
            self.preparations.lock().unwrap().push(expected_version);
            Ok(ticket(expected_version + 1))
        }

        async fn commit_state(
            &self,
            _actor: &ActorKey,
            _host_id: &super::super::HostId,
            _owner_epoch: u64,
            expected_version: u64,
            _state_object: &str,
            _request_id: &str,
        ) -> Result<CommittedState> {
            self.commits.lock().unwrap().push(expected_version);
            if self
                .commit_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                anyhow::bail!("commit response was lost");
            }
            Ok(CommittedState {
                state_version: expected_version + 1,
                next_write: Some(ticket(expected_version + 2)),
            })
        }
    }

    #[derive(Default)]
    struct FakeStateTransport {
        writes: Mutex<Vec<Vec<u8>>>,
        reads: AtomicUsize,
    }

    #[async_trait]
    impl StateTransport for FakeStateTransport {
        async fn read(&self, _signed_url: &str) -> Result<LoadedState> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("new actor should not read storage")
        }

        async fn write(&self, _signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
            self.writes.lock().unwrap().push(bytes);
            Ok(StateWrite::Written)
        }
    }

    #[tokio::test]
    async fn resident_actor_uses_immutable_snapshots_and_replays_the_last_request() -> Result<()> {
        let authority = Arc::new(FakeAuthority::default());
        let state = Arc::new(FakeStateTransport::default());
        let executor = Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        });
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            executor.clone(),
            authority.clone(),
            state.clone(),
        );

        assert_eq!(invoke(&host, "request-1").await?, completed(1));
        assert_eq!(invoke(&host, "request-2").await?, completed(2));
        assert_eq!(invoke(&host, "request-2").await?, completed(2));

        assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);
        assert_eq!(state.reads.load(Ordering::Relaxed), 0);
        assert_eq!(state.writes.lock().unwrap().len(), 2);
        assert_eq!(*authority.preparations.lock().unwrap(), [0]);
        assert_eq!(*authority.commits.lock().unwrap(), [0, 1]);
        let snapshots = state
            .writes
            .lock()
            .unwrap()
            .iter()
            .map(|bytes| StateSnapshot::decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(snapshots[0].state_version, 1);
        assert_eq!(snapshots[1].state_version, 2);
        Ok(())
    }

    #[tokio::test]
    async fn retries_an_ambiguous_commit_without_executing_the_request_twice() -> Result<()> {
        let authority = Arc::new(FakeAuthority::default());
        authority.commit_failures.store(1, Ordering::SeqCst);
        let state = Arc::new(FakeStateTransport::default());
        let executor = Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        });
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            executor.clone(),
            authority.clone(),
            state.clone(),
        );

        let first = invoke(&host, "request-1").await?;
        assert!(matches!(
            first,
            ActorExecutionResult::Failed { ref failure } if failure.code == "outcome_unknown"
        ));
        assert_eq!(invoke(&host, "request-1").await?, completed(1));

        assert_eq!(executor.invocations.load(Ordering::Relaxed), 1);
        assert_eq!(state.writes.lock().unwrap().len(), 1);
        assert_eq!(*authority.commits.lock().unwrap(), [0, 0]);
        Ok(())
    }

    async fn invoke(host: &ActorHost, request_id: &str) -> Result<ActorExecutionResult> {
        host.invoke_actor(
            ActorInvocation {
                request_id: request_id.into(),
                actor: ActorKey {
                    namespace_id: "project-1".into(),
                    actor_type: "Counter".into(),
                    actor_id: "counter-1".into(),
                },
                method: "increment".into(),
                args: Vec::new(),
            },
            1,
            0,
            String::new(),
        )
        .await
    }

    fn completed(count: u64) -> ActorExecutionResult {
        ActorExecutionResult::Completed {
            result: json!(count),
        }
    }

    fn ticket(state_version: u64) -> StateWriteTicket {
        StateWriteTicket {
            state_version,
            object_name: format!("snapshots/{state_version}.json"),
            url: format!("https://state.invalid/{state_version}"),
            expires_at_ms: i64::MAX,
        }
    }
}
