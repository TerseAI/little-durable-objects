use std::{
    future::Future,
    ops::Deref,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationDeadline,
        ActorInvocationFailure, ActorMethodCancellation, ActorMethodEviction,
        ActorMethodInvocation, ActorMethodOutcome, ActorScope,
    },
    actor_state::{
        ActorDatabaseStore, ActorExecutionAdmission, ActorExecutionGuard, ActorExecutionLocks,
        ActorOwner, ActorRestoreCache, ActorStorageKey, SqliteActorDatabase,
    },
    clock::{Clock, SystemClock},
    durability::{
        ActorChangeCapture, ActorDurabilityStore, ActorStateRestorer, CapturedActorChanges,
        OwnershipClaimResult, StatePublicationStatus, VersionedActorManifest,
    },
    host_leases::HostLeaseStore,
    telemetry::{
        ActorExecutionKind, ActorExecutionTelemetry, ActorSystemRole, ActorTelemetry,
        ActorTelemetryEvent, ActorTelemetryScope, elapsed_ms, noop_actor_telemetry,
    },
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use super::{ConfirmedLeaseState, HostEndpoint, HostId};

const DEFAULT_PUBLICATION_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_PUBLICATION_RETRY_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_ACTOR_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActorDrainReason {
    Shutdown,
    LeaseLost,
    Idle,
}

impl ActorDrainReason {
    fn description(self) -> &'static str {
        match self {
            Self::Shutdown => "the actor host is shutting down",
            Self::LeaseLost => "the actor host lost its lease",
            Self::Idle => "the actor host reached its idle timeout",
        }
    }
}

#[derive(Clone)]
pub struct ActorHost {
    endpoint: HostEndpoint,
    dependencies: ActorHostDependencies,
    actor_tasks: Arc<ActorTaskTracker>,
}

#[derive(Clone)]
pub struct ActorHostDependencies {
    durability: Arc<dyn ActorDurabilityStore>,
    leases: Arc<dyn HostLeaseStore>,
    databases: Arc<ActorDatabaseStore>,
    change_capture: Arc<dyn ActorChangeCapture>,
    state_restorer: Arc<dyn ActorStateRestorer>,
    execution_locks: Arc<ActorExecutionLocks>,
    restore_cache: Arc<ActorRestoreCache>,
    clock: Arc<dyn Clock>,
    confirmed_lease: Option<Arc<ConfirmedLeaseState>>,
    actor_executor: Option<Arc<dyn ActorExecutor>>,
    actor_scope: Option<ActorScope>,
    actor_timeouts: ActorRuntimeTimeouts,
    telemetry: Arc<dyn ActorTelemetry>,
}

#[derive(Clone, Copy)]
struct ActorRuntimeTimeouts {
    publication_probe: Duration,
    publication_retry: Duration,
    cleanup: Duration,
}

impl Default for ActorRuntimeTimeouts {
    fn default() -> Self {
        Self {
            publication_probe: DEFAULT_PUBLICATION_PROBE_TIMEOUT,
            publication_retry: DEFAULT_PUBLICATION_RETRY_TIMEOUT,
            cleanup: DEFAULT_ACTOR_CLEANUP_TIMEOUT,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct ExecutionPhaseTimings {
    executor_ms: f64,
    capture_ms: f64,
    publish_ms: f64,
    checkpoint_ms: f64,
}

type ActorOperationResult = OwnershipGuardResult<ActorLocalResult>;

enum ActorInvocationStep<T> {
    Continue(T),
    Complete(ActorOperationResult),
}

struct ActorLockAdmission {
    guard: ActorExecutionGuard,
    queue_wait_ms: f64,
    execution_started: Instant,
}

struct PreparedActorState {
    manifest: VersionedActorManifest,
    database: SqliteActorDatabase,
    cold_start: bool,
    actor_ready_ms: f64,
}

struct ExecutedActorInvocation {
    result: ActorLocalResult,
    proposed_state: Option<Value>,
    initializes_state: bool,
    state_changed: bool,
    executor_ms: f64,
}

struct ActorInvocationDurability {
    manifest: VersionedActorManifest,
    database: SqliteActorDatabase,
    receipts: ActorInvocationReceipts,
    execution: ExecutedActorInvocation,
    deadline: ActorInvocationDeadline,
    execution_started: Instant,
    measurements: ActorExecutionMeasurements,
}

struct Timed<T> {
    value: T,
    elapsed_ms: f64,
}

#[derive(Clone, Copy)]
struct ActorExecutionMeasurements {
    started: Instant,
    queue_wait_ms: f64,
    actor_ready_ms: f64,
    phases: ExecutionPhaseTimings,
    cold_start: bool,
    state_changed: bool,
    receipt_replay: bool,
}

#[derive(Clone, Copy)]
enum ActorExecutionStop {
    Drain(ActorDrainReason),
    Deadline,
}

enum ActorExecutorWait {
    Completed(Result<ActorMethodOutcome>),
    Stopped(ActorExecutionStop),
}

impl ActorHostDependencies {
    pub fn new(
        durability: Arc<dyn ActorDurabilityStore>,
        leases: Arc<dyn HostLeaseStore>,
        databases: Arc<ActorDatabaseStore>,
        change_capture: Arc<dyn ActorChangeCapture>,
        state_restorer: Arc<dyn ActorStateRestorer>,
    ) -> Self {
        Self {
            durability,
            leases,
            databases,
            change_capture,
            state_restorer,
            execution_locks: Arc::new(ActorExecutionLocks::new()),
            restore_cache: Arc::new(ActorRestoreCache::new()),
            clock: Arc::new(SystemClock),
            confirmed_lease: None,
            actor_executor: None,
            actor_scope: None,
            actor_timeouts: ActorRuntimeTimeouts::default(),
            telemetry: noop_actor_telemetry(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub(crate) fn with_confirmed_lease(
        mut self,
        confirmed_lease: Arc<ConfirmedLeaseState>,
    ) -> Self {
        self.confirmed_lease = Some(confirmed_lease);
        self
    }

    pub fn with_actor_executor(
        mut self,
        actor_scope: ActorScope,
        actor_executor: Arc<dyn ActorExecutor>,
    ) -> Self {
        self.actor_scope = Some(actor_scope);
        self.actor_executor = Some(actor_executor);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Arc<dyn ActorTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_actor_timeouts(
        mut self,
        publication_probe: Duration,
        publication_retry: Duration,
        cleanup: Duration,
    ) -> Self {
        self.actor_timeouts = ActorRuntimeTimeouts {
            publication_probe,
            publication_retry,
            cleanup,
        };
        self
    }

    pub(crate) fn lease_store(&self) -> Arc<dyn HostLeaseStore> {
        self.leases.clone()
    }

    pub(crate) fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }
}

impl Deref for ActorHost {
    type Target = ActorHostDependencies;

    fn deref(&self) -> &Self::Target {
        &self.dependencies
    }
}

impl ActorHost {
    pub fn new(endpoint: HostEndpoint, dependencies: ActorHostDependencies) -> Self {
        Self {
            endpoint,
            dependencies,
            actor_tasks: Arc::new(ActorTaskTracker::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn host_id(&self) -> &HostId {
        &self.endpoint.id
    }

    pub(crate) async fn drain_actor_invocations(
        &self,
        reason: ActorDrainReason,
        timeout: Duration,
    ) -> Result<()> {
        self.actor_tasks.begin_drain(reason)?;
        match tokio::time::timeout(timeout, self.actor_tasks.wait_until_empty()).await {
            Ok(()) => {
                info!(?reason, "actor invocation tasks drained");
                Ok(())
            }
            Err(_) => anyhow::bail!(
                "actor invocations did not drain within {}ms after {reason:?}",
                timeout.as_millis()
            ),
        }
    }

    pub(crate) fn activity(&self) -> watch::Receiver<usize> {
        self.actor_tasks.activity.subscribe()
    }

    fn open_actor_database(&self, object: &ActorStorageKey) -> Result<SqliteActorDatabase> {
        self.databases.open(object)
    }

    /// Invoke customer actor code in this sandbox when this host owns the actor's
    /// durable object. Non-owners return a routing result without contacting the
    /// attached customer JavaScript process.
    #[tracing::instrument(
        name = "actor.request",
        skip(self, invocation),
        fields(
            host_id = %self.endpoint.id,
            request_id = %invocation.request_id,
            namespace = %invocation.actor.namespace_id,
            actor_type = %invocation.actor.actor_type,
            actor_id = %invocation.actor.actor_id,
            method = %invocation.method
        )
    )]
    pub(crate) async fn invoke_actor(
        &self,
        invocation: ActorInvocation,
    ) -> Result<ActorExecutionResult> {
        let actor_scope = self
            .actor_scope
            .as_ref()
            .context("this host has no actor scope configured")?;
        anyhow::ensure!(
            actor_scope.contains(&invocation.actor),
            "actor namespace does not match this host"
        );
        let deadline = ActorInvocationDeadline::from_timeout_ms(invocation.timeout_ms);
        let object = invocation.actor.storage_key();
        let task = match self.actor_tasks.start()? {
            ActorTaskAdmission::Start(task) => task,
            ActorTaskAdmission::Draining(reason) => {
                debug!(
                    ?reason,
                    "rejected actor invocation because the host is draining"
                );
                return Ok(ActorExecutionResult::HostUnavailable);
            }
        };
        let host = self.clone();
        let drain = self.actor_tasks.drain_signal();
        let execution = tokio::spawn(async move {
            let _task = task;
            host.run_actor_invocation(&object, invocation, deadline, drain)
                .await
        });
        let operation = match tokio::time::timeout_at(deadline.at(), execution).await {
            Ok(Ok(result)) => result?,
            Ok(Err(error)) => {
                return Err(anyhow::anyhow!("actor invocation task failed: {error}"));
            }
            Err(_) => {
                return Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::deadline_exceeded_while_waiting(),
                });
            }
        };
        let result = match operation {
            OwnershipGuardResult::Completed(ActorLocalResult::Completed { result }) => {
                ActorExecutionResult::Completed { result }
            }
            OwnershipGuardResult::Completed(ActorLocalResult::Failed { failure }) => {
                ActorExecutionResult::Failed { failure }
            }
            OwnershipGuardResult::NotOwner(_) | OwnershipGuardResult::Conflict(_) => {
                ActorExecutionResult::Reroute
            }
        };
        Ok(result)
    }

    async fn run_actor_invocation(
        &self,
        object: &ActorStorageKey,
        invocation: ActorInvocation,
        deadline: ActorInvocationDeadline,
        mut drain: watch::Receiver<Option<ActorDrainReason>>,
    ) -> Result<OwnershipGuardResult<ActorLocalResult>> {
        let telemetry_started = Instant::now();
        if let Some(reason) = *drain.borrow() {
            return Ok(cancelled_before_execution(reason));
        }
        let executor = match self.resolve_actor_executor(&invocation)? {
            ActorInvocationStep::Continue(executor) => executor,
            ActorInvocationStep::Complete(result) => return Ok(result),
        };
        let admission = match self
            .acquire_actor_lock(object, deadline, &mut drain)
            .await?
        {
            ActorInvocationStep::Continue(admission) => admission,
            ActorInvocationStep::Complete(result) => return Ok(result),
        };
        let ActorLockAdmission {
            guard: _execution,
            queue_wait_ms,
            execution_started,
        } = admission;

        let prepared = match self
            .prepare_actor_object(object, deadline, &mut drain)
            .await?
        {
            ActorInvocationStep::Continue(prepared) => prepared,
            ActorInvocationStep::Complete(result) => return Ok(result),
        };
        let measurements = ActorExecutionMeasurements {
            started: telemetry_started,
            queue_wait_ms,
            actor_ready_ms: prepared.actor_ready_ms,
            phases: ExecutionPhaseTimings::default(),
            cold_start: prepared.cold_start,
            state_changed: false,
            receipt_replay: false,
        };

        let receipts = match self
            .resolve_actor_receipt(
                &prepared.database,
                &prepared.manifest,
                &invocation,
                deadline,
                &mut drain,
                measurements,
            )
            .await?
        {
            ActorInvocationStep::Continue(receipts) => receipts,
            ActorInvocationStep::Complete(result) => return Ok(result),
        };

        let execution = match self
            .execute_actor_method(
                object,
                &prepared.database,
                &executor,
                &invocation,
                deadline,
                &mut drain,
            )
            .await?
        {
            ActorInvocationStep::Continue(execution) => execution,
            ActorInvocationStep::Complete(result) => return Ok(result),
        };
        let durability = ActorInvocationDurability {
            manifest: prepared.manifest,
            database: prepared.database,
            receipts,
            execution,
            deadline,
            execution_started,
            measurements,
        };
        self.persist_actor_invocation(object, &invocation, durability)
            .await
    }

    fn resolve_actor_executor(
        &self,
        invocation: &ActorInvocation,
    ) -> Result<ActorInvocationStep<Arc<dyn ActorExecutor>>> {
        let executor = self
            .actor_executor
            .clone()
            .context("this host has no attached actor executor")?;
        if executor.supports(&invocation.actor.actor_type) {
            return Ok(ActorInvocationStep::Continue(executor));
        }
        Ok(ActorInvocationStep::Complete(
            OwnershipGuardResult::Completed(ActorLocalResult::Failed {
                failure: ActorInvocationFailure {
                    code: "actor_type_not_loaded".into(),
                    message: format!(
                        "actor type {} is not loaded in this customer process",
                        invocation.actor.actor_type
                    ),
                },
            }),
        ))
    }

    async fn acquire_actor_lock(
        &self,
        object: &ActorStorageKey,
        deadline: ActorInvocationDeadline,
        drain: &mut watch::Receiver<Option<ActorDrainReason>>,
    ) -> Result<ActorInvocationStep<ActorLockAdmission>> {
        let queued_at = Instant::now();
        debug!("waiting for exclusive actor execution gate");
        let admission =
            match before_actor_execution(deadline, drain, self.execution_locks.admit(object))
                .await?
            {
                BeforeActorExecution::Completed(admission) => admission,
                BeforeActorExecution::DeadlineExceeded => {
                    debug!("actor invocation expired in the execution queue");
                    return Ok(ActorInvocationStep::Complete(
                        deadline_exceeded_before_execution(),
                    ));
                }
                BeforeActorExecution::Draining(reason) => {
                    return Ok(ActorInvocationStep::Complete(cancelled_before_execution(
                        reason,
                    )));
                }
            };
        let guard = match admission {
            ActorExecutionAdmission::Acquired(guard) => guard,
            ActorExecutionAdmission::Full => {
                return Ok(ActorInvocationStep::Complete(
                    OwnershipGuardResult::Completed(ActorLocalResult::Failed {
                        failure: ActorInvocationFailure::resource_exhausted_before_execution(),
                    }),
                ));
            }
        };
        let queue_wait_ms = queued_at.elapsed().as_secs_f64() * 1_000.0;
        debug!(queue_wait_ms, "acquired exclusive actor execution gate");
        Ok(ActorInvocationStep::Continue(ActorLockAdmission {
            guard,
            queue_wait_ms,
            execution_started: Instant::now(),
        }))
    }

    async fn prepare_actor_object(
        &self,
        object: &ActorStorageKey,
        deadline: ActorInvocationDeadline,
        drain: &mut watch::Receiver<Option<ActorDrainReason>>,
    ) -> Result<ActorInvocationStep<PreparedActorState>> {
        let manifest =
            match before_actor_execution(deadline, drain, self.resolve_actor_ownership(object))
                .await?
            {
                BeforeActorExecution::Completed(manifest) => manifest,
                BeforeActorExecution::DeadlineExceeded => {
                    debug!("actor invocation expired while resolving ownership");
                    return Ok(ActorInvocationStep::Complete(
                        deadline_exceeded_before_execution(),
                    ));
                }
                BeforeActorExecution::Draining(reason) => {
                    return Ok(ActorInvocationStep::Complete(cancelled_before_execution(
                        reason,
                    )));
                }
            };
        let manifest = match manifest {
            OwnershipResolution::Owned(manifest) => manifest,
            OwnershipResolution::NotOwner(owner) => {
                return Ok(ActorInvocationStep::Complete(
                    OwnershipGuardResult::NotOwner(owner),
                ));
            }
            OwnershipResolution::Conflict(owner) => {
                return Ok(ActorInvocationStep::Complete(
                    OwnershipGuardResult::Conflict(owner),
                ));
            }
        };
        let actor_ready_started = Instant::now();
        let cold_start = match before_actor_execution(
            deadline,
            drain,
            self.ensure_actor_state_ready(object, &manifest),
        )
        .await?
        {
            BeforeActorExecution::Completed(cold_start) => cold_start,
            BeforeActorExecution::DeadlineExceeded => {
                debug!("actor invocation expired while restoring durable state");
                return Ok(ActorInvocationStep::Complete(
                    deadline_exceeded_before_execution(),
                ));
            }
            BeforeActorExecution::Draining(reason) => {
                return Ok(ActorInvocationStep::Complete(cancelled_before_execution(
                    reason,
                )));
            }
        };
        Ok(ActorInvocationStep::Continue(PreparedActorState {
            manifest,
            database: self.open_actor_database(object)?,
            cold_start,
            actor_ready_ms: elapsed_ms(actor_ready_started),
        }))
    }

    async fn resolve_actor_receipt(
        &self,
        database: &SqliteActorDatabase,
        manifest: &VersionedActorManifest,
        invocation: &ActorInvocation,
        deadline: ActorInvocationDeadline,
        drain: &mut watch::Receiver<Option<ActorDrainReason>>,
        measurements: ActorExecutionMeasurements,
    ) -> Result<ActorInvocationStep<ActorInvocationReceipts>> {
        let receipts = database
            .get_bytes(ACTOR_RECEIPTS_KEY)?
            .map(|bytes| serde_json::from_slice::<ActorInvocationReceipts>(&bytes))
            .transpose()
            .context("decode durable actor invocation receipts")?
            .unwrap_or_default();
        match receipts.lookup(invocation) {
            ActorReceiptLookup::Missing => Ok(ActorInvocationStep::Continue(receipts)),
            ActorReceiptLookup::Replay(outcome) => {
                match self
                    .confirm_actor_ownership_before_result(deadline, drain)
                    .await?
                {
                    ActorInvocationStep::Continue(()) => {}
                    ActorInvocationStep::Complete(result) => {
                        return Ok(ActorInvocationStep::Complete(result));
                    }
                }
                info!(
                    durable_txid = manifest.max_txid(),
                    "replayed durable actor invocation receipt"
                );
                self.publish_actor_execution(
                    invocation,
                    ActorExecutionMeasurements {
                        receipt_replay: true,
                        ..measurements
                    },
                    &outcome,
                );
                Ok(ActorInvocationStep::Complete(
                    OwnershipGuardResult::Completed(outcome),
                ))
            }
            ActorReceiptLookup::Conflict => {
                match self
                    .confirm_actor_ownership_before_result(deadline, drain)
                    .await?
                {
                    ActorInvocationStep::Continue(()) => {}
                    ActorInvocationStep::Complete(result) => {
                        return Ok(ActorInvocationStep::Complete(result));
                    }
                }
                Ok(ActorInvocationStep::Complete(
                    OwnershipGuardResult::Completed(ActorLocalResult::Failed {
                        failure: idempotency_key_reused_failure(invocation),
                    }),
                ))
            }
        }
    }

    async fn confirm_actor_ownership_before_result(
        &self,
        deadline: ActorInvocationDeadline,
        drain: &mut watch::Receiver<Option<ActorDrainReason>>,
    ) -> Result<ActorInvocationStep<()>> {
        Ok(
            match before_actor_execution(deadline, drain, self.require_active_host_lease()).await? {
                BeforeActorExecution::Completed(()) => ActorInvocationStep::Continue(()),
                BeforeActorExecution::DeadlineExceeded => {
                    ActorInvocationStep::Complete(deadline_exceeded_before_execution())
                }
                BeforeActorExecution::Draining(reason) => {
                    ActorInvocationStep::Complete(cancelled_before_execution(reason))
                }
            },
        )
    }

    async fn execute_actor_method(
        &self,
        object: &ActorStorageKey,
        database: &SqliteActorDatabase,
        executor: &Arc<dyn ActorExecutor>,
        invocation: &ActorInvocation,
        deadline: ActorInvocationDeadline,
        drain: &mut watch::Receiver<Option<ActorDrainReason>>,
    ) -> Result<ActorInvocationStep<ExecutedActorInvocation>> {
        let state = database
            .get_bytes(ACTOR_STATE_KEY)?
            .map(|bytes| serde_json::from_slice::<Value>(&bytes))
            .transpose()
            .context("decode durable actor state")?;
        match before_actor_execution(deadline, drain, self.change_capture.prepare(object)).await? {
            BeforeActorExecution::Completed(()) => {}
            BeforeActorExecution::DeadlineExceeded => {
                debug!("actor invocation expired during LTX preparation");
                return Ok(ActorInvocationStep::Complete(
                    deadline_exceeded_before_execution(),
                ));
            }
            BeforeActorExecution::Draining(reason) => {
                return Ok(ActorInvocationStep::Complete(cancelled_before_execution(
                    reason,
                )));
            }
        }
        let executor_started = Instant::now();
        let outcome = match self
            .invoke_customer_actor(executor, invocation, state.clone(), deadline, drain)
            .await?
        {
            ActorInvocationStep::Continue(outcome) => outcome,
            ActorInvocationStep::Complete(result) => {
                return Ok(ActorInvocationStep::Complete(result));
            }
        };
        let executor_ms = elapsed_ms(executor_started);
        let (result, proposed_state) = match outcome {
            ActorMethodOutcome::Completed { result, state } => {
                (ActorLocalResult::Completed { result }, Some(state))
            }
            ActorMethodOutcome::Failed(failure) if failure.code == "resource_exhausted" => {
                return Ok(ActorInvocationStep::Complete(
                    OwnershipGuardResult::Completed(ActorLocalResult::Failed { failure }),
                ));
            }
            ActorMethodOutcome::Failed(failure) => (ActorLocalResult::Failed { failure }, None),
        };
        let initializes_state = state.is_none() && proposed_state.is_some();
        let state_changed = proposed_state
            .as_ref()
            .is_some_and(|proposed_state| state.as_ref() != Some(proposed_state));
        Ok(ActorInvocationStep::Continue(ExecutedActorInvocation {
            result,
            proposed_state,
            initializes_state,
            state_changed,
            executor_ms,
        }))
    }

    async fn invoke_customer_actor(
        &self,
        executor: &Arc<dyn ActorExecutor>,
        invocation: &ActorInvocation,
        state: Option<Value>,
        deadline: ActorInvocationDeadline,
        drain: &mut watch::Receiver<Option<ActorDrainReason>>,
    ) -> Result<ActorInvocationStep<ActorMethodOutcome>> {
        let executor_invocation = ActorMethodInvocation {
            request_id: invocation.request_id.clone(),
            actor: invocation.actor.clone(),
            method: invocation.method.clone(),
            args: invocation.args.clone(),
            state,
            timeout_ms: deadline.remaining_ms().max(1),
        };
        let mut executor_call = executor.invoke(executor_invocation);
        let wait = tokio::select! {
            biased;
            reason = wait_for_actor_drain(drain) => {
                ActorExecutorWait::Stopped(ActorExecutionStop::Drain(reason))
            }
            result = tokio::time::timeout_at(deadline.at(), &mut executor_call) => {
                match result {
                    Ok(outcome) => ActorExecutorWait::Completed(outcome),
                    Err(_) => ActorExecutorWait::Stopped(ActorExecutionStop::Deadline),
                }
            }
        };

        match wait {
            ActorExecutorWait::Completed(outcome) => Ok(ActorInvocationStep::Continue(
                outcome.context("invoke actor in attached customer process")?,
            )),
            ActorExecutorWait::Stopped(stop) => {
                self.cancel_actor_execution(executor, invocation, &mut executor_call, stop)
                    .await;
                let failure = match stop {
                    ActorExecutionStop::Drain(reason) => {
                        ActorInvocationFailure::cancelled_during_execution(reason.description())
                    }
                    ActorExecutionStop::Deadline => {
                        ActorInvocationFailure::deadline_exceeded_during_execution()
                    }
                };
                Ok(ActorInvocationStep::Complete(
                    OwnershipGuardResult::Completed(ActorLocalResult::Failed { failure }),
                ))
            }
        }
    }

    async fn cancel_actor_execution<F>(
        &self,
        executor: &Arc<dyn ActorExecutor>,
        invocation: &ActorInvocation,
        executor_call: &mut F,
        stop: ActorExecutionStop,
    ) where
        F: Future<Output = Result<ActorMethodOutcome>> + Unpin,
    {
        match stop {
            ActorExecutionStop::Drain(reason) => warn!(
                ?reason,
                "actor host started draining during JavaScript execution; requesting cancellation"
            ),
            ActorExecutionStop::Deadline => warn!(
                "actor invocation deadline expired during JavaScript execution; requesting cancellation"
            ),
        }
        let cancellation = executor.cancel(ActorMethodCancellation {
            request_id: invocation.request_id.clone(),
            actor: invocation.actor.clone(),
        });
        tokio::pin!(cancellation);
        let terminal = tokio::select! {
            outcome = &mut *executor_call => outcome,
            cancellation_result = &mut cancellation => {
                match (stop, cancellation_result) {
                    (ActorExecutionStop::Drain(_), Ok(())) => {
                        debug!("customer JavaScript acknowledged actor drain cancellation")
                    }
                    (ActorExecutionStop::Deadline, Ok(())) => {
                        debug!("customer JavaScript acknowledged actor cancellation")
                    }
                    (ActorExecutionStop::Drain(_), Err(error)) => warn!(
                        error = %format!("{error:#}"),
                        "could not deliver actor drain cancellation; waiting for terminal invocation"
                    ),
                    (ActorExecutionStop::Deadline, Err(error)) => warn!(
                        error = %format!("{error:#}"),
                        "could not deliver actor cancellation; waiting for terminal invocation"
                    ),
                }
                executor_call.await
            }
        };
        if let Err(error) = terminal {
            match stop {
                ActorExecutionStop::Drain(_) => warn!(
                    error = %format!("{error:#}"),
                    "actor executor disconnected while terminating a draining invocation"
                ),
                ActorExecutionStop::Deadline => warn!(
                    error = %format!("{error:#}"),
                    "actor executor disconnected while terminating an expired invocation"
                ),
            }
        }
        self.evict_actor_instance(executor, &invocation.actor).await;
    }

    async fn persist_actor_invocation(
        &self,
        object: &ActorStorageKey,
        invocation: &ActorInvocation,
        durability: ActorInvocationDurability,
    ) -> Result<ActorOperationResult> {
        let ActorInvocationDurability {
            manifest,
            database,
            mut receipts,
            execution,
            deadline,
            execution_started,
            mut measurements,
        } = durability;
        if deadline.is_elapsed() {
            warn!("actor deadline expired after JavaScript completed but before durable staging");
            return Ok(self
                .abandon_actor_with_unknown_outcome(object, &invocation.actor)
                .await);
        }
        if let Err(error) =
            Self::stage_actor_transition(&database, invocation, &mut receipts, &execution)
        {
            warn!(
                error = %format!("{:#}", error.context("stage actor state transition and idempotency receipt in SQLite")),
                "actor execution completed but local durability staging failed"
            );
            return Ok(self
                .abandon_actor_with_unknown_outcome(object, &invocation.actor)
                .await);
        }

        let Some(captured) = self.capture_actor_transition(object, deadline).await else {
            return Ok(self
                .abandon_actor_with_unknown_outcome(object, &invocation.actor)
                .await);
        };
        let Some(published) = self
            .publish_actor_transition(object, &manifest, &captured.value, deadline)
            .await
        else {
            return Ok(self
                .abandon_actor_with_unknown_outcome(object, &invocation.actor)
                .await);
        };
        let checkpoint_ms = self
            .checkpoint_actor_transition(object, &captured.value, &published.value)
            .await;
        if let Err(error) = self
            .databases
            .mark_cache_current(object, published.value.max_txid())
        {
            warn!(
                error = %format!("{error:#}"),
                "could not persist the local actor cache watermark; the next host will restore canonical state"
            );
            if let Err(invalidation_error) = self.databases.invalidate_cache(object) {
                warn!(error = %format!("{invalidation_error:#}"), "could not invalidate the local actor cache watermark");
            }
        }
        if !self.confirm_actor_ownership_after_publication().await {
            if let Some(executor) = &self.actor_executor {
                self.evict_actor_instance(executor, &invocation.actor).await;
            }
            return Ok(OwnershipGuardResult::Completed(ActorLocalResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            }));
        }

        measurements.phases = ExecutionPhaseTimings {
            executor_ms: execution.executor_ms,
            capture_ms: captured.elapsed_ms,
            publish_ms: published.elapsed_ms,
            checkpoint_ms,
        };
        measurements.state_changed = execution.state_changed;
        info!(
            owner_epoch = manifest.owner().epoch,
            durable_txid = published.value.max_txid(),
            state_initialized = execution.initializes_state,
            state_changed = execution.state_changed,
            queue_wait_ms = measurements.queue_wait_ms,
            execution_ms = execution_started.elapsed().as_secs_f64() * 1_000.0,
            "actor invocation completed across the durability boundary"
        );
        self.publish_actor_execution(invocation, measurements, &execution.result);
        Ok(OwnershipGuardResult::Completed(execution.result))
    }

    fn stage_actor_transition(
        database: &SqliteActorDatabase,
        invocation: &ActorInvocation,
        receipts: &mut ActorInvocationReceipts,
        execution: &ExecutedActorInvocation,
    ) -> Result<()> {
        receipts.record(ActorInvocationReceipt::new(
            invocation,
            execution.result.clone(),
        ))?;
        let receipt_bytes = serde_json::to_vec(receipts)?;
        let state_bytes = execution
            .proposed_state
            .as_ref()
            .filter(|_| execution.initializes_state || execution.state_changed)
            .map(serde_json::to_vec)
            .transpose()?;
        let mut writes = vec![(ACTOR_RECEIPTS_KEY, receipt_bytes.as_slice())];
        if let Some(state_bytes) = &state_bytes {
            writes.push((ACTOR_STATE_KEY, state_bytes.as_slice()));
        }
        database.set_bytes_batch(&writes)?;
        Ok(())
    }

    async fn capture_actor_transition(
        &self,
        object: &ActorStorageKey,
        deadline: ActorInvocationDeadline,
    ) -> Option<Timed<CapturedActorChanges>> {
        let started = Instant::now();
        let captured = match tokio::time::timeout_at(
            deadline.at(),
            self.change_capture.capture(object),
        )
        .await
        {
            Ok(Ok(captured)) => captured,
            Ok(Err(error)) => {
                warn!(
                    error = %format!("{:#}", error.context("capture actor state transition as LTX")),
                    "actor execution completed but LTX capture failed"
                );
                return None;
            }
            Err(_) => {
                warn!("actor deadline expired during LTX capture");
                return None;
            }
        };
        debug!(
            segment_count = captured.len(),
            max_captured_txid = captured.segments().last().map(|segment| segment.max_txid),
            "captured actor state transition"
        );
        Some(Timed {
            value: captured,
            elapsed_ms: elapsed_ms(started),
        })
    }

    async fn publish_actor_transition(
        &self,
        object: &ActorStorageKey,
        manifest: &VersionedActorManifest,
        captured: &CapturedActorChanges,
        deadline: ActorInvocationDeadline,
    ) -> Option<Timed<VersionedActorManifest>> {
        let started = Instant::now();
        let published = if captured.is_empty() {
            // Every newly executed actor invocation writes a receipt. This branch is
            // retained defensively for a storage implementation reporting a no-op batch.
            manifest.clone()
        } else {
            self.publish_actor_state(object, manifest, captured, deadline)
                .await?
        };
        Some(Timed {
            value: published,
            elapsed_ms: elapsed_ms(started),
        })
    }

    async fn checkpoint_actor_transition(
        &self,
        object: &ActorStorageKey,
        captured: &CapturedActorChanges,
        published: &VersionedActorManifest,
    ) -> f64 {
        let started = Instant::now();
        if !captured.is_empty() {
            match tokio::time::timeout(
                self.actor_timeouts.cleanup,
                self.change_capture
                    .checkpoint_durable(object, published.max_txid()),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(
                    durable_txid = published.max_txid(),
                    error = %format!("{error:#}"),
                    "could not checkpoint remotely durable actor WAL frames"
                ),
                Err(_) => warn!(
                    durable_txid = published.max_txid(),
                    timeout_ms = self.actor_timeouts.cleanup.as_millis(),
                    "timed out checkpointing remotely durable actor WAL frames"
                ),
            }
        }
        elapsed_ms(started)
    }

    async fn confirm_actor_ownership_after_publication(&self) -> bool {
        match tokio::time::timeout(
            self.actor_timeouts.cleanup,
            self.require_active_host_lease(),
        )
        .await
        {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                warn!(
                    error = %format!("{error:#}"),
                    "lost ownership after actor publication"
                );
                false
            }
            Err(_) => {
                warn!(
                    timeout_ms = self.actor_timeouts.cleanup.as_millis(),
                    "timed out confirming ownership after actor publication"
                );
                false
            }
        }
    }

    fn publish_actor_execution(
        &self,
        invocation: &ActorInvocation,
        measurements: ActorExecutionMeasurements,
        result: &ActorLocalResult,
    ) {
        let (success, outcome, failure_class, error_code) = match result {
            ActorLocalResult::Completed { .. } => (true, "completed".into(), None, None),
            ActorLocalResult::Failed { failure } => {
                let failure_class = if matches!(
                    failure.code.as_str(),
                    "deadline_exceeded" | "outcome_unknown" | "cancelled"
                ) {
                    "system"
                } else {
                    "customer"
                };
                (
                    false,
                    failure.code.clone(),
                    Some(failure_class.into()),
                    Some(failure.code.clone()),
                )
            }
        };
        let execution_kind = if measurements.receipt_replay {
            ActorExecutionKind::ReceiptReplay
        } else {
            match (measurements.cold_start, measurements.state_changed) {
                (true, true) => ActorExecutionKind::ColdWrite,
                (true, false) => ActorExecutionKind::ColdRead,
                (false, true) => ActorExecutionKind::HotWrite,
                (false, false) => ActorExecutionKind::HotRead,
            }
        };
        self.telemetry
            .publish(ActorTelemetryEvent::ActorExecutionFinished(
                ActorExecutionTelemetry {
                    scope: ActorTelemetryScope {
                        namespace_id: Some(invocation.actor.namespace_id.clone()),
                    },
                    role: ActorSystemRole::Host,
                    total_ms: elapsed_ms(measurements.started),
                    queue_wait_ms: measurements.queue_wait_ms,
                    actor_ready_ms: measurements.actor_ready_ms,
                    executor_ms: measurements.phases.executor_ms,
                    capture_ms: measurements.phases.capture_ms,
                    publish_ms: measurements.phases.publish_ms,
                    checkpoint_ms: measurements.phases.checkpoint_ms,
                    cold_start: measurements.cold_start,
                    state_changed: measurements.state_changed,
                    receipt_replay: measurements.receipt_replay,
                    execution_kind,
                    success,
                    outcome,
                    failure_class,
                    error_code,
                    actor_type: invocation.actor.actor_type.clone(),
                    method: invocation.method.clone(),
                },
            ));
    }

    async fn publish_actor_state(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
        deadline: ActorInvocationDeadline,
    ) -> Option<VersionedActorManifest> {
        match tokio::time::timeout_at(
            deadline.at(),
            self.durability.publish(object, current, captured),
        )
        .await
        {
            Ok(Ok(published)) => return Some(published),
            Ok(Err(error)) => warn!(
                error = %format!("{error:#}"),
                "actor publication failed; reconciling the canonical manifest"
            ),
            Err(_) => warn!(
                "actor deadline expired during publication; reconciling the canonical manifest"
            ),
        }

        match self
            .probe_actor_publication(object, current, captured)
            .await
        {
            Some(StatePublicationStatus::Published(published)) => return Some(published),
            Some(StatePublicationStatus::Conflict(observed)) => {
                warn!(
                    observed_owner = ?observed.as_ref().map(VersionedActorManifest::owner),
                    observed_txid = ?observed.as_ref().map(VersionedActorManifest::max_txid),
                    "actor publication conflicted with another canonical manifest"
                );
                return None;
            }
            Some(StatePublicationStatus::Unchanged) | None => {}
        }

        // The commit bundle and target manifest are deterministic, so retrying the
        // same CAS cannot create a second actor transition.
        match tokio::time::timeout(
            self.actor_timeouts.publication_retry,
            self.durability.publish(object, current, captured),
        )
        .await
        {
            Ok(Ok(published)) => return Some(published),
            Ok(Err(error)) => warn!(
                error = %format!("{error:#}"),
                "idempotent actor publication retry failed"
            ),
            Err(_) => warn!(
                timeout_ms = self.actor_timeouts.publication_retry.as_millis(),
                "idempotent actor publication retry timed out"
            ),
        }

        match self
            .probe_actor_publication(object, current, captured)
            .await
        {
            Some(StatePublicationStatus::Published(published)) => Some(published),
            Some(StatePublicationStatus::Unchanged)
            | Some(StatePublicationStatus::Conflict(_))
            | None => {
                warn!("actor publication outcome remains unknown after bounded reconciliation");
                None
            }
        }
    }

    async fn probe_actor_publication(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Option<StatePublicationStatus> {
        match tokio::time::timeout(
            self.actor_timeouts.publication_probe,
            self.durability
                .publication_status(object, current, captured),
        )
        .await
        {
            Ok(Ok(status)) => Some(status),
            Ok(Err(error)) => {
                warn!(
                    error = %format!("{error:#}"),
                    "could not read the canonical manifest while reconciling actor publication"
                );
                None
            }
            Err(_) => {
                warn!(
                    timeout_ms = self.actor_timeouts.publication_probe.as_millis(),
                    "timed out reading the canonical manifest while reconciling actor publication"
                );
                None
            }
        }
    }

    async fn abandon_actor_with_unknown_outcome(
        &self,
        object: &ActorStorageKey,
        actor: &crate::actor::ActorKey,
    ) -> OwnershipGuardResult<ActorLocalResult> {
        self.abandon_unpublished_actor_state(object).await;
        if let Some(executor) = &self.actor_executor {
            self.evict_actor_instance(executor, actor).await;
        }
        OwnershipGuardResult::Completed(ActorLocalResult::Failed {
            failure: ActorInvocationFailure::outcome_unknown_after_execution(),
        })
    }

    async fn evict_actor_instance(
        &self,
        executor: &Arc<dyn ActorExecutor>,
        actor: &crate::actor::ActorKey,
    ) {
        if let Err(error) = executor
            .evict(ActorMethodEviction {
                actor: actor.clone(),
            })
            .await
        {
            warn!(
                actor_type = %actor.actor_type,
                actor_id = %actor.actor_id,
                error = %format!("{error:#}"),
                "could not evict the resident actor instance"
            );
        }
    }

    async fn abandon_unpublished_actor_state(&self, object: &ActorStorageKey) {
        if let Err(error) = self.restore_cache.forget(object) {
            warn!(error = %format!("{error:#}"), "could not forget actor readiness cache");
        }
        if let Err(error) = self.databases.invalidate_cache(object) {
            warn!(error = %format!("{error:#}"), "could not invalidate local actor cache watermark");
        }
    }

    #[tracing::instrument(
        name = "object.operation",
        skip(self, operation),
        fields(host_id = %self.endpoint.id, object = %object)
    )]
    #[cfg(test)]
    pub(super) async fn with_actor_ownership<T, F>(
        &self,
        object: &ActorStorageKey,
        operation: F,
    ) -> Result<OwnershipGuardResult<T>>
    where
        F: FnOnce(&SqliteActorDatabase) -> Result<T>,
    {
        object.validate()?;
        debug!("waiting for the actor execution lock");
        let _execution = self.execution_locks.acquire(object).await?;
        debug!("acquired the actor execution lock");

        match self.resolve_actor_ownership(object).await? {
            OwnershipResolution::Owned(manifest) => {
                let owner = manifest.owner().clone();
                debug!(
                    owner_host = %owner.host,
                    owner_epoch = owner.epoch,
                    "actor ownership confirmed"
                );
                self.ensure_actor_state_ready(object, &manifest).await?;
                let database = self.open_actor_database(object)?;
                self.change_capture.prepare(object).await?;

                let operation = operation(&database);
                if let Err(operation_error) = &operation {
                    warn!(
                        error = %format!("{operation_error:#}"),
                        "actor state operation returned an error; committed SQLite changes will still cross the durability barrier"
                    );
                }
                let captured = self.change_capture.capture(object).await?;
                debug!(
                    segment_count = captured.len(),
                    max_captured_txid = captured.segments().last().map(|segment| segment.max_txid),
                    "captured committed SQLite changes"
                );
                let published = if captured.is_empty() {
                    manifest.clone()
                } else {
                    match self.durability.publish(object, &manifest, &captured).await {
                        Ok(manifest) => manifest,
                        Err(error) => {
                            self.restore_cache.forget(object)?;
                            self.databases.invalidate_cache(object)?;
                            return Err(error);
                        }
                    }
                };
                debug!(
                    owner_epoch = published.owner().epoch,
                    durable_txid = published.max_txid(),
                    "published actor state through canonical manifest"
                );

                if !captured.is_empty()
                    && let Err(error) = self
                        .change_capture
                        .checkpoint_durable(object, published.max_txid())
                        .await
                {
                    // Publication is already canonical. A local checkpoint is an
                    // optimization and must not turn a durable operation into an
                    // ambiguous client failure. Leaving the WAL intact is safe; the
                    // next successful publication will retry the checkpoint.
                    warn!(
                        durable_txid = published.max_txid(),
                        error = %format!("{error:#}"),
                        "could not checkpoint remotely durable WAL frames"
                    );
                }

                if let Err(error) = self
                    .databases
                    .mark_cache_current(object, published.max_txid())
                {
                    warn!(error = %format!("{error:#}"), "could not persist local actor cache watermark");
                    let _ = self.databases.invalidate_cache(object);
                }

                if let Err(error) = self.require_active_host_lease().await {
                    error!(
                        expected_host = %owner.host,
                        expected_epoch = owner.epoch,
                        durable_txid = published.max_txid(),
                        "refusing to acknowledge after the locally confirmed lease expired"
                    );
                    return Err(error.context(format!("lost ownership for {object}")));
                }

                let value = match operation {
                    Ok(value) => value,
                    Err(operation_error) => return Err(operation_error),
                };

                info!(
                    owner_epoch = owner.epoch,
                    durable_txid = published.max_txid(),
                    captured_segments = captured.len(),
                    "actor state operation completed across the durability boundary"
                );

                Ok(OwnershipGuardResult::Completed(value))
            }
            OwnershipResolution::NotOwner(owner) => {
                debug!(
                    owner_host = %owner.host,
                    owner_epoch = owner.epoch,
                    "actor state operation rejected by non-owner host"
                );
                Ok(OwnershipGuardResult::NotOwner(owner))
            }
            OwnershipResolution::Conflict(owner) => {
                warn!(owner = ?owner, "actor state operation stopped after ownership conflict");
                Ok(OwnershipGuardResult::Conflict(owner))
            }
        }
    }

    async fn ensure_actor_state_ready(
        &self,
        object: &ActorStorageKey,
        manifest: &VersionedActorManifest,
    ) -> Result<bool> {
        let owner = manifest.owner();
        if self.restore_cache.is_ready(object, owner.epoch)? {
            debug!(
                owner_epoch = owner.epoch,
                "actor state is ready for execution"
            );
            return Ok(false);
        }

        if self
            .databases
            .cache_is_current(object, manifest.max_txid())?
        {
            self.restore_cache.mark_ready(object, owner.epoch)?;
            info!(
                owner_epoch = owner.epoch,
                durable_txid = manifest.max_txid(),
                "reused provider-local SQLite cache"
            );
            return Ok(false);
        }

        info!(
            owner_epoch = owner.epoch,
            "preparing actor state for a new ownership epoch"
        );
        self.change_capture.reset(object).await?;
        if let Err(restore_error) = self
            .state_restorer
            .restore(object, &manifest.manifest)
            .await
        {
            error!(
                owner_epoch = owner.epoch,
                durable_txid = manifest.max_txid(),
                error = %format!("{restore_error:#}"),
                "actor restore failed"
            );
            return Err(restore_error);
        }

        self.require_active_host_lease()
            .await
            .with_context(|| format!("lost ownership while restoring {object}"))?;

        self.databases
            .mark_cache_current(object, manifest.max_txid())?;
        self.restore_cache.mark_ready(object, owner.epoch)?;
        info!(
            owner_epoch = owner.epoch,
            durable_txid = manifest.max_txid(),
            "actor state is restored and ready"
        );

        Ok(true)
    }

    #[cfg(test)]
    pub(super) async fn ensure_ownership(
        &self,
        object: &ActorStorageKey,
    ) -> Result<EnsureOwnershipResult> {
        Ok(match self.resolve_actor_ownership(object).await? {
            OwnershipResolution::Owned(manifest) => {
                EnsureOwnershipResult::Owned(manifest.owner().clone())
            }
            OwnershipResolution::NotOwner(owner) => EnsureOwnershipResult::NotOwner(owner),
            OwnershipResolution::Conflict(owner) => EnsureOwnershipResult::Conflict(owner),
        })
    }

    async fn resolve_actor_ownership(
        &self,
        object: &ActorStorageKey,
    ) -> Result<OwnershipResolution> {
        let current = self.durability.manifest(object).await?;

        if let Some(manifest) = current.as_ref() {
            let owner = manifest.owner();
            if self.host_has_active_lease(&owner.host).await? {
                debug!(
                    owner_host = %owner.host,
                    owner_epoch = owner.epoch,
                    max_txid = manifest.max_txid(),
                    "found authoritative actor manifest"
                );
                return Ok(self.classify_ownership(manifest.clone()));
            }
            info!(
                stale_owner_host = %owner.host,
                stale_owner_epoch = owner.epoch,
                "actor owner is stale; attempting takeover"
            );
        } else {
            debug!("actor is unowned; attempting initial claim");
        }

        self.require_active_host_lease().await?;

        match self
            .durability
            .claim(object, current.as_ref(), &self.endpoint.id)
            .await?
        {
            OwnershipClaimResult::Acquired(manifest) => {
                self.require_active_host_lease().await?;
                let owner = manifest.owner();
                info!(
                    owner_host = %owner.host,
                    owner_epoch = owner.epoch,
                    previous_owner = ?current.as_ref().map(VersionedActorManifest::owner),
                    max_txid = manifest.max_txid(),
                    "actor manifest ownership acquired"
                );
                Ok(OwnershipResolution::Owned(manifest))
            }
            OwnershipClaimResult::Conflict(Some(winner)) => {
                let owner = winner.owner();
                if self.host_has_active_lease(&owner.host).await? {
                    debug!(
                        winner_host = %owner.host,
                        winner_epoch = owner.epoch,
                        "ownership claim lost to an active owner host"
                    );
                    Ok(self.classify_ownership(winner))
                } else {
                    warn!(
                        winner_host = %owner.host,
                        winner_epoch = owner.epoch,
                        "ownership claim conflicted with a non-authoritative record"
                    );
                    Ok(OwnershipResolution::Conflict(Some(owner.clone())))
                }
            }
            OwnershipClaimResult::Conflict(None) => {
                warn!("ownership claim conflicted without a current owner");
                Ok(OwnershipResolution::Conflict(None))
            }
        }
    }

    async fn require_active_host_lease(&self) -> Result<()> {
        if !self.host_has_active_lease(&self.endpoint.id).await? {
            warn!(host_id = %self.endpoint.id, "host is self-fenced by an inactive lease");
            anyhow::bail!("host {} does not hold an active lease", self.endpoint.id);
        }

        Ok(())
    }

    async fn host_has_active_lease(&self, host: &HostId) -> Result<bool> {
        let now_ms = self.clock.now_ms()?;
        if let Some(confirmed_lease) = &self.confirmed_lease
            && let Some(active) = confirmed_lease.lease_is_active(host, now_ms)
        {
            return Ok(active);
        }

        Ok(self.leases.lease_status(host).await?.is_active())
    }

    fn classify_ownership(&self, manifest: VersionedActorManifest) -> OwnershipResolution {
        if manifest.owner().host == self.endpoint.id {
            OwnershipResolution::Owned(manifest)
        } else {
            OwnershipResolution::NotOwner(manifest.owner().clone())
        }
    }
}

fn deadline_exceeded_before_execution() -> OwnershipGuardResult<ActorLocalResult> {
    OwnershipGuardResult::Completed(ActorLocalResult::Failed {
        failure: ActorInvocationFailure::deadline_exceeded_before_execution(),
    })
}

fn cancelled_before_execution(reason: ActorDrainReason) -> OwnershipGuardResult<ActorLocalResult> {
    OwnershipGuardResult::Completed(ActorLocalResult::Failed {
        failure: ActorInvocationFailure::cancelled_before_execution(reason.description()),
    })
}

enum BeforeActorExecution<T> {
    Completed(T),
    DeadlineExceeded,
    Draining(ActorDrainReason),
}

async fn before_actor_execution<T>(
    deadline: ActorInvocationDeadline,
    drain: &mut watch::Receiver<Option<ActorDrainReason>>,
    operation: impl Future<Output = Result<T>>,
) -> Result<BeforeActorExecution<T>> {
    tokio::select! {
        biased;
        reason = wait_for_actor_drain(drain) => Ok(BeforeActorExecution::Draining(reason)),
        result = tokio::time::timeout_at(deadline.at(), operation) => match result {
            Ok(result) => result.map(BeforeActorExecution::Completed),
            Err(_) => Ok(BeforeActorExecution::DeadlineExceeded),
        }
    }
}

async fn wait_for_actor_drain(
    drain: &mut watch::Receiver<Option<ActorDrainReason>>,
) -> ActorDrainReason {
    loop {
        if let Some(reason) = *drain.borrow() {
            return reason;
        }
        if drain.changed().await.is_err() {
            return ActorDrainReason::Shutdown;
        }
    }
}

enum OwnershipResolution {
    Owned(VersionedActorManifest),
    NotOwner(ActorOwner),
    Conflict(Option<ActorOwner>),
}

enum ActorTaskAdmission {
    Start(ActorTask),
    Draining(ActorDrainReason),
}

struct ActorTaskTracker {
    state: Mutex<ActorTaskTrackerState>,
    drain: watch::Sender<Option<ActorDrainReason>>,
    activity: watch::Sender<usize>,
}

#[derive(Default)]
struct ActorTaskTrackerState {
    active: usize,
    drain_reason: Option<ActorDrainReason>,
}

struct ActorTask {
    tracker: Arc<ActorTaskTracker>,
}

impl Default for ActorTaskTracker {
    fn default() -> Self {
        let (drain, _) = watch::channel(None);
        let (activity, _) = watch::channel(0);
        Self {
            state: Mutex::new(ActorTaskTrackerState::default()),
            drain,
            activity,
        }
    }
}

impl ActorTaskTracker {
    fn start(self: &Arc<Self>) -> Result<ActorTaskAdmission> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("actor task tracker lock poisoned"))?;
        if let Some(reason) = state.drain_reason {
            return Ok(ActorTaskAdmission::Draining(reason));
        }
        state.active += 1;
        self.activity.send_replace(state.active);
        Ok(ActorTaskAdmission::Start(ActorTask {
            tracker: self.clone(),
        }))
    }

    fn drain_signal(&self) -> watch::Receiver<Option<ActorDrainReason>> {
        self.drain.subscribe()
    }

    fn begin_drain(&self, reason: ActorDrainReason) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("actor task tracker lock poisoned"))?;
        if state.drain_reason.is_none() {
            state.drain_reason = Some(reason);
            self.drain.send_replace(Some(reason));
        }
        Ok(())
    }

    async fn wait_until_empty(&self) {
        let mut activity = self.activity.subscribe();
        loop {
            if *activity.borrow() == 0 {
                return;
            }
            if activity.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for ActorTask {
    fn drop(&mut self) {
        if let Ok(mut state) = self.tracker.state.lock() {
            state.active = state.active.saturating_sub(1);
            self.tracker.activity.send_replace(state.active);
        }
    }
}

fn idempotency_key_reused_failure(invocation: &ActorInvocation) -> ActorInvocationFailure {
    ActorInvocationFailure {
        code: "idempotency_key_reused".into(),
        message: format!(
            "idempotency key {} was already used for a different actor invocation",
            invocation.request_id
        ),
    }
}

const ACTOR_STATE_KEY: &str = "__durable_object.state.v1";
const ACTOR_RECEIPTS_KEY: &str = "__durable_object.receipts.v1";
const MAX_ACTOR_RECEIPTS: usize = 256;
const MAX_ACTOR_RECEIPT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ActorLocalResult {
    Completed { result: Value },
    Failed { failure: ActorInvocationFailure },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ActorInvocationReceipt {
    request_id: String,
    method: String,
    args: Vec<Value>,
    outcome: ActorLocalResult,
}

impl ActorInvocationReceipt {
    fn new(invocation: &ActorInvocation, outcome: ActorLocalResult) -> Self {
        Self {
            request_id: invocation.request_id.clone(),
            method: invocation.method.clone(),
            args: invocation.args.clone(),
            outcome,
        }
    }

    fn matches(&self, invocation: &ActorInvocation) -> bool {
        // Timeout and call ancestry are intentionally absent from durable
        // receipts because neither changes the operation payload.
        self.method == invocation.method && self.args == invocation.args
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ActorInvocationReceipts {
    entries: Vec<ActorInvocationReceipt>,
}

impl ActorInvocationReceipts {
    fn lookup(&self, invocation: &ActorInvocation) -> ActorReceiptLookup {
        let Some(receipt) = self
            .entries
            .iter()
            .rev()
            .find(|receipt| receipt.request_id == invocation.request_id)
        else {
            return ActorReceiptLookup::Missing;
        };
        if receipt.matches(invocation) {
            ActorReceiptLookup::Replay(receipt.outcome.clone())
        } else {
            ActorReceiptLookup::Conflict
        }
    }

    fn record(&mut self, receipt: ActorInvocationReceipt) -> serde_json::Result<()> {
        self.entries.push(receipt);
        while self.entries.len() > MAX_ACTOR_RECEIPTS {
            self.entries.remove(0);
        }
        while self.entries.len() > 1 && serde_json::to_vec(self)?.len() > MAX_ACTOR_RECEIPT_BYTES {
            self.entries.remove(0);
        }
        Ok(())
    }
}

enum ActorReceiptLookup {
    Missing,
    Replay(ActorLocalResult),
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(test)]
pub(super) enum EnsureOwnershipResult {
    Owned(ActorOwner),
    NotOwner(ActorOwner),
    Conflict(Option<ActorOwner>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OwnershipGuardResult<T> {
    Completed(T),
    NotOwner(ActorOwner),
    Conflict(Option<ActorOwner>),
}

#[cfg(test)]
mod tests;
