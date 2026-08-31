use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::sync::{Notify, watch};
use tracing::warn;

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure,
        ActorMethodEviction, ActorMethodInvocation, ActorMethodOutcome,
    },
    actor_state::{ActorExecutionAdmission, ActorExecutionLocks},
    control_plane::ControlPlaneClient,
    state_log::StateAppend,
    state_transport::{StateTransport, StateWrite},
};

use super::HostEndpoint;

pub(crate) struct ActorHost {
    endpoint: HostEndpoint,
    namespace_id: String,
    executor: Arc<dyn ActorExecutor>,
    control_plane: Arc<ControlPlaneClient>,
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
        control_plane: Arc<ControlPlaneClient>,
        state: Arc<dyn StateTransport>,
    ) -> Self {
        let (activity_tx, _) = watch::channel(0);
        Self {
            endpoint,
            namespace_id,
            executor,
            control_plane,
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
        if !self.accepting.load(Ordering::SeqCst) {
            return Ok(ActorExecutionResult::HostUnavailable);
        }
        invocation.validate()?;
        if invocation.actor.namespace_id != self.namespace_id {
            anyhow::bail!("actor invocation crossed the host namespace");
        }
        if !self.executor.supports(&invocation.actor.actor_type) {
            return Ok(failed(
                "actor_error",
                "actor type is not loaded by this host",
            ));
        }

        let _activity = ActivityGuard::begin(self);
        let object = invocation.actor.storage_key();
        let _execution = match self.executions.admit(&object).await? {
            ActorExecutionAdmission::Acquired(guard) => guard,
            ActorExecutionAdmission::Full => return Ok(ActorExecutionResult::HostUnavailable),
        };
        if !self.accepting.load(Ordering::SeqCst) {
            return Ok(ActorExecutionResult::HostUnavailable);
        }

        let mut loaded = self
            .state
            .read(&state_read_url)
            .await
            .context("load actor state")?;
        if loaded
            .log
            .latest()
            .is_some_and(|record| record.owner_epoch > owner_epoch)
        {
            return Ok(ActorExecutionResult::Reroute);
        }
        if loaded.log.claim(owner_epoch).map_err(state_error)? {
            let write_url = self
                .control_plane
                .authorize_state_write(
                    &invocation.actor,
                    &self.endpoint.id,
                    owner_epoch,
                    &loaded.generation,
                )
                .await
                .context("authorize actor ownership claim")?;
            match self.state.write(&write_url, loaded.log.encode()?).await? {
                StateWrite::Written => {
                    loaded = self.state.read(&state_read_url).await?;
                    if !loaded
                        .log
                        .latest()
                        .is_some_and(|record| record.owner_epoch == owner_epoch)
                    {
                        return Ok(ActorExecutionResult::Reroute);
                    }
                }
                StateWrite::GenerationMismatch => return Ok(ActorExecutionResult::Reroute),
            }
        }

        let outcome = self
            .executor
            .invoke(ActorMethodInvocation {
                request_id: invocation.request_id,
                actor: invocation.actor.clone(),
                method: invocation.method,
                args: invocation.args,
                state: loaded.log.latest_state().cloned(),
                timeout_ms: invocation.timeout_ms,
            })
            .await;
        let (result, next_state) = match outcome {
            Ok(ActorMethodOutcome::Completed { result, state }) => (result, state),
            Ok(ActorMethodOutcome::Failed(failure)) => {
                self.evict(&invocation.actor).await;
                return Ok(failed("actor_error", failure.message));
            }
            Err(error) => {
                self.evict(&invocation.actor).await;
                return Ok(failed(
                    "actor_error",
                    format!("actor executor failed: {error:#}"),
                ));
            }
        };

        let append = match loaded.log.append(owner_epoch, next_state) {
            Ok(append) => append,
            Err(error) => {
                self.evict(&invocation.actor).await;
                return Ok(failed("state_error", format!("{error:#}")));
            }
        };
        if append == StateAppend::Unchanged {
            return Ok(ActorExecutionResult::Completed { result });
        }

        let publication = async {
            let write_url = self
                .control_plane
                .authorize_state_write(
                    &invocation.actor,
                    &self.endpoint.id,
                    owner_epoch,
                    &loaded.generation,
                )
                .await?;
            match self.state.write(&write_url, loaded.log.encode()?).await? {
                StateWrite::Written => Ok(()),
                StateWrite::GenerationMismatch => {
                    anyhow::bail!("actor state generation changed during execution")
                }
            }
        }
        .await;
        if let Err(error) = publication {
            self.evict(&invocation.actor).await;
            warn!(
                actor = %object,
                error = %format!("{error:#}"),
                "actor completed but state publication could not be confirmed"
            );
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }

        Ok(ActorExecutionResult::Completed { result })
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
