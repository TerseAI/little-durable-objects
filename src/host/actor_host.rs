use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
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
        if let Some(result) = self.validate_invocation(&invocation)? {
            return Ok(result);
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
        let mut loaded = match self
            .load_owned_state(&invocation, owner_epoch, &state_read_url)
            .await?
        {
            OwnershipRead::Owned(loaded) => loaded,
            OwnershipRead::Reroute => return Ok(ActorExecutionResult::Reroute),
        };
        let (result, next_state) = match self.execute_method(&invocation, &loaded).await {
            Ok(outcome) => outcome,
            Err(failure) => return Ok(failure),
        };
        let append = match loaded.log.append(owner_epoch, next_state) {
            Ok(append) => append,
            Err(error) => return Ok(self.state_failure(&invocation, error).await),
        };
        if append == StateAppend::Unchanged {
            return Ok(ActorExecutionResult::Completed { result });
        }
        self.publish_result(&invocation, owner_epoch, loaded, result)
            .await
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

    async fn publish_claim(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        loaded: &LoadedState,
    ) -> Result<bool> {
        let write_url = self
            .authorize_write(invocation, owner_epoch, &loaded.generation)
            .await
            .context("authorize actor ownership claim")?;
        match self.state.write(&write_url, loaded.log.encode()?).await? {
            StateWrite::Written => Ok(true),
            StateWrite::GenerationMismatch => Ok(false),
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

    async fn publish_state(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        loaded: &LoadedState,
    ) -> Result<()> {
        let write_url = self
            .authorize_write(invocation, owner_epoch, &loaded.generation)
            .await?;
        match self.state.write(&write_url, loaded.log.encode()?).await? {
            StateWrite::Written => Ok(()),
            StateWrite::GenerationMismatch => {
                anyhow::bail!("actor state generation changed during execution")
            }
        }
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

enum OwnershipRead {
    Owned(LoadedState),
    Reroute,
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
