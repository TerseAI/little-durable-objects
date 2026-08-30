use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Notify;

use crate::{
    actor_state::ActorDatabaseStore,
    durability::{
        ActorDurabilityStore, ActorManifest, CapturedActorChanges, LocalActorChangeCapture,
        LocalActorStore, LtxActorStateRestorer, OwnershipClaimResult, RecoveryData,
        VersionedActorManifest,
    },
    host::{ActorDrainReason, ActorHost, ActorHostDependencies, HostEndpoint, HostId},
    host_leases::{HostLeaseRequest, HostLeaseStore, LocalHostLeaseStore},
    telemetry::{
        ActorExecutionKind, ActorTelemetry, ActorTelemetryEvent, LocalActorTelemetry,
        noop_actor_telemetry,
    },
};

use super::*;

#[derive(Default)]
struct CounterExecutor {
    invocations: AtomicUsize,
}

#[derive(Default)]
struct BlockingCounterExecutor {
    invocations: AtomicUsize,
    active: AtomicUsize,
    maximum_active: AtomicUsize,
    first_started: Notify,
    second_started: Notify,
    release_first: Notify,
}

#[derive(Default)]
struct CooperativeCounterExecutor {
    invocations: AtomicUsize,
    cancellations: AtomicUsize,
    first_started: Notify,
    first_terminated: Notify,
    cancel_first: Notify,
}

struct FaultingObjectStore {
    inner: Arc<LocalActorStore>,
    mode: AtomicUsize,
    publish_started: Notify,
}

const STORAGE_NORMAL: usize = 0;
const STORAGE_BLOCK_MANIFEST: usize = 1;
const STORAGE_COMMIT_THEN_LOSE_REPLY: usize = 2;
const STORAGE_BLOCK_PUBLICATION: usize = 3;
const STORAGE_ARM_UNKNOWN_PUBLICATION: usize = 4;

impl FaultingObjectStore {
    fn new(inner: Arc<LocalActorStore>, mode: usize) -> Self {
        Self {
            inner,
            mode: AtomicUsize::new(mode),
            publish_started: Notify::new(),
        }
    }
}

#[async_trait]
impl ActorDurabilityStore for FaultingObjectStore {
    async fn manifest(
        &self,
        object: &crate::actor_state::ActorStorageKey,
    ) -> Result<Option<VersionedActorManifest>> {
        if matches!(
            self.mode.load(Ordering::SeqCst),
            STORAGE_BLOCK_MANIFEST | STORAGE_BLOCK_PUBLICATION
        ) {
            return std::future::pending().await;
        }
        self.inner.manifest(object).await
    }

    async fn claim(
        &self,
        object: &crate::actor_state::ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        node: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.inner.claim(object, expected, node).await
    }

    async fn publish(
        &self,
        object: &crate::actor_state::ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<VersionedActorManifest> {
        match self.mode.load(Ordering::SeqCst) {
            STORAGE_COMMIT_THEN_LOSE_REPLY => {
                let published = self.inner.publish(object, current, captured).await?;
                self.publish_started.notify_one();
                std::future::pending::<()>().await;
                Ok(published)
            }
            STORAGE_ARM_UNKNOWN_PUBLICATION => {
                self.mode.store(STORAGE_BLOCK_PUBLICATION, Ordering::SeqCst);
                self.publish_started.notify_one();
                std::future::pending().await
            }
            STORAGE_BLOCK_PUBLICATION => std::future::pending().await,
            _ => self.inner.publish(object, current, captured).await,
        }
    }

    async fn recovery(
        &self,
        object: &crate::actor_state::ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<RecoveryData> {
        self.inner.recovery(object, manifest).await
    }
}

#[async_trait]
impl ActorExecutor for BlockingCounterExecutor {
    fn supports(&self, actor_type: &str) -> bool {
        actor_type == "counter"
    }

    async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum_active.fetch_max(active, Ordering::SeqCst);

        if invocation.request_id == "local-hold" {
            self.first_started.notify_one();
            self.release_first.notified().await;
        } else {
            self.second_started.notify_one();
        }

        let count = invocation
            .state
            .as_ref()
            .and_then(|state| state.get("count"))
            .and_then(|count| count.as_i64())
            .unwrap_or(0)
            + 1;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(ActorMethodOutcome::Completed {
            result: json!(count),
            state: json!({ "count": count }),
        })
    }

    async fn cancel(&self, _cancellation: ActorMethodCancellation) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ActorExecutor for CooperativeCounterExecutor {
    fn supports(&self, actor_type: &str) -> bool {
        actor_type == "counter"
    }

    async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome> {
        ensure!(invocation.request_id == "cooperative-cancel");
        let attempt = self.invocations.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            self.first_started.notify_one();
            self.cancel_first.notified().await;
            self.first_terminated.notify_one();
            return Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                code: "actor_method_failed".into(),
                message: "cancelled".into(),
            }));
        }
        Ok(ActorMethodOutcome::Completed {
            result: json!(1),
            state: json!({ "count": 1 }),
        })
    }

    async fn cancel(&self, cancellation: ActorMethodCancellation) -> Result<()> {
        ensure!(cancellation.request_id == "cooperative-cancel");
        self.cancellations.fetch_add(1, Ordering::SeqCst);
        self.cancel_first.notify_one();
        Ok(())
    }
}

#[async_trait]
impl ActorExecutor for CounterExecutor {
    fn supports(&self, actor_type: &str) -> bool {
        actor_type == "counter"
    }

    async fn invoke(&self, invocation: ActorMethodInvocation) -> Result<ActorMethodOutcome> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let count = invocation
            .state
            .as_ref()
            .and_then(|state| state.get("count"))
            .and_then(|count| count.as_i64())
            .unwrap_or(0);
        match invocation.method.as_str() {
            "increment" => {
                let amount = invocation
                    .args
                    .first()
                    .and_then(|amount| amount.as_i64())
                    .unwrap_or(1);
                let next = count + amount;
                Ok(ActorMethodOutcome::Completed {
                    result: json!(next),
                    state: json!({ "count": next }),
                })
            }
            "get_count" => Ok(ActorMethodOutcome::Completed {
                result: json!(count),
                state: json!({ "count": count }),
            }),
            "fail" => Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                code: "actor_method_failed".into(),
                message: "boom".into(),
            })),
            method => anyhow::bail!("unexpected test actor method {method}"),
        }
    }

    async fn cancel(&self, _cancellation: ActorMethodCancellation) -> Result<()> {
        Ok(())
    }
}

struct ClusterFixture {
    _root: TempDir,
    store: Arc<LocalActorStore>,
    nodes: Arc<LocalHostLeaseStore>,
}

impl ClusterFixture {
    async fn new() -> Result<Self> {
        let root = TempDir::new()?;
        Ok(Self {
            store: Arc::new(LocalActorStore::new(root.path().join("durable"))),
            nodes: Arc::new(LocalHostLeaseStore::new(root.path().join("nodes")).await?),
            _root: root,
        })
    }

    async fn host(
        &self,
        host_id: &str,
        route: &str,
        local_name: &str,
        executor: Arc<dyn ActorExecutor>,
    ) -> Result<Arc<ActorHost>> {
        self.host_with_telemetry(host_id, route, local_name, executor, noop_actor_telemetry())
            .await
    }

    async fn host_with_telemetry(
        &self,
        host_id: &str,
        route: &str,
        local_name: &str,
        executor: Arc<dyn ActorExecutor>,
        telemetry: Arc<dyn ActorTelemetry>,
    ) -> Result<Arc<ActorHost>> {
        let id = HostId::new(host_id);
        self.nodes
            .register(&HostLeaseRequest {
                id: id.clone(),
                session_id: format!("session-{host_id}"),
                route: route.into(),
                duration_ms: 60_000,
            })
            .await?;
        let local_root = self._root.path().join(local_name);
        let databases = Arc::new(ActorDatabaseStore::new(&local_root));
        let restore = Arc::new(LtxActorStateRestorer::new(
            self.store.clone(),
            databases.clone(),
        ));
        let dependencies = ActorHostDependencies::new(
            self.store.clone(),
            self.nodes.clone(),
            databases,
            Arc::new(LocalActorChangeCapture::new(&local_root)),
            restore,
        )
        .with_actor_executor(
            ActorScope {
                namespace_id: "namespace-1".into(),
            },
            executor,
        )
        .with_telemetry(telemetry);
        Ok(Arc::new(ActorHost::new(
            HostEndpoint {
                id,
                route: route.into(),
            },
            dependencies,
        )))
    }

    async fn host_with_faulting_store(
        &self,
        host_id: &str,
        route: &str,
        local_name: &str,
        store: Arc<dyn ActorDurabilityStore>,
        executor: Arc<dyn ActorExecutor>,
    ) -> Result<Arc<ActorHost>> {
        let id = HostId::new(host_id);
        self.nodes
            .register(&HostLeaseRequest {
                id: id.clone(),
                session_id: format!("session-{host_id}"),
                route: route.into(),
                duration_ms: 60_000,
            })
            .await?;
        let local_root = self._root.path().join(local_name);
        let databases = Arc::new(ActorDatabaseStore::new(&local_root));
        let restore = Arc::new(LtxActorStateRestorer::new(store.clone(), databases.clone()));
        let dependencies = ActorHostDependencies::new(
            store,
            self.nodes.clone(),
            databases,
            Arc::new(LocalActorChangeCapture::new(&local_root)),
            restore,
        )
        .with_actor_executor(
            ActorScope {
                namespace_id: "namespace-1".into(),
            },
            executor,
        )
        .with_actor_timeouts(
            Duration::from_millis(25),
            Duration::from_millis(25),
            Duration::from_millis(25),
        );
        Ok(Arc::new(ActorHost::new(
            HostEndpoint {
                id,
                route: route.into(),
            },
            dependencies,
        )))
    }
}

#[tokio::test]
async fn records_cold_hot_and_replayed_actor_latency_events() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let telemetry = Arc::new(LocalActorTelemetry::default());
    let host = fixture
        .host_with_telemetry(
            "node-a",
            "route-a",
            "local-a",
            Arc::new(CounterExecutor::default()),
            telemetry.clone(),
        )
        .await?;
    completed(execute(&host, invocation("write-1", "increment", vec![json!(2)])).await?);
    let read = invocation("read-1", "get_count", vec![]);
    completed(execute(&host, read.clone()).await?);
    completed(execute(&host, read).await?);

    let events = telemetry.events()?;
    let execution_kinds = events
        .iter()
        .filter_map(|event| match event {
            ActorTelemetryEvent::ActorExecutionFinished(event) => Some(event.execution_kind),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        execution_kinds,
        [
            ActorExecutionKind::ColdWrite,
            ActorExecutionKind::HotRead,
            ActorExecutionKind::ReceiptReplay,
        ]
    );
    Ok(())
}

fn invocation(request_id: &str, method: &str, args: Vec<serde_json::Value>) -> ActorInvocation {
    ActorInvocation {
        request_id: request_id.into(),
        actor: ActorKey {
            namespace_id: "namespace-1".into(),
            actor_type: "counter".into(),
            actor_id: "counter-1".into(),
        },
        method: method.into(),
        args,
        timeout_ms: 30_000,
    }
}

fn completed(result: ActorExecutionResult) -> serde_json::Value {
    match result {
        ActorExecutionResult::Completed { result } => result,
        other => panic!("expected completed actor result, got {other:?}"),
    }
}

async fn execute(host: &ActorHost, invocation: ActorInvocation) -> Result<ActorExecutionResult> {
    host.invoke_actor(invocation).await
}

async fn require_executor_start(
    started: &Notify,
    invocation: &mut tokio::task::JoinHandle<Result<ActorExecutionResult>>,
    label: &str,
) -> Result<()> {
    tokio::select! {
        _ = started.notified() => Ok(()),
        result = &mut *invocation => {
            let result = result??;
            anyhow::bail!("{label} completed before entering the actor executor: {result:?}")
        }
    }
}

#[tokio::test]
async fn does_not_claim_an_actor_type_missing_from_the_customer_process() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let host = fixture
        .host(
            "node-a",
            "route-a",
            "local-a",
            Arc::new(CounterExecutor::default()),
        )
        .await?;
    let mut request = invocation("request-1", "increment", vec![]);
    request.actor.actor_type = "conversation".into();
    let object = request.actor.storage_key();

    let reply = execute(&host, request).await?;

    assert_eq!(
        reply,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure {
                code: "actor_type_not_loaded".into(),
                message: "actor type conversation is not loaded in this customer process".into(),
            },
        }
    );
    assert!(fixture.store.manifest(&object).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn repeated_request_id_replays_the_durable_result() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let request = invocation("request-1", "increment", vec![json!(2)]);
    let object = request.actor.storage_key();

    let first = completed(execute(&host, request.clone()).await?);
    let second = completed(execute(&host, request).await?);

    assert_eq!(first, json!(2));
    assert_eq!(second, json!(2));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    let manifest = fixture
        .store
        .manifest(&object)
        .await?
        .expect("actor manifest");
    assert_eq!(manifest.max_txid(), 1);
    let database = ActorDatabaseStore::new(fixture._root.path().join("local-a")).open(&object)?;
    let keys: Vec<String> =
        database.query("SELECT key FROM __objects ORDER BY key", [], |row| {
            row.get(0)
        })?;
    assert_eq!(
        keys,
        [
            "__durable_object.receipts.v1".to_owned(),
            "__durable_object.state.v1".to_owned(),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn storage_timeout_before_execution_releases_the_actor_lock() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CounterExecutor::default());
    let store = Arc::new(FaultingObjectStore::new(
        fixture.store.clone(),
        STORAGE_BLOCK_MANIFEST,
    ));
    let host = fixture
        .host_with_faulting_store(
            "node-a",
            "route-a",
            "local-a",
            store.clone(),
            executor.clone(),
        )
        .await?;
    let mut blocked = invocation("blocked-manifest", "increment", vec![]);
    blocked.timeout_ms = 75;

    let reply = execute(&host, blocked).await?;

    assert!(matches!(
        reply,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "deadline_exceeded"
    ));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 0);

    store.mode.store(STORAGE_NORMAL, Ordering::SeqCst);
    let mut retry = invocation("after-blocked-manifest", "increment", vec![]);
    retry.timeout_ms = 2_000;
    assert_eq!(completed(execute(&host, retry).await?), json!(1));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn committed_publication_with_a_lost_reply_is_reconciled() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CounterExecutor::default());
    let store = Arc::new(FaultingObjectStore::new(
        fixture.store.clone(),
        STORAGE_COMMIT_THEN_LOSE_REPLY,
    ));
    let host = fixture
        .host_with_faulting_store(
            "node-a",
            "route-a",
            "local-a",
            store.clone(),
            executor.clone(),
        )
        .await?;
    let mut request = invocation("lost-publish-reply", "increment", vec![]);
    request.timeout_ms = 1_000;
    let object = request.actor.storage_key();
    let first = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { execute(&host, request).await }
    });
    tokio::time::timeout(Duration::from_secs(3), store.publish_started.notified()).await?;

    let mut duplicate = request.clone();
    duplicate.timeout_ms = 2_000;
    let reconciled = execute(&host, duplicate.clone()).await?;

    assert_eq!(completed(reconciled), json!(1));
    assert!(matches!(
        first.await??,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "deadline_exceeded"
    ));
    store.mode.store(STORAGE_NORMAL, Ordering::SeqCst);
    assert_eq!(completed(execute(&host, duplicate).await?), json!(1));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    let manifest = fixture
        .store
        .manifest(&object)
        .await?
        .expect("actor manifest");
    assert_eq!(manifest.max_txid(), 1);
    Ok(())
}

#[tokio::test]
async fn unconfirmable_publication_returns_outcome_unknown_and_allows_retry() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CounterExecutor::default());
    let store = Arc::new(FaultingObjectStore::new(
        fixture.store.clone(),
        STORAGE_ARM_UNKNOWN_PUBLICATION,
    ));
    let host = fixture
        .host_with_faulting_store(
            "node-a",
            "route-a",
            "local-a",
            store.clone(),
            executor.clone(),
        )
        .await?;
    let mut request = invocation("unknown-publish", "increment", vec![]);
    request.timeout_ms = 1_000;
    let first = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { execute(&host, request).await }
    });
    tokio::time::timeout(Duration::from_secs(3), store.publish_started.notified()).await?;

    let mut duplicate = request.clone();
    duplicate.timeout_ms = 2_000;
    let unknown = execute(&host, duplicate.clone()).await?;

    assert!(matches!(
        unknown,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "deadline_exceeded" || code == "outcome_unknown"
    ));
    assert!(matches!(
        first.await??,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "deadline_exceeded" || code == "outcome_unknown"
    ));

    store.mode.store(STORAGE_NORMAL, Ordering::SeqCst);
    duplicate.timeout_ms = 2_000;
    assert_eq!(completed(execute(&host, duplicate).await?), json!(1));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn reused_request_id_with_different_payload_is_rejected() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;

    assert_eq!(
        completed(execute(&host, invocation("request-1", "increment", vec![json!(2)]),).await?,),
        json!(2)
    );
    let conflict = execute(&host, invocation("request-1", "increment", vec![json!(3)])).await?;

    assert_eq!(
        conflict,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure {
                code: "idempotency_key_reused".into(),
                message:
                    "idempotency key request-1 was already used for a different actor invocation"
                        .into(),
            },
        }
    );
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn repeated_failed_request_replays_without_calling_the_actor_again() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let request = invocation("request-1", "fail", vec![]);

    let first = execute(&host, request.clone()).await?;
    let second = execute(&host, request).await?;

    assert_eq!(first, second);
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .manifest(
                &ActorKey {
                    namespace_id: "namespace-1".into(),
                    actor_type: "counter".into(),
                    actor_id: "counter-1".into(),
                }
                .storage_key()
            )
            .await?
            .expect("actor manifest")
            .max_txid(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_duplicate_waits_for_and_replays_the_first_invocation() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(BlockingCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let request = invocation("local-hold", "increment", vec![]);

    let first = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { execute(&host, request).await }
    });
    executor.first_started.notified().await;
    let second = tokio::spawn({
        let host = host.clone();
        async move { execute(&host, request).await }
    });

    tokio::task::yield_now().await;
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    executor.release_first.notify_one();

    assert_eq!(completed(first.await??), json!(1));
    assert_eq!(completed(second.await??), json!(1));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    assert_eq!(executor.maximum_active.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_request_id_reuse_with_a_different_payload_is_rejected() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(BlockingCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;

    let first = tokio::spawn({
        let host = host.clone();
        async move { execute(&host, invocation("local-hold", "increment", vec![json!(1)])).await }
    });
    executor.first_started.notified().await;

    let conflict = tokio::spawn({
        let host = host.clone();
        async move { execute(&host, invocation("local-hold", "increment", vec![json!(2)])).await }
    });
    tokio::task::yield_now().await;
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);

    executor.release_first.notify_one();
    assert_eq!(completed(first.await??), json!(1));
    let conflict = conflict.await??;
    assert_eq!(
        conflict,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure {
                code: "idempotency_key_reused".into(),
                message:
                    "idempotency key local-hold was already used for a different actor invocation"
                        .into(),
            },
        }
    );
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn dropping_a_waiter_does_not_abandon_execution_or_release_the_actor_lock() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(BlockingCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let request = invocation("local-hold", "increment", vec![]);

    let abandoned_waiter = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { execute(&host, request).await }
    });
    executor.first_started.notified().await;
    abandoned_waiter.abort();
    assert!(
        abandoned_waiter
            .await
            .expect_err("the original waiter should be cancelled")
            .is_cancelled()
    );

    let duplicate = tokio::spawn({
        let host = host.clone();
        async move { execute(&host, request).await }
    });
    let next = tokio::spawn({
        let host = host.clone();
        async move {
            execute(
                &host,
                invocation("after-abandoned-waiter", "increment", vec![]),
            )
            .await
        }
    });

    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            executor.second_started.notified()
        )
        .await
        .is_err(),
        "a later invocation entered JavaScript after only the waiter was dropped"
    );
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);

    executor.release_first.notify_one();

    assert_eq!(completed(duplicate.await??), json!(1));
    assert_eq!(completed(next.await??), json!(2));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 2);
    assert_eq!(executor.maximum_active.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn caller_deadline_rolls_back_a_late_completion_before_releasing_the_gate() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(BlockingCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let mut request = invocation("local-hold", "increment", vec![]);
    request.timeout_ms = 5_000;

    let mut first = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { execute(&host, request).await }
    });
    require_executor_start(&executor.first_started, &mut first, "late completion test").await?;

    let deadline_reply = first.await??;
    assert_eq!(
        deadline_reply,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure {
                code: "deadline_exceeded".into(),
                message: "actor invocation deadline exceeded; execution may still complete".into(),
            },
        }
    );
    assert_eq!(executor.active.load(Ordering::SeqCst), 1);
    request.timeout_ms = 10_000;
    let duplicate = tokio::spawn({
        let host = host.clone();
        async move { execute(&host, request).await }
    });
    let next = tokio::spawn({
        let host = host.clone();
        async move {
            let mut request = invocation("after-deadline", "increment", vec![]);
            request.timeout_ms = 2_000;
            execute(&host, request).await
        }
    });
    tokio::task::yield_now().await;
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);

    executor.release_first.notify_one();

    executor.first_started.notified().await;
    executor.release_first.notify_one();
    assert_eq!(completed(duplicate.await??), json!(1));
    assert_eq!(completed(next.await??), json!(2));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 3);
    assert_eq!(executor.maximum_active.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn queued_invocation_that_expires_never_enters_the_actor_executor() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(BlockingCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;

    let first = tokio::spawn({
        let host = host.clone();
        async move { execute(&host, invocation("local-hold", "increment", vec![])).await }
    });
    executor.first_started.notified().await;

    let mut queued_request = invocation("queued-deadline", "increment", vec![]);
    queued_request.timeout_ms = 100;
    let queued = tokio::spawn({
        let host = host.clone();
        let request = queued_request.clone();
        async move { execute(&host, request).await }
    });
    tokio::task::yield_now().await;

    let expired = queued.await??;
    assert!(matches!(
        expired,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "deadline_exceeded"
    ));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);

    executor.release_first.notify_one();
    assert_eq!(completed(first.await??), json!(1));
    tokio::task::yield_now().await;
    assert_eq!(
        executor.invocations.load(Ordering::SeqCst),
        1,
        "expired queued work entered JavaScript after the gate was released"
    );

    queued_request.timeout_ms = 2_000;
    assert_eq!(completed(execute(&host, queued_request).await?), json!(2));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn running_invocation_is_cancelled_and_rolled_back_at_its_deadline() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CooperativeCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let mut request = invocation("cooperative-cancel", "increment", vec![]);
    request.timeout_ms = 5_000;

    let mut first = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { execute(&host, request).await }
    });
    require_executor_start(
        &executor.first_started,
        &mut first,
        "cooperative cancellation test",
    )
    .await?;

    assert!(matches!(
        first.await??,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "deadline_exceeded"
    ));
    executor.first_terminated.notified().await;
    assert_eq!(executor.cancellations.load(Ordering::SeqCst), 1);
    tokio::task::yield_now().await;

    request.timeout_ms = 10_000;
    assert_eq!(completed(execute(&host, request).await?), json!(1));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn host_drain_cancels_running_execution_and_rejects_new_admission() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(CooperativeCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let mut request = invocation("cooperative-cancel", "increment", vec![]);
    request.timeout_ms = 10_000;

    let mut active = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { execute(&host, request).await }
    });
    require_executor_start(&executor.first_started, &mut active, "host drain test").await?;

    host.drain_actor_invocations(ActorDrainReason::Shutdown, Duration::from_secs(2))
        .await?;

    assert_eq!(
        active.await??,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure::cancelled_during_execution(
                "the actor host is shutting down",
            ),
        }
    );
    assert_eq!(executor.cancellations.load(Ordering::SeqCst), 1);
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);

    let rejected = execute(&host, invocation("after-host-drain", "increment", vec![])).await?;
    assert_eq!(
        rejected,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure::cancelled_before_execution(
                "the actor host is shutting down",
            ),
        }
    );
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn host_drain_timeout_does_not_release_an_unterminated_actor_lock() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let executor = Arc::new(BlockingCounterExecutor::default());
    let host = fixture
        .host("node-a", "route-a", "local-a", executor.clone())
        .await?;
    let active = tokio::spawn({
        let host = host.clone();
        async move { execute(&host, invocation("local-hold", "increment", vec![])).await }
    });
    executor.first_started.notified().await;

    host.drain_actor_invocations(ActorDrainReason::Shutdown, Duration::from_millis(50))
        .await
        .expect_err("an unterminated actor must exhaust the drain budget");

    assert_eq!(executor.active.load(Ordering::SeqCst), 1);
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);
    let rejected = execute(
        &host,
        invocation("after-drain-timeout", "increment", vec![]),
    )
    .await?;
    assert!(matches!(
        rejected,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "cancelled"
    ));
    assert_eq!(executor.invocations.load(Ordering::SeqCst), 1);

    executor.release_first.notify_one();
    assert!(matches!(
        active.await??,
        ActorExecutionResult::Failed {
            failure: ActorInvocationFailure { ref code, .. }
        } if code == "cancelled"
    ));
    host.drain_actor_invocations(ActorDrainReason::Shutdown, Duration::from_secs(1))
        .await?;
    assert_eq!(executor.active.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn restores_actor_state_into_a_fresh_sandbox_host() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let first_executor = Arc::new(CounterExecutor::default());
    let first = fixture
        .host("node-a", "route-a", "local-a", first_executor)
        .await?;
    completed(execute(&first, invocation("write-1", "increment", vec![json!(4)])).await?);

    let cold_executor = Arc::new(CounterExecutor::default());
    let cold = fixture
        .host(
            "node-a",
            "route-a",
            "local-after-restart",
            cold_executor.clone(),
        )
        .await?;
    let output = completed(execute(&cold, invocation("read-1", "get_count", vec![])).await?);

    assert_eq!(output, json!(4));
    assert_eq!(cold_executor.invocations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn restores_and_replays_a_receipt_after_owner_handoff() -> Result<()> {
    let fixture = ClusterFixture::new().await?;
    let request = invocation("write-1", "increment", vec![json!(4)]);
    let first_executor = Arc::new(CounterExecutor::default());
    let first = fixture
        .host("node-a", "route-a", "local-a", first_executor.clone())
        .await?;
    assert_eq!(completed(execute(&first, request.clone()).await?), json!(4));
    fixture
        .nodes
        .unregister(first.host_id(), "session-node-a")
        .await?;

    let cold_executor = Arc::new(CounterExecutor::default());
    let cold = fixture
        .host(
            "node-b",
            "route-b",
            "local-after-handoff",
            cold_executor.clone(),
        )
        .await?;
    let replayed = completed(execute(&cold, request).await?);

    assert_eq!(replayed, json!(4));
    assert_eq!(first_executor.invocations.load(Ordering::SeqCst), 1);
    assert_eq!(cold_executor.invocations.load(Ordering::SeqCst), 0);
    Ok(())
}
