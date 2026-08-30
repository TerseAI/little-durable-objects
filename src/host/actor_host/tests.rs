use super::*;
use crate::{
    actor_state::{ActorDatabaseStore, ActorDatabaseTestExt},
    clock::Clock,
    durability::{
        ActorDurabilityStore, ActorManifest, ActorStateRestorer, CapturedActorChanges,
        LocalActorChangeCapture, LocalActorStore, LtxActorStateRestorer, OwnershipClaimResult,
        RecoveryData, VersionedActorManifest,
    },
    host::{ConfirmedLeaseState, HostId},
    host_leases::{
        HostLease, HostLeaseRequest, HostLeaseStatus, HostLeaseStore, LocalHostLeaseStore,
    },
    ltx::ltx_dir,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};
use tempfile::TempDir;

#[test]
fn actor_receipts_retain_the_most_recent_idempotency_window() -> Result<()> {
    let actor = crate::actor::ActorKey {
        namespace_id: "namespace-1".into(),
        actor_type: "counter".into(),
        actor_id: "counter-1".into(),
    };
    let invocation = |index: usize| ActorInvocation {
        request_id: format!("request-{index}"),
        actor: actor.clone(),
        method: "increment".into(),
        args: vec![serde_json::json!(index)],
        timeout_ms: 30_000,
    };
    let mut receipts = ActorInvocationReceipts::default();
    for index in 0..=MAX_ACTOR_RECEIPTS {
        receipts.record(ActorInvocationReceipt::new(
            &invocation(index),
            ActorLocalResult::Completed {
                result: serde_json::json!(index),
            },
        ))?;
    }

    assert!(matches!(
        receipts.lookup(&invocation(0)),
        ActorReceiptLookup::Missing
    ));
    assert!(matches!(
        receipts.lookup(&invocation(MAX_ACTOR_RECEIPTS)),
        ActorReceiptLookup::Replay(ActorLocalResult::Completed { result })
            if result == serde_json::json!(MAX_ACTOR_RECEIPTS)
    ));
    Ok(())
}

struct Fixture {
    _dir: TempDir,
    store: Arc<LocalActorStore>,
    nodes: Arc<LocalHostLeaseStore>,
    databases: Arc<ActorDatabaseStore>,
    dependencies: ActorHostDependencies,
    object: ActorStorageKey,
}

impl Fixture {
    async fn new() -> Result<Self> {
        Self::with_active_nodes(&["node-a", "node-b"]).await
    }

    async fn with_active_nodes(active_nodes: &[&str]) -> Result<Self> {
        let dir = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(dir.path().join("shared")));
        let nodes =
            lease_store_with_active_nodes(dir.path().join("coordination"), active_nodes).await?;
        let databases = Arc::new(ActorDatabaseStore::new(dir.path().join("local")));
        let restore = Arc::new(LtxActorStateRestorer::new(store.clone(), databases.clone()));
        let dependencies = ActorHostDependencies::new(
            store.clone(),
            nodes.clone(),
            databases.clone(),
            Arc::new(LocalActorChangeCapture::new(dir.path().join("local"))),
            restore,
        );

        Ok(Self {
            _dir: dir,
            store,
            nodes,
            databases,
            dependencies,
            object: ActorStorageKey::new("object-x"),
        })
    }

    fn host(&self, node: &str) -> ActorHost {
        ActorHost::new(node_descriptor(node), self.dependencies.clone())
    }

    async fn claim(&self, node: &str) -> Result<VersionedActorManifest> {
        let current = self.store.manifest(&self.object).await?;
        match self
            .store
            .claim(&self.object, current.as_ref(), &HostId::new(node))
            .await?
        {
            OwnershipClaimResult::Acquired(manifest) => Ok(manifest),
            result => anyhow::bail!("fixture claim should succeed, got {result:?}"),
        }
    }

    async fn manifest(&self) -> Result<Option<VersionedActorManifest>> {
        self.store.manifest(&self.object).await
    }

    async fn durable_segments(&self) -> Result<Vec<crate::ltx::LtxSegment>> {
        let Some(manifest) = self.manifest().await? else {
            return Ok(Vec::new());
        };
        self.store
            .canonical_segments(&self.object, &manifest.manifest)
            .await
    }
}

struct CountingObjectStore {
    inner: Arc<LocalActorStore>,
    manifest_calls: AtomicUsize,
    publish_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ActorDurabilityStore for CountingObjectStore {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        self.manifest_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.manifest(object).await
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        node: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.inner.claim(object, expected, node).await
    }

    async fn publish(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<VersionedActorManifest> {
        self.publish_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.publish(object, current, captured).await
    }

    async fn recovery(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<RecoveryData> {
        self.inner.recovery(object, manifest).await
    }
}

struct CountingHostLeaseStore {
    inner: Arc<LocalHostLeaseStore>,
    get_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl HostLeaseStore for CountingHostLeaseStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        self.inner.register(request).await
    }

    async fn lease_status(&self, node: &HostId) -> Result<HostLeaseStatus> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.lease_status(node).await
    }

    async fn unregister(&self, node: &HostId, session_id: &str) -> Result<()> {
        self.inner.unregister(node, session_id).await
    }
}

struct TogglePublishStore {
    inner: Arc<LocalActorStore>,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl ActorDurabilityStore for TogglePublishStore {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        self.inner.manifest(object).await
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        node: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.inner.claim(object, expected, node).await
    }

    async fn publish(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<VersionedActorManifest> {
        if self.fail.load(Ordering::SeqCst) && !captured.is_empty() {
            anyhow::bail!("object storage publication failed");
        }
        self.inner.publish(object, current, captured).await
    }

    async fn recovery(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<RecoveryData> {
        self.inner.recovery(object, manifest).await
    }
}

struct ObservedLtxCapture {
    inner: LocalActorChangeCapture,
    checkpoint_calls: AtomicUsize,
    fail_checkpoint: AtomicBool,
}

impl ObservedLtxCapture {
    fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: LocalActorChangeCapture::new(root),
            checkpoint_calls: AtomicUsize::new(0),
            fail_checkpoint: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl crate::durability::ActorChangeCapture for ObservedLtxCapture {
    async fn prepare(&self, object: &ActorStorageKey) -> Result<()> {
        self.inner.prepare(object).await
    }

    async fn reset(&self, object: &ActorStorageKey) -> Result<()> {
        self.inner.reset(object).await
    }

    async fn capture(&self, object: &ActorStorageKey) -> Result<CapturedActorChanges> {
        self.inner.capture(object).await
    }

    async fn checkpoint_durable(&self, object: &ActorStorageKey, durable_txid: u64) -> Result<()> {
        self.checkpoint_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_checkpoint.load(Ordering::SeqCst) {
            anyhow::bail!("injected WAL checkpoint failure");
        }
        self.inner.checkpoint_durable(object, durable_txid).await
    }
}

struct ToggleRestore {
    inner: Arc<LtxActorStateRestorer>,
    calls: AtomicUsize,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl ActorStateRestorer for ToggleRestore {
    async fn restore(&self, object: &ActorStorageKey, manifest: &ActorManifest) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            anyhow::bail!("restore unavailable");
        }
        self.inner.restore(object, manifest).await
    }
}

struct ManualClock(AtomicU64);

impl ManualClock {
    fn new(now_ms: u64) -> Self {
        Self(AtomicU64::new(now_ms))
    }

    fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

struct OwnershipLookupFixture {
    _dir: TempDir,
    store: Arc<LocalActorStore>,
    counted_store: Arc<CountingObjectStore>,
    counted_leases: Arc<CountingHostLeaseStore>,
    clock: Arc<ManualClock>,
    host: ActorHost,
    object: ActorStorageKey,
}

impl OwnershipLookupFixture {
    async fn new() -> Result<Self> {
        let dir = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(dir.path().join("shared")));
        let counted_store = Arc::new(CountingObjectStore {
            inner: store.clone(),
            manifest_calls: AtomicUsize::new(0),
            publish_calls: AtomicUsize::new(0),
        });
        let nodes = Arc::new(LocalHostLeaseStore::new(dir.path().join("coordination")).await?);
        for node in ["node-a", "node-b"] {
            nodes
                .register(&HostLeaseRequest {
                    id: HostId::new(node),
                    session_id: format!("session-{node}"),
                    route: format!("{node}-sandbox"),
                    duration_ms: 60_000,
                })
                .await?;
        }
        let counted_leases = Arc::new(CountingHostLeaseStore {
            inner: nodes,
            get_calls: AtomicUsize::new(0),
        });
        let local_root = dir.path().join("local");
        let databases = Arc::new(ActorDatabaseStore::new(&local_root));
        let confirmed_lease = Arc::new(ConfirmedLeaseState::new(HostId::new("node-a")));
        confirmed_lease.record_renewal(1_000);
        let clock = Arc::new(ManualClock::new(10));
        let restore = Arc::new(LtxActorStateRestorer::new(
            counted_store.clone(),
            databases.clone(),
        ));
        let dependencies = ActorHostDependencies::new(
            counted_store.clone(),
            counted_leases.clone(),
            databases.clone(),
            Arc::new(LocalActorChangeCapture::new(&local_root)),
            restore,
        )
        .with_clock(clock.clone())
        .with_confirmed_lease(confirmed_lease);

        Ok(Self {
            _dir: dir,
            store,
            counted_store,
            counted_leases,
            clock,
            host: ActorHost::new(node_descriptor("node-a"), dependencies),
            object: ActorStorageKey::new("object-x"),
        })
    }

    async fn warm(&self) -> Result<()> {
        self.host
            .with_actor_ownership(&self.object, |database| {
                database.set("warm", &true)?;
                Ok(())
            })
            .await?;
        self.counted_store.manifest_calls.store(0, Ordering::SeqCst);
        self.counted_store.publish_calls.store(0, Ordering::SeqCst);
        self.counted_leases.get_calls.store(0, Ordering::SeqCst);
        Ok(())
    }
}

fn node_descriptor(id: &str) -> HostEndpoint {
    HostEndpoint {
        id: HostId::new(id),
        route: "127.0.0.1:7000".into(),
    }
}

async fn lease_store_with_active_nodes(
    path: impl Into<std::path::PathBuf>,
    active_nodes: &[&str],
) -> Result<Arc<LocalHostLeaseStore>> {
    let nodes = Arc::new(LocalHostLeaseStore::new(path).await?);
    for id in active_nodes {
        nodes
            .register(&HostLeaseRequest {
                id: HostId::new(*id),
                session_id: format!("session-{id}"),
                route: "127.0.0.1:7000".into(),
                duration_ms: 60_000,
            })
            .await?;
    }
    Ok(nodes)
}

fn host_with_shared_store(
    node: &str,
    local_root: &std::path::Path,
    store: Arc<LocalActorStore>,
    nodes: Arc<LocalHostLeaseStore>,
) -> (ActorHost, Arc<ActorDatabaseStore>) {
    let databases = Arc::new(ActorDatabaseStore::new(local_root));
    let restore = Arc::new(LtxActorStateRestorer::new(store.clone(), databases.clone()));
    let dependencies = ActorHostDependencies::new(
        store,
        nodes,
        databases.clone(),
        Arc::new(LocalActorChangeCapture::new(local_root)),
        restore,
    );
    (
        ActorHost::new(node_descriptor(node), dependencies),
        databases,
    )
}

#[tokio::test]
async fn claims_unowned_object_and_preserves_existing_owner() -> Result<()> {
    let fixture = Fixture::new().await?;
    let host_a = fixture.host("node-a");
    let expected = ActorOwner {
        host: HostId::new("node-a"),
        epoch: 1,
    };

    assert_eq!(
        host_a.ensure_ownership(&fixture.object).await?,
        EnsureOwnershipResult::Owned(expected.clone())
    );
    assert_eq!(
        fixture
            .host("node-b")
            .ensure_ownership(&fixture.object)
            .await?,
        EnsureOwnershipResult::NotOwner(expected.clone())
    );
    assert_eq!(fixture.manifest().await?.unwrap().owner(), &expected);
    Ok(())
}

#[tokio::test]
async fn concurrent_initial_claims_have_one_winner() -> Result<()> {
    let fixture = Fixture::new().await?;
    let host_a = fixture.host("node-a");
    let host_b = fixture.host("node-b");
    let (a, b) = tokio::join!(
        host_a.ensure_ownership(&fixture.object),
        host_b.ensure_ownership(&fixture.object)
    );

    let winner = match (a?, b?) {
        (EnsureOwnershipResult::Owned(owner), EnsureOwnershipResult::NotOwner(observed))
        | (EnsureOwnershipResult::NotOwner(observed), EnsureOwnershipResult::Owned(owner)) => {
            assert_eq!(owner, observed);
            owner
        }
        pair => panic!("expected one winner and one observer, got {pair:?}"),
    };
    assert_eq!(winner.epoch, 1);
    assert_eq!(fixture.manifest().await?.unwrap().owner(), &winner);
    Ok(())
}

#[tokio::test]
async fn takes_over_stale_owner_in_one_manifest_cas() -> Result<()> {
    let fixture = Fixture::with_active_nodes(&["node-a"]).await?;
    let stale = fixture.claim("node-b").await?;
    let replacement = ActorOwner {
        host: HostId::new("node-a"),
        epoch: 2,
    };

    assert_eq!(
        fixture
            .host("node-a")
            .ensure_ownership(&fixture.object)
            .await?,
        EnsureOwnershipResult::Owned(replacement.clone())
    );
    let manifest = fixture.manifest().await?.unwrap();
    assert_eq!(manifest.owner(), &replacement);
    assert_eq!(manifest.max_txid(), stale.max_txid());
    Ok(())
}

#[tokio::test]
async fn host_without_active_lease_cannot_claim() -> Result<()> {
    let fixture = Fixture::with_active_nodes(&[]).await?;
    fixture
        .host("node-a")
        .ensure_ownership(&fixture.object)
        .await
        .expect_err("a host without a lease must be fenced");
    assert!(fixture.manifest().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn warm_mutation_reads_manifest_but_uses_confirmed_lease() -> Result<()> {
    let fixture = OwnershipLookupFixture::new().await?;
    fixture.warm().await?;

    let result = fixture
        .host
        .with_actor_ownership(&fixture.object, |database| {
            database.set("second", &true)?;
            Ok(())
        })
        .await?;

    assert_eq!(result, OwnershipGuardResult::Completed(()));
    assert_eq!(
        fixture.counted_store.manifest_calls.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture.counted_store.publish_calls.load(Ordering::SeqCst),
        1
    );
    assert_eq!(fixture.counted_leases.get_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .store
            .manifest(&fixture.object)
            .await?
            .unwrap()
            .max_txid(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn warm_read_reads_manifest_but_uses_confirmed_lease() -> Result<()> {
    let fixture = OwnershipLookupFixture::new().await?;
    fixture.warm().await?;

    let result = fixture
        .host
        .with_actor_ownership(&fixture.object, |database| {
            database.get::<bool>("warm").map_err(Into::into)
        })
        .await?;

    assert_eq!(result, OwnershipGuardResult::Completed(Some(true)));
    assert_eq!(
        fixture.counted_store.manifest_calls.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture.counted_store.publish_calls.load(Ordering::SeqCst),
        0
    );
    assert_eq!(fixture.counted_leases.get_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn fresh_manifest_observes_ownership_replacement_before_execution() -> Result<()> {
    let fixture = OwnershipLookupFixture::new().await?;
    fixture.warm().await?;
    let current = fixture.store.manifest(&fixture.object).await?.unwrap();
    let replacement = match fixture
        .store
        .claim(&fixture.object, Some(&current), &HostId::new("node-b"))
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => anyhow::bail!("test takeover failed: {result:?}"),
    };

    let calls = AtomicUsize::new(0);
    let result = fixture
        .host
        .with_actor_ownership(&fixture.object, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await?;

    assert_eq!(
        result,
        OwnershipGuardResult::NotOwner(replacement.owner().clone())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.counted_store.manifest_calls.load(Ordering::SeqCst),
        1
    );
    assert_eq!(fixture.counted_leases.get_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.store.manifest(&fixture.object).await?.unwrap(),
        replacement
    );
    assert_eq!(
        fixture
            .store
            .canonical_segments(&fixture.object, &replacement.manifest)
            .await?
            .len(),
        1,
        "no bundle should be published after observing the new owner"
    );
    Ok(())
}

#[tokio::test]
async fn expired_confirmed_lease_fences_before_execution() -> Result<()> {
    let fixture = OwnershipLookupFixture::new().await?;
    fixture.warm().await?;
    fixture.clock.set(1_000);
    let calls = AtomicUsize::new(0);

    let error = fixture
        .host
        .with_actor_ownership(&fixture.object, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .expect_err("expired lease must self-fence");

    assert!(error.to_string().contains("does not hold an active lease"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.counted_store.manifest_calls.load(Ordering::SeqCst),
        1
    );
    assert_eq!(fixture.counted_leases.get_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn restore_failure_prevents_execution_and_is_retried() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let store = Arc::new(LocalActorStore::new(dir.path().join("shared")));
    let nodes = lease_store_with_active_nodes(dir.path().join("coordination"), &["node-a"]).await?;
    let local_root = dir.path().join("local");
    let databases = Arc::new(ActorDatabaseStore::new(&local_root));
    let restore = Arc::new(ToggleRestore {
        inner: Arc::new(LtxActorStateRestorer::new(store.clone(), databases.clone())),
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(true),
    });
    let host = ActorHost::new(
        node_descriptor("node-a"),
        ActorHostDependencies::new(
            store,
            nodes,
            databases,
            Arc::new(LocalActorChangeCapture::new(&local_root)),
            restore.clone(),
        ),
    );
    let operation_calls = AtomicUsize::new(0);

    host.with_actor_ownership(&object, |_| {
        operation_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await
    .expect_err("restore failure must prevent execution");
    assert_eq!(operation_calls.load(Ordering::SeqCst), 0);

    restore.fail.store(false, Ordering::SeqCst);
    host.with_actor_ownership(&object, |_| {
        operation_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await?;
    host.with_actor_ownership(&object, |_| Ok(())).await?;
    assert_eq!(operation_calls.load(Ordering::SeqCst), 1);
    assert_eq!(restore.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn a_new_host_process_reuses_a_current_provider_volume_cache() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let store = Arc::new(LocalActorStore::new(dir.path().join("shared")));
    let nodes = lease_store_with_active_nodes(dir.path().join("coordination"), &["node-a"]).await?;
    let local_root = dir.path().join("provider-volume");
    let (first_host, databases) =
        host_with_shared_store("node-a", &local_root, store.clone(), nodes.clone());
    first_host
        .with_actor_ownership(&object, |database| {
            database.set("counter", &41).map_err(Into::into)
        })
        .await?;
    drop(first_host);

    let restore = Arc::new(ToggleRestore {
        inner: Arc::new(LtxActorStateRestorer::new(store.clone(), databases.clone())),
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(true),
    });
    let restarted = ActorHost::new(
        node_descriptor("node-a"),
        ActorHostDependencies::new(
            store,
            nodes,
            databases,
            Arc::new(LocalActorChangeCapture::new(&local_root)),
            restore.clone(),
        ),
    );

    let value = restarted
        .with_actor_ownership(&object, |database| {
            database.get::<u64>("counter").map_err(Into::into)
        })
        .await?;

    assert_eq!(value, OwnershipGuardResult::Completed(Some(41)));
    assert_eq!(restore.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn operations_publish_contiguous_history_and_reads_do_not_advance_it() -> Result<()> {
    let fixture = Fixture::new().await?;
    let host = fixture.host("node-a");
    host.with_actor_ownership(&fixture.object, |database| {
        database.set("counter", &41)?;
        Ok(())
    })
    .await?;
    let result = host
        .with_actor_ownership(&fixture.object, |database| {
            let counter = database.get::<i32>("counter")?.unwrap_or_default() + 1;
            database.set("counter", &counter)?;
            Ok(counter)
        })
        .await?;
    host.with_actor_ownership(&fixture.object, |database| {
        database.get::<i32>("counter").map_err(Into::into)
    })
    .await?;

    assert_eq!(result, OwnershipGuardResult::Completed(42));
    assert_eq!(
        fixture
            .durable_segments()
            .await?
            .iter()
            .map(|segment| (segment.min_txid, segment.max_txid))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2)]
    );
    assert_eq!(fixture.manifest().await?.unwrap().max_txid(), 2);
    Ok(())
}

#[tokio::test]
async fn non_owner_does_not_open_or_execute_actor() -> Result<()> {
    let fixture = Fixture::new().await?;
    let current = fixture.claim("node-b").await?;
    let calls = AtomicUsize::new(0);
    let result = fixture
        .host("node-a")
        .with_actor_ownership(&fixture.object, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await?;

    assert_eq!(
        result,
        OwnershipGuardResult::NotOwner(current.owner().clone())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!fixture.databases.path_for(&fixture.object)?.exists());
    Ok(())
}

#[tokio::test]
async fn publication_failure_forces_restore_before_next_execution() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let inner = Arc::new(LocalActorStore::new(dir.path().join("shared")));
    let store = Arc::new(TogglePublishStore {
        inner: inner.clone(),
        fail: AtomicBool::new(true),
    });
    let nodes = lease_store_with_active_nodes(dir.path().join("coordination"), &["node-a"]).await?;
    let local_root = dir.path().join("local");
    let databases = Arc::new(ActorDatabaseStore::new(&local_root));
    let restore = Arc::new(LtxActorStateRestorer::new(store.clone(), databases.clone()));
    let capture = Arc::new(ObservedLtxCapture::new(&local_root));
    let host = ActorHost::new(
        node_descriptor("node-a"),
        ActorHostDependencies::new(
            store.clone(),
            nodes,
            databases.clone(),
            capture.clone(),
            restore,
        ),
    );

    let error = host
        .with_actor_ownership(&object, |database| {
            database.set("rejected", &true)?;
            Ok(())
        })
        .await
        .expect_err("publication failure must prevent acknowledgement");
    assert_eq!(error.to_string(), "object storage publication failed");
    assert!(
        ltx_dir(&databases.path_for(&object)?)
            .join("0000000000000001-0000000000000001.ltx")
            .is_file()
    );
    assert_eq!(inner.manifest(&object).await?.unwrap().max_txid(), 0);
    assert_eq!(
        capture.checkpoint_calls.load(Ordering::SeqCst),
        0,
        "a failed publication must not permit WAL recycling"
    );

    store.fail.store(false, Ordering::SeqCst);
    host.with_actor_ownership(&object, |database| {
        assert_eq!(database.get::<bool>("rejected")?, None);
        database.set("accepted", &true)?;
        Ok(())
    })
    .await?;
    assert_eq!(capture.checkpoint_calls.load(Ordering::SeqCst), 1);
    assert_eq!(inner.manifest(&object).await?.unwrap().max_txid(), 1);
    assert_eq!(
        databases.open(&object)?.get::<bool>("accepted")?,
        Some(true)
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_failure_does_not_turn_a_durable_write_into_a_failed_request() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let store = Arc::new(LocalActorStore::new(dir.path().join("shared")));
    let nodes = lease_store_with_active_nodes(dir.path().join("coordination"), &["node-a"]).await?;
    let local_root = dir.path().join("local");
    let databases = Arc::new(ActorDatabaseStore::new(&local_root));
    let restore = Arc::new(LtxActorStateRestorer::new(store.clone(), databases.clone()));
    let capture = Arc::new(ObservedLtxCapture::new(&local_root));
    capture.fail_checkpoint.store(true, Ordering::SeqCst);
    let host = ActorHost::new(
        node_descriptor("node-a"),
        ActorHostDependencies::new(store.clone(), nodes, databases, capture.clone(), restore),
    );

    assert_eq!(
        host.with_actor_ownership(&object, |database| {
            database.set("first", &true)?;
            Ok("acknowledged")
        })
        .await?,
        OwnershipGuardResult::Completed("acknowledged")
    );
    assert_eq!(store.manifest(&object).await?.unwrap().max_txid(), 1);
    assert_eq!(capture.checkpoint_calls.load(Ordering::SeqCst), 1);

    // The untruncated WAL remains valid, so a later transaction can still be captured
    // and published while checkpointing is unavailable.
    host.with_actor_ownership(&object, |database| {
        database.set("second", &true)?;
        Ok(())
    })
    .await?;
    assert_eq!(store.manifest(&object).await?.unwrap().max_txid(), 2);
    assert_eq!(capture.checkpoint_calls.load(Ordering::SeqCst), 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takeover_during_mutation_fences_manifest_advance() -> Result<()> {
    let fixture = Fixture::new().await?;
    let current = fixture.claim("node-a").await?;
    let host = Arc::new(fixture.host("node-a"));
    let object = fixture.object.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let operation = tokio::spawn(async move {
        host.with_actor_ownership(&object, move |database| {
            database.set("rejected", &true)?;
            started_tx.send(()).expect("start receiver");
            release_rx.recv().expect("release sender");
            Ok(())
        })
        .await
    });

    started_rx.recv_timeout(Duration::from_secs(1))?;
    let replacement = match fixture
        .store
        .claim(&fixture.object, Some(&current), &HostId::new("node-b"))
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => anyhow::bail!("test takeover failed: {result:?}"),
    };
    release_tx.send(())?;
    let error = operation
        .await?
        .expect_err("old owner must not advance the manifest");

    assert_eq!(
        error.to_string(),
        "actor manifest changed while publishing object-x"
    );
    assert_eq!(replacement.owner().epoch, 2);
    assert_eq!(
        fixture.store.manifest(&fixture.object).await?.unwrap(),
        replacement
    );
    assert!(fixture.durable_segments().await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_expiration_after_publish_prevents_ack_but_keeps_canonical_commit() -> Result<()> {
    let fixture = Fixture::new().await?;
    let host = Arc::new(fixture.host("node-a"));
    let object = fixture.object.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let operation = tokio::spawn(async move {
        host.with_actor_ownership(&object, move |database| {
            database.set("committed", &true)?;
            started_tx.send(()).expect("start receiver");
            release_rx.recv().expect("release sender");
            Ok(())
        })
        .await
    });

    started_rx.recv_timeout(Duration::from_secs(1))?;
    fixture
        .nodes
        .register(&HostLeaseRequest {
            id: HostId::new("node-a"),
            session_id: "session-node-a".into(),
            route: "127.0.0.1:7000".into(),
            duration_ms: 1,
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(2)).await;
    release_tx.send(())?;
    let error = operation
        .await?
        .expect_err("expired lease must prevent ack");

    assert_eq!(error.to_string(), "lost ownership for object-x");
    assert_eq!(fixture.durable_segments().await?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_actor_operations_are_serialized() -> Result<()> {
    let fixture = Fixture::new().await?;
    let host = Arc::new(fixture.host("node-a"));
    let object = fixture.object.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first = tokio::spawn({
        let host = host.clone();
        let object = object.clone();
        async move {
            host.with_actor_ownership(&object, move |database| {
                started_tx.send(()).expect("start receiver");
                release_rx.recv().expect("release sender");
                database.set("sequence", &1)?;
                Ok(())
            })
            .await
        }
    });
    started_rx.recv_timeout(Duration::from_secs(1))?;

    let (second_started_tx, second_started_rx) = mpsc::channel();
    let second = tokio::spawn({
        let host = host.clone();
        async move {
            host.with_actor_ownership(&object, move |database| {
                second_started_tx.send(()).expect("second receiver");
                database.get::<i32>("sequence").map_err(Into::into)
            })
            .await
        }
    });
    assert!(
        second_started_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    release_tx.send(())?;
    first.await??;
    second_started_rx.recv_timeout(Duration::from_secs(1))?;
    assert_eq!(second.await??, OwnershipGuardResult::Completed(Some(1)));
    Ok(())
}

#[tokio::test]
async fn fresh_host_restores_and_continues_history_after_takeover() -> Result<()> {
    let dir = TempDir::new()?;
    let store = Arc::new(LocalActorStore::new(dir.path().join("shared")));
    let nodes =
        lease_store_with_active_nodes(dir.path().join("coordination"), &["node-a", "node-b"])
            .await?;
    let object = ActorStorageKey::new("object-x");
    let (host_a, _) = host_with_shared_store(
        "node-a",
        &dir.path().join("node-a"),
        store.clone(),
        nodes.clone(),
    );

    for expected in [1, 2] {
        assert_eq!(
            host_a
                .with_actor_ownership(&object, |database| {
                    let next = database.get::<i32>("counter")?.unwrap_or_default() + 1;
                    database.set("counter", &next)?;
                    Ok(next)
                })
                .await?,
            OwnershipGuardResult::Completed(expected)
        );
    }
    nodes
        .register(&HostLeaseRequest {
            id: HostId::new("node-a"),
            session_id: "session-node-a".into(),
            route: "dead".into(),
            duration_ms: 1,
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(2)).await;

    let node_b_root = dir.path().join("node-b");
    let (host_b, databases_b) =
        host_with_shared_store("node-b", &node_b_root, store.clone(), nodes);
    let result = host_b
        .with_actor_ownership(&object, |database| {
            let next = database.get::<i32>("counter")?.unwrap_or_default() + 1;
            database.set("counter", &next)?;
            Ok(next)
        })
        .await?;

    assert_eq!(result, OwnershipGuardResult::Completed(3));
    assert_eq!(databases_b.open(&object)?.get::<i32>("counter")?, Some(3));
    let manifest = store.manifest(&object).await?.unwrap();
    assert_eq!(manifest.owner().host, HostId::new("node-b"));
    assert_eq!(manifest.owner().epoch, 2);
    assert_eq!(manifest.max_txid(), 3);
    assert_eq!(
        store
            .canonical_segments(&object, &manifest.manifest)
            .await?
            .len(),
        3
    );
    Ok(())
}
