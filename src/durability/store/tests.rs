use super::*;
use crate::{
    actor_state::{ActorDatabaseStore, ActorDatabaseTestExt},
    durability::{ActorChangeCapture, LocalActorChangeCapture},
};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tempfile::TempDir;

async fn capture_writes(
    root: &std::path::Path,
    object: &ActorStorageKey,
    writes: &[(&str, &str)],
) -> Result<CapturedActorChanges> {
    let capture = LocalActorChangeCapture::new(root);
    capture.prepare(object).await?;
    let database = ActorDatabaseStore::new(root).open(object)?;
    for (key, value) in writes {
        database.set(key, value)?;
    }
    capture.capture(object).await
}

#[tokio::test]
async fn claim_publish_and_restore_use_one_manifest_and_commit_bundles() -> Result<()> {
    let dir = TempDir::new()?;
    let store = LocalActorStore::new(dir.path().join("store"));
    let object = ActorStorageKey::new("object-x");
    assert_eq!(store.manifest(&object).await?, None);

    let claimed = match store.claim(&object, None, &HostId::new("node-a")).await? {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("initial claim should succeed: {result:?}"),
    };
    assert_eq!(claimed.owner().epoch, 1);
    assert_eq!(claimed.max_txid(), 0);
    let manifest_json = serde_json::to_value(&claimed.manifest)?;
    assert_eq!(manifest_json["storage_region"], "default");
    assert_eq!(manifest_json["owner"]["node"], "node-a");
    assert!(manifest_json.get("home_region").is_none());

    let captured = capture_writes(
        &dir.path().join("node-a"),
        &object,
        &[("first", "one"), ("second", "two")],
    )
    .await?;
    let published = store.publish(&object, &claimed, &captured).await?;

    assert_eq!(published.max_txid(), 2);
    assert_eq!(store.manifest(&object).await?, Some(published.clone()));
    let restored = store
        .canonical_segments(&object, &published.manifest)
        .await?;
    assert_eq!(restored.len(), 2);
    assert_eq!(restored[0].bytes, captured.segments()[0].bytes);
    assert_eq!(restored[1].bytes, captured.segments()[1].bytes);
    Ok(())
}

#[tokio::test]
async fn takeover_is_one_manifest_cas_and_retains_the_recovery_tip() -> Result<()> {
    let dir = TempDir::new()?;
    let store = LocalActorStore::new(dir.path().join("store"));
    let object = ActorStorageKey::new("object-x");
    let first = match store.claim(&object, None, &HostId::new("node-a")).await? {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("initial claim should succeed: {result:?}"),
    };
    let captured = capture_writes(&dir.path().join("node-a"), &object, &[("key", "value")]).await?;
    let first = store.publish(&object, &first, &captured).await?;

    let second = match store
        .claim(&object, Some(&first), &HostId::new("node-b"))
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("takeover should succeed: {result:?}"),
    };

    assert_eq!(second.owner().epoch, 2);
    assert_eq!(second.owner().host, HostId::new("node-b"));
    assert_eq!(second.manifest.tip, first.manifest.tip);
    assert_eq!(
        store
            .canonical_segments(&object, &second.manifest)
            .await?
            .len(),
        1
    );
    Ok(())
}

#[derive(Clone, Default)]
struct MemoryPersistence {
    state: Arc<Mutex<MemoryState>>,
    fail_next_manifest_write: Arc<AtomicBool>,
    immutable_puts: Arc<AtomicUsize>,
    conditional_writes: Arc<AtomicUsize>,
}

#[derive(Default)]
struct MemoryState {
    next_version: u64,
    blobs: HashMap<String, (Vec<u8>, u64)>,
}

impl MemoryPersistence {
    fn version(value: u64) -> ManifestVersion {
        ManifestVersion::from_bytes(value.to_string().into_bytes())
    }

    fn decode_version(value: &ManifestVersion) -> u64 {
        std::str::from_utf8(value.as_bytes())
            .expect("memory version UTF-8")
            .parse()
            .expect("memory version integer")
    }
}

#[async_trait]
impl ManifestPersistence for MemoryPersistence {
    async fn load(&self, key: &str) -> Result<Option<VersionedBlob>> {
        Ok(self
            .state
            .lock()
            .expect("memory store lock")
            .blobs
            .get(key)
            .map(|(bytes, version)| VersionedBlob {
                bytes: bytes.clone(),
                version: Self::version(*version),
            }))
    }

    async fn write_if_version(
        &self,
        key: &str,
        expected: Option<&ManifestVersion>,
        bytes: &[u8],
    ) -> Result<Option<ManifestVersion>> {
        self.conditional_writes.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_manifest_write.swap(false, Ordering::SeqCst) {
            return Ok(None);
        }
        let mut state = self.state.lock().expect("memory store lock");
        let current = state.blobs.get(key).map(|(_, version)| *version);
        if current != expected.map(Self::decode_version) {
            return Ok(None);
        }
        state.next_version += 1;
        let version = state.next_version;
        state.blobs.insert(key.into(), (bytes.to_vec(), version));
        Ok(Some(Self::version(version)))
    }
}

#[async_trait]
impl ImmutablePersistence for MemoryPersistence {
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .state
            .lock()
            .expect("memory store lock")
            .blobs
            .get(key)
            .map(|(bytes, _)| bytes.clone()))
    }

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let mut state = self.state.lock().expect("memory store lock");
        if let Some((existing, _)) = state.blobs.get(key) {
            ensure!(existing == bytes, "immutable conflict for {key}");
            return Ok(());
        }
        state.next_version += 1;
        let version = state.next_version;
        state.blobs.insert(key.into(), (bytes.to_vec(), version));
        self.immutable_puts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl ArchiveStore for MemoryPersistence {
    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        ImmutablePersistence::put_immutable(self, key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        ImmutablePersistence::load(self, key).await
    }
}

fn memory_store(persistence: MemoryPersistence) -> RegionalActorStore {
    RegionalActorStore::new(
        Arc::new(PersistedManifestStore::new(persistence.clone())),
        Arc::new(PersistedCommitStore::new(persistence)),
    )
}

#[tokio::test]
async fn tiered_recovery_falls_back_to_the_archive() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let position = CommitPosition {
        epoch: 3,
        log_generation: 2,
        max_txid: 145,
    };
    let archive: Arc<dyn ArchiveStore> = Arc::new(LocalArchiveStore::new(dir.path()));
    let id = CommitLogId::from(&position);
    archive
        .put_immutable(
            &archive_log_key(&object, &id),
            &rapid::encode_rapid_frame(&position, b"commit-bundle")?,
        )
        .await?;
    let hot: Arc<dyn CommitStore> =
        Arc::new(PersistedCommitStore::new(MemoryPersistence::default()));
    let tiered = TieredCommitStore::new(hot, archive);

    assert_eq!(
        tiered.get(&object, &position).await?.as_deref(),
        Some(b"commit-bundle".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn one_mutation_uploads_exactly_one_immutable_bundle() -> Result<()> {
    let dir = TempDir::new()?;
    let persistence = MemoryPersistence::default();
    let store = memory_store(persistence.clone());
    let object = ActorStorageKey::new("object-x");
    let claimed = match store.claim(&object, None, &HostId::new("node-a")).await? {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("initial claim should succeed: {result:?}"),
    };
    let captured = capture_writes(
        &dir.path().join("node-a"),
        &object,
        &[("first", "one"), ("second", "two")],
    )
    .await?;
    persistence.conditional_writes.store(0, Ordering::SeqCst);

    store.publish(&object, &claimed, &captured).await?;

    assert_eq!(persistence.immutable_puts.load(Ordering::SeqCst), 1);
    assert_eq!(persistence.conditional_writes.load(Ordering::SeqCst), 1);
    let keys = persistence
        .state
        .lock()
        .expect("memory store lock")
        .blobs
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 2, "one manifest and one commit bundle");
    assert_eq!(
        keys.iter().filter(|key| key.ends_with(".ltxpack")).count(),
        1
    );
    assert_eq!(
        keys.iter()
            .filter(|key| key.ends_with("manifest.json"))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn retrying_the_same_publish_returns_the_already_committed_manifest() -> Result<()> {
    let dir = TempDir::new()?;
    let persistence = MemoryPersistence::default();
    let store = memory_store(persistence.clone());
    let object = ActorStorageKey::new("object-x");
    let claimed = match store.claim(&object, None, &HostId::new("node-a")).await? {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("initial claim should succeed: {result:?}"),
    };
    let captured = capture_writes(&dir.path().join("node-a"), &object, &[("key", "value")]).await?;
    persistence.conditional_writes.store(0, Ordering::SeqCst);

    let first = store.publish(&object, &claimed, &captured).await?;
    let retry = store.publish(&object, &claimed, &captured).await?;

    assert_eq!(retry, first);
    assert_eq!(store.manifest(&object).await?, Some(first));
    assert_eq!(persistence.immutable_puts.load(Ordering::SeqCst), 1);
    assert_eq!(persistence.conditional_writes.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn failed_manifest_cas_leaves_bundle_outside_canonical_history() -> Result<()> {
    let dir = TempDir::new()?;
    let persistence = MemoryPersistence::default();
    let store = memory_store(persistence.clone());
    let object = ActorStorageKey::new("object-x");
    let claimed = match store.claim(&object, None, &HostId::new("node-a")).await? {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("initial claim should succeed: {result:?}"),
    };
    let captured = capture_writes(&dir.path().join("node-a"), &object, &[("key", "value")]).await?;
    persistence
        .fail_next_manifest_write
        .store(true, Ordering::SeqCst);

    store
        .publish(&object, &claimed, &captured)
        .await
        .expect_err("manifest CAS must fail");

    let manifest = store.manifest(&object).await?.expect("claimed manifest");
    assert_eq!(manifest.max_txid(), 0);
    assert!(
        store
            .canonical_segments(&object, &manifest.manifest)
            .await?
            .is_empty()
    );
    assert_eq!(persistence.immutable_puts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn takeover_manifest_cas_fences_a_late_old_owner_publish() -> Result<()> {
    let dir = TempDir::new()?;
    let persistence = MemoryPersistence::default();
    let store = memory_store(persistence);
    let object = ActorStorageKey::new("object-x");
    let first = match store.claim(&object, None, &HostId::new("node-a")).await? {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("initial claim should succeed: {result:?}"),
    };
    let capture = LocalActorChangeCapture::new(dir.path().join("node-a"));
    capture.prepare(&object).await?;
    let database = ActorDatabaseStore::new(dir.path().join("node-a")).open(&object)?;
    database.set("counter", &1)?;
    let first_capture = capture.capture(&object).await?;
    let first = store.publish(&object, &first, &first_capture).await?;

    database.set("counter", &2)?;
    let late_capture = capture.capture(&object).await?;
    let second = match store
        .claim(&object, Some(&first), &HostId::new("node-b"))
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("takeover should succeed: {result:?}"),
    };

    store
        .publish(&object, &first, &late_capture)
        .await
        .expect_err("old manifest generation must be fenced");

    assert_eq!(store.manifest(&object).await?, Some(second.clone()));
    assert_eq!(second.max_txid(), 1);
    assert_eq!(
        store
            .canonical_segments(&object, &second.manifest)
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_initial_claims_have_one_manifest_winner() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let store = Arc::new(memory_store(persistence));
    let object = ActorStorageKey::new("object-x");
    let first = {
        let store = store.clone();
        let object = object.clone();
        async move { store.claim(&object, None, &HostId::new("node-a")).await }
    };
    let second = {
        let store = store.clone();
        let object = object.clone();
        async move { store.claim(&object, None, &HostId::new("node-b")).await }
    };
    let (first, second) = tokio::join!(first, second);
    let results = [first?, second?];
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, OwnershipClaimResult::Acquired(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, OwnershipClaimResult::Conflict(Some(_))))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn initial_region_is_immutable_across_owner_handoffs() -> Result<()> {
    let store = memory_store(MemoryPersistence::default());
    let object = ActorStorageKey::new("regional-object");
    let first = match store
        .claim_in_home_region(&object, None, &HostId::new("node-a"), "us-east")
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        OwnershipClaimResult::Conflict(_) => anyhow::bail!("initial regional claim conflicted"),
    };
    assert_eq!(first.manifest.home_region, "us-east");

    let error = store
        .claim_in_home_region(&object, Some(&first), &HostId::new("node-b"), "eu-west")
        .await
        .expect_err("an ownership handoff must not move the object home region");
    assert!(
        error
            .to_string()
            .contains("object home region cannot change")
    );

    let handed_off = match store
        .claim_in_home_region(&object, Some(&first), &HostId::new("node-b"), "us-east")
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        OwnershipClaimResult::Conflict(_) => anyhow::bail!("same-region handoff conflicted"),
    };
    assert_eq!(handed_off.manifest.home_region, "us-east");
    assert_eq!(handed_off.owner().host, HostId::new("node-b"));
    Ok(())
}

#[tokio::test]
async fn consolidated_checkpoints_follow_the_objects_standard_multi_region_map() -> Result<()> {
    let dir = TempDir::new()?;
    let manifests = Arc::new(PersistedManifestStore::new(MemoryPersistence::default()));
    let us_commits: Arc<dyn CommitStore> =
        Arc::new(PersistedCommitStore::new(MemoryPersistence::default()));
    let eu_commits: Arc<dyn CommitStore> =
        Arc::new(PersistedCommitStore::new(MemoryPersistence::default()));
    let us_standard = MemoryPersistence::default();
    let eu_standard = MemoryPersistence::default();
    let store = RegionalActorStore::with_region_stores(
        manifests,
        HashMap::from([
            ("us-east".into(), us_commits),
            ("eu-west".into(), eu_commits),
        ]),
        HashMap::from([
            (
                "us-east".into(),
                Arc::new(us_standard.clone()) as Arc<dyn ArchiveStore>,
            ),
            (
                "eu-west".into(),
                Arc::new(eu_standard.clone()) as Arc<dyn ArchiveStore>,
            ),
        ]),
    )?;

    for (object_name, region, fill) in [
        ("us-object", "us-east", 0x11),
        ("eu-object", "eu-west", 0x22),
    ] {
        let object = ActorStorageKey::new(object_name);
        let claimed = match store
            .claim_in_home_region(&object, None, &HostId::new("node-a"), region)
            .await?
        {
            OwnershipClaimResult::Acquired(manifest) => manifest,
            OwnershipClaimResult::Conflict(_) => anyhow::bail!("initial claim conflicted"),
        };
        let captured =
            capture_writes(&dir.path().join(object_name), &object, &[("key", "value")]).await?;
        let published = store.publish(&object, &claimed, &captured).await?;
        store
            .install_checkpoint(&object, &published, &vec![fill; 4096], 4096, fill.into())
            .await?
            .context("checkpoint manifest CAS did not complete")?;
    }

    let us_keys = us_standard
        .state
        .lock()
        .expect("US Standard store lock")
        .blobs
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let eu_keys = eu_standard
        .state
        .lock()
        .expect("EU Standard store lock")
        .blobs
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        us_keys
            .iter()
            .any(|key| key.contains("us-object/checkpoints/"))
    );
    assert!(
        !us_keys
            .iter()
            .any(|key| key.contains("eu-object/checkpoints/"))
    );
    assert!(
        eu_keys
            .iter()
            .any(|key| key.contains("eu-object/checkpoints/"))
    );
    assert!(
        !eu_keys
            .iter()
            .any(|key| key.contains("us-object/checkpoints/"))
    );
    Ok(())
}
