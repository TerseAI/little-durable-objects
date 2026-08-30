use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;

use super::{
    ActorDurabilityStore, ActorManifest, ArchiveStore, CommitPosition, CommitStore,
    ImmutablePersistence, ManifestPersistence, ManifestStore, ManifestVersion,
    OwnershipClaimResult, PersistedCommitStore, PersistedManifestStore, RegionalActorStore,
    VersionedActorManifest, VersionedBlob,
};
use crate::{actor_state::ActorStorageKey, durability::CapturedActorChanges, host::HostId};

#[cfg(test)]
use crate::ltx::LtxSegment;

static LOCAL_STORE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct LocalFiles {
    root: PathBuf,
}

impl LocalFiles {
    fn path(&self, key: &str) -> PathBuf {
        key.split('/')
            .fold(self.root.clone(), |path, part| path.join(part))
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match std::fs::read(self.path(key)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn write_atomically(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path(key);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("durable object path has no parent: {}", path.display()))?;
        std::fs::create_dir_all(parent)?;
        let sequence = WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("durable object path has no file name: {}", path.display()))?;
        let temporary = parent.join(format!(".{name}.{}.{sequence}.tmp", std::process::id()));

        if let Err(error) = (|| -> Result<()> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &path)?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok(())
        })() {
            let _ = std::fs::remove_file(&temporary);
            return Err(error);
        }
        Ok(())
    }
}

struct LocalManifestPersistence {
    files: LocalFiles,
}

struct LocalImmutablePersistence {
    files: LocalFiles,
}

#[async_trait]
impl ManifestPersistence for LocalManifestPersistence {
    async fn load(&self, key: &str) -> Result<Option<VersionedBlob>> {
        Ok(self.files.read(key)?.map(|bytes| VersionedBlob {
            version: ManifestVersion::from_bytes(bytes.clone()),
            bytes,
        }))
    }

    async fn write_if_version(
        &self,
        key: &str,
        expected: Option<&ManifestVersion>,
        bytes: &[u8],
    ) -> Result<Option<ManifestVersion>> {
        let _guard = LOCAL_STORE_LOCK.lock().await;
        let current = self.files.read(key)?;
        if current.as_deref() != expected.map(ManifestVersion::as_bytes) {
            return Ok(None);
        }
        self.files.write_atomically(key, bytes)?;
        Ok(Some(ManifestVersion::from_bytes(bytes.to_vec())))
    }
}

#[async_trait]
impl ImmutablePersistence for LocalImmutablePersistence {
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.files.read(key)
    }

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let _guard = LOCAL_STORE_LOCK.lock().await;
        if let Some(existing) = self.files.read(key)? {
            if existing == bytes {
                return Ok(());
            }
            bail!("immutable durable object already exists with different bytes: {key}");
        }
        self.files.write_atomically(key, bytes)
    }
}

pub struct LocalManifestStore {
    inner: PersistedManifestStore<LocalManifestPersistence>,
}

impl LocalManifestStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            inner: PersistedManifestStore::new(LocalManifestPersistence {
                files: LocalFiles { root: root.into() },
            }),
        }
    }
}

#[async_trait]
impl ManifestStore for LocalManifestStore {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        self.inner.manifest(object).await
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.inner.claim(object, expected, host).await
    }

    async fn claim_in_home_region(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
        home_region: &str,
    ) -> Result<OwnershipClaimResult> {
        self.inner
            .claim_in_home_region(object, expected, host, home_region)
            .await
    }

    async fn advance(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        next: &ActorManifest,
    ) -> Result<Option<VersionedActorManifest>> {
        self.inner.advance(object, current, next).await
    }
}

pub struct LocalCommitStore {
    inner: PersistedCommitStore<LocalImmutablePersistence>,
}

pub struct LocalArchiveStore {
    persistence: LocalImmutablePersistence,
}

impl LocalArchiveStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            persistence: LocalImmutablePersistence {
                files: LocalFiles { root: root.into() },
            },
        }
    }
}

#[async_trait]
impl ArchiveStore for LocalArchiveStore {
    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.persistence.put_immutable(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.persistence.load(key).await
    }
}

impl LocalCommitStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            inner: PersistedCommitStore::new(LocalImmutablePersistence {
                files: LocalFiles { root: root.into() },
            }),
        }
    }
}

#[async_trait]
impl CommitStore for LocalCommitStore {
    async fn put_immutable(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
        bytes: &[u8],
    ) -> Result<()> {
        self.inner.put_immutable(object, position, bytes).await
    }

    async fn get(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
    ) -> Result<Option<Vec<u8>>> {
        self.inner.get(object, position).await
    }
}

pub struct LocalActorStore {
    inner: RegionalActorStore,
}

impl LocalActorStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let manifests = Arc::new(LocalManifestStore::new(root.clone()));
        let commits = Arc::new(LocalCommitStore::new(root.clone()));
        let archive = Arc::new(LocalArchiveStore::new(root.clone()));
        Self {
            inner: RegionalActorStore::with_archive(manifests, commits, archive),
        }
    }

    #[cfg(test)]
    pub(crate) async fn canonical_segments(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<Vec<LtxSegment>> {
        self.inner.canonical_segments(object, manifest).await
    }
}

#[async_trait]
impl ActorDurabilityStore for LocalActorStore {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        self.inner.manifest(object).await
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.inner.claim(object, expected, host).await
    }

    async fn claim_in_home_region(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
        home_region: &str,
    ) -> Result<OwnershipClaimResult> {
        self.inner
            .claim_in_home_region(object, expected, host, home_region)
            .await
    }

    async fn publish(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<VersionedActorManifest> {
        self.inner.publish(object, current, captured).await
    }

    async fn recovery(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<super::RecoveryData> {
        self.inner.recovery(object, manifest).await
    }
}
