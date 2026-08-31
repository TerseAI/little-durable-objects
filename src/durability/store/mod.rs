mod archive;
mod local;
mod postgres;
mod rapid;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::debug;

use super::CapturedActorChanges;
use crate::{
    actor_state::{ActorOwner, ActorStorageKey},
    host::HostId,
    ltx::LtxSegment,
};

pub use self::{
    archive::GcsArchiveStore,
    local::{LocalActorStore, LocalArchiveStore, LocalCommitStore, LocalManifestStore},
    postgres::PostgresManifestStore,
    rapid::{FinalizedCommitLog, RapidCommitStore},
};

const MANIFEST_FORMAT_VERSION: u32 = 2;
const COMMIT_FORMAT_VERSION: u32 = 2;
const COMMIT_MAGIC: &[u8; 8] = b"TDOLTX02";
const MAX_COMMIT_HEADER_BYTES: usize = 1024 * 1024;
/// Deterministic Rapid rotation boundary. The commit position records the selected
/// generation, so a bundle that spans a boundary remains addressable as one record.
const COMMIT_LOG_TXIDS: u64 = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPosition {
    pub epoch: u64,
    pub log_generation: u64,
    pub max_txid: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CommitLogId {
    pub epoch: u64,
    pub generation: u64,
}

impl From<&CommitPosition> for CommitLogId {
    fn from(position: &CommitPosition) -> Self {
        Self {
            epoch: position.epoch,
            generation: position.log_generation,
        }
    }
}

/// An immutable, multi-region SQLite recovery image installed through the manifest CAS.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// The exact canonical commit represented by the SQLite image.
    pub through: CommitPosition,
    /// Immutable object name in the checkpoint store.
    pub object_key: String,
    pub byte_len: u64,
    pub crc32c: u32,
    pub page_size: u32,
    /// LTX's running checksum at `through`.
    pub post_apply_checksum: u64,
}

impl CheckpointMetadata {
    fn validate(&self) -> Result<()> {
        ensure!(self.through.epoch > 0, "checkpoint epoch must be positive");
        ensure!(
            self.through.max_txid > 0,
            "checkpoint TXID must be positive"
        );
        ensure!(
            !self.object_key.is_empty(),
            "checkpoint object key is empty"
        );
        ensure!(
            self.byte_len > 0,
            "checkpoint must contain a SQLite database"
        );
        ensure!(
            self.page_size.is_power_of_two() && (512..=65_536).contains(&self.page_size),
            "invalid checkpoint SQLite page size {}",
            self.page_size
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorManifest {
    pub format_version: u32,
    #[serde(default = "default_home_region", rename = "storage_region")]
    pub home_region: String,
    pub owner: ActorOwner,
    pub tip: Option<CommitPosition>,
    pub checkpoint: Option<CheckpointMetadata>,
    /// Highest TXID copied into multi-region archive storage.
    pub archived_txid: u64,
    /// Highest TXID whose obsolete Rapid logs have actually been deleted.
    pub rapid_gc_txid: u64,
}

impl ActorManifest {
    fn initial(host: HostId, home_region: String) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            home_region,
            owner: ActorOwner { host, epoch: 1 },
            tip: None,
            checkpoint: None,
            archived_txid: 0,
            rapid_gc_txid: 0,
        }
    }

    pub fn max_txid(&self) -> u64 {
        self.tip.as_ref().map_or(0, |tip| tip.max_txid)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == MANIFEST_FORMAT_VERSION,
            "unsupported object manifest format version {}",
            self.format_version
        );
        ensure!(self.owner.epoch > 0, "object owner epoch must be positive");
        ensure!(
            valid_home_region(&self.home_region),
            "object storage region is invalid"
        );
        if let Some(tip) = &self.tip {
            ensure!(tip.epoch > 0, "commit epoch must be positive");
            ensure!(tip.max_txid > 0, "commit TXID must be positive");
            ensure!(
                tip.epoch <= self.owner.epoch,
                "commit epoch {} is newer than owner epoch {}",
                tip.epoch,
                self.owner.epoch
            );
        }
        let max_txid = self.max_txid();
        ensure!(
            self.archived_txid <= max_txid,
            "archived TXID {} exceeds manifest tip {max_txid}",
            self.archived_txid
        );
        ensure!(
            self.rapid_gc_txid <= max_txid,
            "Rapid GC TXID {} exceeds manifest tip {max_txid}",
            self.rapid_gc_txid
        );
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
            ensure!(
                checkpoint.through.max_txid <= max_txid,
                "checkpoint TXID {} exceeds manifest tip {max_txid}",
                checkpoint.through.max_txid
            );
        }
        let recoverable_through = self
            .checkpoint
            .as_ref()
            .map_or(self.archived_txid, |checkpoint| {
                self.archived_txid.max(checkpoint.through.max_txid)
            });
        ensure!(
            self.rapid_gc_txid <= recoverable_through,
            "Rapid GC TXID {} exceeds Standard recovery watermark {recoverable_through}",
            self.rapid_gc_txid
        );
        Ok(())
    }
}

fn default_home_region() -> String {
    "default".into()
}

fn valid_home_region(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestVersion(Vec<u8>);

impl ManifestVersion {
    fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for ManifestVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ManifestVersion")
            .field(&String::from_utf8_lossy(&self.0))
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedActorManifest {
    pub manifest: ActorManifest,
    version: ManifestVersion,
}

impl VersionedActorManifest {
    pub fn owner(&self) -> &ActorOwner {
        &self.manifest.owner
    }

    pub fn max_txid(&self) -> u64 {
        self.manifest.max_txid()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnershipClaimResult {
    Acquired(VersionedActorManifest),
    Conflict(Option<VersionedActorManifest>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatePublicationStatus {
    Published(VersionedActorManifest),
    Unchanged,
    Conflict(Option<VersionedActorManifest>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredSegment {
    min_txid: u64,
    max_txid: u64,
    byte_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CommitHeader {
    format_version: u32,
    position: CommitPosition,
    parent: Option<CommitPosition>,
    segments: Vec<StoredSegment>,
}

struct VersionedBlob {
    bytes: Vec<u8>,
    version: ManifestVersion,
}

#[async_trait]
trait ManifestPersistence: Send + Sync {
    async fn load(&self, key: &str) -> Result<Option<VersionedBlob>>;

    async fn write_if_version(
        &self,
        key: &str,
        expected: Option<&ManifestVersion>,
        bytes: &[u8],
    ) -> Result<Option<ManifestVersion>>;
}

#[async_trait]
trait ImmutablePersistence: Send + Sync {
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>>;

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()>;
}

struct PersistedManifestStore<P> {
    persistence: P,
}

impl<P> PersistedManifestStore<P>
where
    P: ManifestPersistence,
{
    fn new(persistence: P) -> Self {
        Self { persistence }
    }

    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        let Some(blob) = self.persistence.load(&manifest_key(object)).await? else {
            return Ok(None);
        };
        let manifest = decode_json::<ActorManifest>(&blob.bytes)?;
        manifest.validate()?;
        Ok(Some(VersionedActorManifest {
            manifest,
            version: blob.version,
        }))
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.claim_in_home_region(object, expected, host, "default")
            .await
    }

    async fn claim_in_home_region(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
        home_region: &str,
    ) -> Result<OwnershipClaimResult> {
        ensure!(
            valid_home_region(home_region),
            "object storage region is invalid"
        );
        let next = match expected {
            Some(current) => {
                current.manifest.validate()?;
                ensure!(
                    current.manifest.home_region == home_region,
                    "object home region cannot change from {} to {home_region}",
                    current.manifest.home_region
                );
                ActorManifest {
                    format_version: MANIFEST_FORMAT_VERSION,
                    home_region: current.manifest.home_region.clone(),
                    owner: ActorOwner {
                        host: host.clone(),
                        epoch: current
                            .manifest
                            .owner
                            .epoch
                            .checked_add(1)
                            .context("object ownership epoch overflow")?,
                    },
                    tip: current.manifest.tip.clone(),
                    checkpoint: current.manifest.checkpoint.clone(),
                    archived_txid: current.manifest.archived_txid,
                    rapid_gc_txid: current.manifest.rapid_gc_txid,
                }
            }
            None => ActorManifest::initial(host.clone(), home_region.to_owned()),
        };

        debug!(
            object = %object,
            owner_node = %next.owner.host,
            owner_epoch = next.owner.epoch,
            max_txid = next.max_txid(),
            "conditionally claiming object manifest"
        );
        let version = self
            .persistence
            .write_if_version(
                &manifest_key(object),
                expected.map(|current| &current.version),
                &serde_json::to_vec(&next)?,
            )
            .await?;

        if let Some(version) = version {
            return Ok(OwnershipClaimResult::Acquired(VersionedActorManifest {
                manifest: next,
                version,
            }));
        }

        Ok(OwnershipClaimResult::Conflict(self.manifest(object).await?))
    }

    async fn advance(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        next: &ActorManifest,
    ) -> Result<Option<VersionedActorManifest>> {
        current.manifest.validate()?;
        next.validate()?;
        ensure!(
            current.manifest.owner == next.owner,
            "manifest advance cannot change the object owner"
        );
        let version = self
            .persistence
            .write_if_version(
                &manifest_key(object),
                Some(&current.version),
                &serde_json::to_vec(next)?,
            )
            .await?;
        Ok(version.map(|version| VersionedActorManifest {
            manifest: next.clone(),
            version,
        }))
    }
}

#[async_trait]
impl<P> ManifestStore for PersistedManifestStore<P>
where
    P: ManifestPersistence,
{
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        self.manifest(object).await
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.claim(object, expected, host).await
    }

    async fn claim_in_home_region(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
        home_region: &str,
    ) -> Result<OwnershipClaimResult> {
        self.claim_in_home_region(object, expected, host, home_region)
            .await
    }

    async fn advance(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        next: &ActorManifest,
    ) -> Result<Option<VersionedActorManifest>> {
        self.advance(object, current, next).await
    }
}

#[async_trait]
pub trait ManifestStore: Send + Sync {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>>;

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
    ) -> Result<OwnershipClaimResult>;

    async fn claim_in_home_region(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
        home_region: &str,
    ) -> Result<OwnershipClaimResult> {
        ensure!(
            home_region == "default",
            "this manifest store does not support home-region placement"
        );
        self.claim(object, expected, host).await
    }

    async fn advance(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        next: &ActorManifest,
    ) -> Result<Option<VersionedActorManifest>>;

    /// Advance only the canonical commit tip. PostgreSQL overrides this so unrelated
    /// asynchronous checkpoint/watermark updates do not make a warm writer fail its
    /// durability CAS. The expected owner and tip still fence takeovers and competing
    /// writers.
    async fn advance_tip(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        next: &ActorManifest,
    ) -> Result<Option<VersionedActorManifest>> {
        self.advance(object, current, next).await
    }

    /// Actors with asynchronous durability or cleanup work. Stores without a queryable
    /// index (notably the local test store) may return an empty list.
    async fn maintenance_candidates(
        &self,
        _minimum_checkpoint_tail: u64,
        _limit: usize,
    ) -> Result<Vec<ActorStorageKey>> {
        Ok(Vec::new())
    }
}

#[async_trait]
pub trait CommitStore: Send + Sync {
    async fn put_immutable(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
        bytes: &[u8],
    ) -> Result<()>;

    async fn get(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
    ) -> Result<Option<Vec<u8>>>;
}

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()>;

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Whether an immutable artifact has aged past the configured window for
    /// asynchronous cross-region replication. Local stores have no asynchronous
    /// replication phase and can use the default.
    async fn replication_grace_elapsed(&self, _key: &str, _minimum_age: Duration) -> Result<bool> {
        Ok(true)
    }
}

const REGIONAL_LOG_CACHE_ENTRIES: usize = 32;

#[derive(Default)]
struct RegionalLogCache {
    entries: HashMap<String, Arc<Vec<u8>>>,
    order: VecDeque<String>,
}

impl RegionalLogCache {
    fn get(&mut self, key: &str) -> Option<Arc<Vec<u8>>> {
        let value = self.entries.get(key)?.clone();
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.to_owned());
        Some(value)
    }

    fn insert(&mut self, key: String, bytes: Arc<Vec<u8>>) {
        self.order.retain(|candidate| candidate != &key);
        self.entries.insert(key.clone(), bytes);
        self.order.push_back(key);
        while self.order.len() > REGIONAL_LOG_CACHE_ENTRIES {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

/// Writes synchronously to Rapid and falls back to the Standard archive during
/// recovery after old Rapid generations have been collected.
pub struct TieredCommitStore {
    hot: Arc<dyn CommitStore>,
    archive: Arc<dyn ArchiveStore>,
    archive_cache: tokio::sync::Mutex<RegionalLogCache>,
}

impl TieredCommitStore {
    pub fn new(hot: Arc<dyn CommitStore>, archive: Arc<dyn ArchiveStore>) -> Self {
        Self {
            hot,
            archive,
            archive_cache: tokio::sync::Mutex::new(RegionalLogCache::default()),
        }
    }

    async fn get_archived(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
    ) -> Result<Option<Vec<u8>>> {
        let key = archive_log_key(object, &CommitLogId::from(position));
        let cached = self.archive_cache.lock().await.get(&key);
        let bytes = match cached {
            Some(bytes) => bytes,
            None => {
                let Some(bytes) = self.archive.get(&key).await? else {
                    return Ok(None);
                };
                let bytes = Arc::new(bytes);
                self.archive_cache
                    .lock()
                    .await
                    .insert(key.clone(), bytes.clone());
                bytes
            }
        };
        rapid::decode_commit_from_log(bytes.as_slice(), position, &key)
    }
}

#[async_trait]
impl CommitStore for TieredCommitStore {
    async fn put_immutable(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
        bytes: &[u8],
    ) -> Result<()> {
        self.hot.put_immutable(object, position, bytes).await
    }

    async fn get(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
    ) -> Result<Option<Vec<u8>>> {
        match self.hot.get(object, position).await? {
            Some(bytes) => Ok(Some(bytes)),
            None => self.get_archived(object, position).await,
        }
    }
}

/// Canonical data needed to rebuild a local actor cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryData {
    pub checkpoint: Option<RecoveredCheckpoint>,
    pub segments: Vec<LtxSegment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveredCheckpoint {
    pub metadata: CheckpointMetadata,
    pub bytes: Vec<u8>,
}

struct PersistedCommitStore<P> {
    persistence: P,
}

impl<P> PersistedCommitStore<P>
where
    P: ImmutablePersistence,
{
    fn new(persistence: P) -> Self {
        Self { persistence }
    }
}

#[async_trait]
impl<P> CommitStore for PersistedCommitStore<P>
where
    P: ImmutablePersistence,
{
    async fn put_immutable(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
        bytes: &[u8],
    ) -> Result<()> {
        self.persistence
            .put_immutable(&commit_key(object, position), bytes)
            .await
    }

    async fn get(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
    ) -> Result<Option<Vec<u8>>> {
        self.persistence.load(&commit_key(object, position)).await
    }
}

pub struct RegionalActorStore {
    manifests: Arc<dyn ManifestStore>,
    commits: HashMap<String, Arc<dyn CommitStore>>,
    archives: HashMap<String, Arc<dyn ArchiveStore>>,
}

impl RegionalActorStore {
    pub fn new(manifests: Arc<dyn ManifestStore>, commits: Arc<dyn CommitStore>) -> Self {
        Self {
            manifests,
            commits: HashMap::from([("default".into(), commits)]),
            archives: HashMap::new(),
        }
    }

    pub fn with_archive(
        manifests: Arc<dyn ManifestStore>,
        commits: Arc<dyn CommitStore>,
        archive: Arc<dyn ArchiveStore>,
    ) -> Self {
        Self {
            manifests,
            commits: HashMap::from([("default".into(), commits)]),
            archives: HashMap::from([("default".into(), archive)]),
        }
    }

    pub fn with_region_stores(
        manifests: Arc<dyn ManifestStore>,
        commits: HashMap<String, Arc<dyn CommitStore>>,
        archives: HashMap<String, Arc<dyn ArchiveStore>>,
    ) -> Result<Self> {
        ensure!(
            !commits.is_empty(),
            "at least one region-specific commit store is required"
        );
        ensure!(
            commits.keys().all(|region| valid_home_region(region)),
            "commit store has an invalid home region"
        );
        ensure!(
            commits.len() == archives.len()
                && commits.keys().all(|region| archives.contains_key(region)),
            "Rapid and Standard bucket maps must contain the same actor regions"
        );
        Ok(Self {
            manifests,
            commits,
            archives,
        })
    }

    fn commits_for(&self, home_region: &str) -> Result<&dyn CommitStore> {
        self.commits
            .get(home_region)
            .or_else(|| {
                (self.commits.len() == 1)
                    .then(|| self.commits.values().next())
                    .flatten()
            })
            .map(Arc::as_ref)
            .with_context(|| {
                format!("no commit store is configured for actor region {home_region:?}")
            })
    }

    fn archive_for(&self, home_region: &str) -> Result<&dyn ArchiveStore> {
        self.archives
            .get(home_region)
            .or_else(|| {
                (self.archives.len() == 1)
                    .then(|| self.archives.values().next())
                    .flatten()
            })
            .map(Arc::as_ref)
            .with_context(|| {
                format!(
                    "no Standard multi-region bucket is configured for actor region {home_region:?}"
                )
            })
    }

    /// Persist an immutable SQLite image before making it visible through one CAS.
    /// A CAS conflict leaves a harmless unreferenced object for lifecycle cleanup.
    pub async fn install_checkpoint(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        bytes: &[u8],
        page_size: u32,
        post_apply_checksum: u64,
    ) -> Result<Option<VersionedActorManifest>> {
        current.manifest.validate()?;
        let through = current
            .manifest
            .tip
            .clone()
            .context("cannot checkpoint an object with no durable commits")?;
        let archives = self.archive_for(&current.manifest.home_region)?;
        let byte_len = u64::try_from(bytes.len()).context("checkpoint is too large")?;
        let crc32c = crc32c::crc32c(bytes);
        let metadata = CheckpointMetadata {
            object_key: checkpoint_key(object, &through, crc32c),
            through,
            byte_len,
            crc32c,
            page_size,
            post_apply_checksum,
        };
        metadata.validate()?;
        archives.put_immutable(&metadata.object_key, bytes).await?;

        let mut candidate = current.clone();
        for _ in 0..8 {
            if candidate
                .manifest
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.through.max_txid >= metadata.through.max_txid)
            {
                return Ok(Some(candidate));
            }
            ensure!(
                candidate.max_txid() >= metadata.through.max_txid,
                "manifest tip moved behind checkpoint while installing it for {object}"
            );
            let mut next = candidate.manifest.clone();
            next.checkpoint = Some(metadata.clone());
            if let Some(updated) = self.manifests.advance(object, &candidate, &next).await? {
                return Ok(Some(updated));
            }
            candidate = self
                .manifests
                .manifest(object)
                .await?
                .with_context(|| format!("object manifest disappeared for {object}"))?;
        }
        Ok(None)
    }

    /// Full canonical LTX history for storage verification and maintenance tools.
    pub async fn canonical_segments(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<Vec<LtxSegment>> {
        canonical_segments(self.commits_for(&manifest.home_region)?, object, manifest).await
    }

    /// Canonical commit positions after `after_txid`, from oldest to newest. Used off the
    /// request path to find bounded Rapid generations that are closed and safe to archive.
    pub async fn canonical_positions_after(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
        after_txid: u64,
    ) -> Result<Vec<CommitPosition>> {
        manifest.validate()?;
        let mut positions = Vec::new();
        let mut cursor = manifest.tip.clone();
        let mut visited = HashSet::new();
        while let Some(position) = cursor {
            if position.max_txid <= after_txid {
                break;
            }
            ensure!(
                visited.insert((position.epoch, position.log_generation, position.max_txid)),
                "cycle in durable LTX history for {object} at epoch {} TXID {}",
                position.epoch,
                position.max_txid
            );
            let key = commit_key(object, &position);
            let bytes = self
                .commits_for(&manifest.home_region)?
                .get(object, &position)
                .await?
                .with_context(|| format!("missing durable LTX commit bundle {key}"))?;
            let (header, _) = decode_commit_bundle(&bytes, &key)?;
            ensure!(
                header.position == position,
                "durable LTX commit identity mismatch for {object}"
            );
            cursor = header.parent;
            positions.push(position);
        }
        positions.reverse();
        Ok(positions)
    }

    /// Advance asynchronous durability/cleanup watermarks without changing ownership
    /// or the canonical commit tip. Conflicts are retried against the newest manifest.
    pub async fn advance_watermarks(
        &self,
        object: &ActorStorageKey,
        archived_txid: u64,
        rapid_gc_txid: u64,
    ) -> Result<VersionedActorManifest> {
        for _ in 0..8 {
            let current = self
                .manifests
                .manifest(object)
                .await?
                .with_context(|| format!("object manifest disappeared for {object}"))?;
            let mut next = current.manifest.clone();
            next.archived_txid = next.archived_txid.max(archived_txid);
            next.rapid_gc_txid = next.rapid_gc_txid.max(rapid_gc_txid);
            next.validate()?;
            if next == current.manifest {
                return Ok(current);
            }
            if let Some(updated) = self.manifests.advance(object, &current, &next).await? {
                return Ok(updated);
            }
        }
        bail!("object manifest kept changing while advancing durability watermarks for {object}")
    }

    pub async fn maintenance_candidates(
        &self,
        minimum_checkpoint_tail: u64,
        limit: usize,
    ) -> Result<Vec<ActorStorageKey>> {
        self.manifests
            .maintenance_candidates(minimum_checkpoint_tail, limit)
            .await
    }
}

#[async_trait]
impl ActorDurabilityStore for RegionalActorStore {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        self.manifests.manifest(object).await
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
    ) -> Result<OwnershipClaimResult> {
        self.manifests.claim(object, expected, host).await
    }

    async fn claim_in_home_region(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
        home_region: &str,
    ) -> Result<OwnershipClaimResult> {
        self.manifests
            .claim_in_home_region(object, expected, host, home_region)
            .await
    }

    async fn publish(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<VersionedActorManifest> {
        let Some(prepared) = prepare_commit(object, current, captured)? else {
            return Ok(current.clone());
        };

        self.commits_for(&current.manifest.home_region)?
            .put_immutable(object, &prepared.position, &prepared.bundle)
            .await?;
        let advanced = self
            .manifests
            .advance_tip(object, current, &prepared.manifest)
            .await;
        let next = match advanced {
            Ok(Some(next)) => next,
            Ok(None) => match publication_status(
                current,
                &prepared.manifest,
                self.manifests.manifest(object).await?,
            ) {
                StatePublicationStatus::Published(next) => next,
                StatePublicationStatus::Unchanged | StatePublicationStatus::Conflict(_) => {
                    bail!("actor manifest changed while publishing {object}")
                }
            },
            Err(error) => match self.manifests.manifest(object).await {
                Ok(observed) => match publication_status(current, &prepared.manifest, observed) {
                    StatePublicationStatus::Published(next) => next,
                    StatePublicationStatus::Unchanged | StatePublicationStatus::Conflict(_) => {
                        return Err(error);
                    }
                },
                Err(reconciliation_error) => {
                    return Err(error.context(format!(
                        "could not reconcile failed publication for {object}: {reconciliation_error:#}"
                    )));
                }
            },
        };

        debug!(
            object = %object,
            owner_epoch = next.owner().epoch,
            max_txid = next.max_txid(),
            "advanced durable object manifest"
        );
        Ok(next)
    }

    async fn recovery(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<RecoveryData> {
        manifest.validate()?;
        let checkpoint = match &manifest.checkpoint {
            Some(metadata) => {
                let store = self.archive_for(&manifest.home_region).context(
                    "manifest references a checkpoint but no Standard multi-region store is configured",
                )?;
                let bytes = store
                    .get(&metadata.object_key)
                    .await?
                    .with_context(|| format!("missing checkpoint {}", metadata.object_key))?;
                ensure!(
                    u64::try_from(bytes.len()).ok() == Some(metadata.byte_len),
                    "checkpoint byte length mismatch for {}",
                    metadata.object_key
                );
                ensure!(
                    crc32c::crc32c(&bytes) == metadata.crc32c,
                    "checkpoint checksum mismatch for {}",
                    metadata.object_key
                );
                Some(RecoveredCheckpoint {
                    metadata: metadata.clone(),
                    bytes,
                })
            }
            None => None,
        };
        let segments = canonical_segments_after(
            self.commits_for(&manifest.home_region)?,
            object,
            manifest,
            manifest.checkpoint.as_ref(),
        )
        .await?;
        Ok(RecoveryData {
            checkpoint,
            segments,
        })
    }
}

#[async_trait]
pub trait ActorDurabilityStore: Send + Sync {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>>;

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
    ) -> Result<OwnershipClaimResult>;

    async fn claim_in_home_region(
        &self,
        object: &ActorStorageKey,
        expected: Option<&VersionedActorManifest>,
        host: &HostId,
        home_region: &str,
    ) -> Result<OwnershipClaimResult> {
        ensure!(
            home_region == "default",
            "this actor durability store does not support home-region placement"
        );
        self.claim(object, expected, host).await
    }

    async fn publish(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<VersionedActorManifest>;

    /// Determine whether a publish whose response was lost advanced the canonical
    /// commit tip. This is read-only and safe to retry after transport cancellation.
    async fn publication_status(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        captured: &CapturedActorChanges,
    ) -> Result<StatePublicationStatus> {
        let Some(prepared) = prepare_commit(object, current, captured)? else {
            return Ok(StatePublicationStatus::Published(current.clone()));
        };
        Ok(publication_status(
            current,
            &prepared.manifest,
            self.manifest(object).await?,
        ))
    }

    async fn recovery(
        &self,
        object: &ActorStorageKey,
        manifest: &ActorManifest,
    ) -> Result<RecoveryData>;
}

struct PreparedCommit {
    position: CommitPosition,
    manifest: ActorManifest,
    bundle: Vec<u8>,
}

fn publication_status(
    current: &VersionedActorManifest,
    expected: &ActorManifest,
    observed: Option<VersionedActorManifest>,
) -> StatePublicationStatus {
    let Some(observed) = observed else {
        return StatePublicationStatus::Conflict(None);
    };
    if observed.manifest.tip == expected.tip {
        return StatePublicationStatus::Published(observed);
    }
    if observed.manifest.owner == current.manifest.owner
        && observed.manifest.tip == current.manifest.tip
    {
        return StatePublicationStatus::Unchanged;
    }
    StatePublicationStatus::Conflict(Some(observed))
}

fn prepare_commit(
    object: &ActorStorageKey,
    current: &VersionedActorManifest,
    captured: &CapturedActorChanges,
) -> Result<Option<PreparedCommit>> {
    current.manifest.validate()?;
    debug!(
        object = %object,
        owner_node = %current.manifest.owner.host,
        owner_epoch = current.manifest.owner.epoch,
        captured_segments = captured.len(),
        "publishing captured LTX commit bundle"
    );

    if captured.is_empty() {
        return Ok(None);
    }

    let durable_txid = current.max_txid();
    let mut next_txid = durable_txid
        .checked_add(1)
        .context("durable LTX TXID overflow")?;
    let mut segments = Vec::new();

    for segment in captured.segments() {
        if segment.max_txid <= durable_txid {
            continue;
        }
        ensure!(
            segment.min_txid == next_txid,
            "non-contiguous LTX publish for {object}: expected TXID {next_txid}, found {}",
            segment.min_txid
        );
        ensure!(
            segment.max_txid >= segment.min_txid,
            "invalid LTX range {}-{} for {object}",
            segment.min_txid,
            segment.max_txid
        );
        segments.push(segment);
        next_txid = segment
            .max_txid
            .checked_add(1)
            .context("durable LTX TXID overflow")?;
    }

    if segments.is_empty() {
        return Ok(None);
    }

    let position = CommitPosition {
        epoch: current.manifest.owner.epoch,
        log_generation: durable_txid / COMMIT_LOG_TXIDS,
        max_txid: next_txid - 1,
    };
    let header = CommitHeader {
        format_version: COMMIT_FORMAT_VERSION,
        position: position.clone(),
        parent: current.manifest.tip.clone(),
        segments: segments
            .iter()
            .map(|segment| {
                Ok(StoredSegment {
                    min_txid: segment.min_txid,
                    max_txid: segment.max_txid,
                    byte_len: u64::try_from(segment.bytes.len())
                        .context("LTX segment is too large")?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let bundle = encode_commit_bundle(&header, &segments)?;
    Ok(Some(PreparedCommit {
        position: position.clone(),
        manifest: ActorManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            home_region: current.manifest.home_region.clone(),
            owner: current.manifest.owner.clone(),
            tip: Some(position),
            checkpoint: current.manifest.checkpoint.clone(),
            archived_txid: current.manifest.archived_txid,
            rapid_gc_txid: current.manifest.rapid_gc_txid,
        },
        bundle,
    }))
}

async fn canonical_segments(
    commits: &dyn CommitStore,
    object: &ActorStorageKey,
    manifest: &ActorManifest,
) -> Result<Vec<LtxSegment>> {
    canonical_segments_after(commits, object, manifest, None).await
}

async fn canonical_segments_after(
    commits: &dyn CommitStore,
    object: &ActorStorageKey,
    manifest: &ActorManifest,
    checkpoint: Option<&CheckpointMetadata>,
) -> Result<Vec<LtxSegment>> {
    manifest.validate()?;
    debug!(
        object = %object,
        owner_epoch = manifest.owner.epoch,
        max_txid = manifest.max_txid(),
        "loading canonical LTX commit bundles"
    );
    let segments = load_commit_chain(commits, object, manifest.tip.clone(), checkpoint).await?;
    validate_segment_chain(object, &segments, checkpoint, manifest.max_txid())?;

    debug!(
        object = %object,
        segment_count = segments.len(),
        max_txid = manifest.max_txid(),
        "loaded canonical LTX history"
    );
    Ok(segments)
}

async fn load_commit_chain(
    commits: &dyn CommitStore,
    object: &ActorStorageKey,
    mut cursor: Option<CommitPosition>,
    checkpoint: Option<&CheckpointMetadata>,
) -> Result<Vec<LtxSegment>> {
    let mut decoded = Vec::new();
    let mut visited = HashSet::new();
    let stop = checkpoint.map(|checkpoint| &checkpoint.through);
    let mut reached_checkpoint = checkpoint.is_none();

    while let Some(position) = cursor {
        if stop == Some(&position) {
            reached_checkpoint = true;
            break;
        }
        ensure!(
            visited.insert((position.epoch, position.max_txid)),
            "cycle in durable LTX history for {object} at epoch {} TXID {}",
            position.epoch,
            position.max_txid
        );
        let key = commit_key(object, &position);
        let bytes = commits
            .get(object, &position)
            .await?
            .with_context(|| format!("missing durable LTX commit bundle {key}"))?;
        let (header, segments) = decode_commit_bundle(&bytes, &key)?;
        ensure!(
            header.position == position,
            "durable LTX commit identity mismatch for {object}"
        );
        cursor = header.parent;
        decoded.push(segments);
    }

    decoded.reverse();
    let segments = decoded.into_iter().flatten().collect::<Vec<_>>();
    ensure!(
        reached_checkpoint,
        "checkpoint is not an ancestor of the durable tip for {object}"
    );
    Ok(segments)
}

fn validate_segment_chain(
    object: &ActorStorageKey,
    segments: &[LtxSegment],
    checkpoint: Option<&CheckpointMetadata>,
    durable_txid: u64,
) -> Result<()> {
    let mut next_txid = match checkpoint {
        Some(checkpoint) => checkpoint
            .through
            .max_txid
            .checked_add(1)
            .context("durable LTX TXID overflow")?,
        None => 1,
    };
    let mut previous_checksum = checkpoint.map(|checkpoint| checkpoint.post_apply_checksum);
    for segment in segments {
        ensure!(
            segment.min_txid == next_txid,
            "gap in durable LTX history for {object}: expected TXID {next_txid}, found {}",
            segment.min_txid
        );
        ensure!(
            segment.pre_apply_checksum == previous_checksum,
            "checksum gap in durable LTX history for {object} before TXID {}",
            segment.min_txid
        );
        previous_checksum = Some(segment.post_apply_checksum);
        next_txid = segment
            .max_txid
            .checked_add(1)
            .context("durable LTX TXID overflow")?;
    }
    ensure!(
        next_txid - 1 == durable_txid,
        "durable LTX history for {object} ends at TXID {}, expected {}",
        next_txid - 1,
        durable_txid
    );
    Ok(())
}

fn encode_commit_bundle(header: &CommitHeader, segments: &[&LtxSegment]) -> Result<Vec<u8>> {
    let header_bytes = serde_json::to_vec(header)?;
    ensure!(
        header_bytes.len() <= MAX_COMMIT_HEADER_BYTES,
        "durable LTX commit header is too large"
    );
    let header_len = u32::try_from(header_bytes.len()).context("commit header is too large")?;
    let payload_len = segments.iter().try_fold(0usize, |total, segment| {
        total
            .checked_add(segment.bytes.len())
            .context("commit bundle size overflow")
    })?;
    let mut bytes = Vec::with_capacity(
        COMMIT_MAGIC
            .len()
            .checked_add(4)
            .and_then(|size| size.checked_add(header_bytes.len()))
            .and_then(|size| size.checked_add(payload_len))
            .context("commit bundle size overflow")?,
    );
    bytes.extend_from_slice(COMMIT_MAGIC);
    bytes.extend_from_slice(&header_len.to_be_bytes());
    bytes.extend_from_slice(&header_bytes);
    for segment in segments {
        bytes.extend_from_slice(&segment.bytes);
    }
    Ok(bytes)
}

fn decode_commit_bundle(bytes: &[u8], key: &str) -> Result<(CommitHeader, Vec<LtxSegment>)> {
    ensure!(
        bytes.len() >= COMMIT_MAGIC.len() + 4,
        "durable LTX commit bundle is truncated: {key}"
    );
    ensure!(
        &bytes[..COMMIT_MAGIC.len()] == COMMIT_MAGIC,
        "durable LTX commit bundle has invalid magic: {key}"
    );
    let header_len = u32::from_be_bytes(
        bytes[COMMIT_MAGIC.len()..COMMIT_MAGIC.len() + 4]
            .try_into()
            .expect("four-byte commit header length"),
    ) as usize;
    ensure!(
        header_len <= MAX_COMMIT_HEADER_BYTES,
        "durable LTX commit header is too large: {key}"
    );
    let header_start = COMMIT_MAGIC.len() + 4;
    let header_end = header_start
        .checked_add(header_len)
        .context("commit header length overflow")?;
    ensure!(
        header_end <= bytes.len(),
        "durable LTX commit header is truncated: {key}"
    );
    let header = decode_json::<CommitHeader>(&bytes[header_start..header_end])?;
    ensure!(
        header.format_version == COMMIT_FORMAT_VERSION,
        "unsupported durable LTX commit format version {}",
        header.format_version
    );

    let mut offset = header_end;
    let mut segments = Vec::with_capacity(header.segments.len());
    for stored in &header.segments {
        let byte_len = usize::try_from(stored.byte_len).context("LTX payload is too large")?;
        let end = offset
            .checked_add(byte_len)
            .context("LTX payload length overflow")?;
        ensure!(
            end <= bytes.len(),
            "durable LTX payload is truncated: {key}"
        );
        let segment = LtxSegment::decode(bytes[offset..end].to_vec())?;
        ensure!(
            segment.min_txid == stored.min_txid && segment.max_txid == stored.max_txid,
            "durable LTX segment range does not match commit metadata: {key}"
        );
        segments.push(segment);
        offset = end;
    }
    ensure!(
        offset == bytes.len(),
        "durable LTX commit bundle has trailing bytes: {key}"
    );
    Ok((header, segments))
}

fn decode_json<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    Ok(serde_json::from_slice(bytes)?)
}

const OBJECTS_PREFIX: &str = "objects";

fn object_prefix(object: &ActorStorageKey) -> String {
    format!("{OBJECTS_PREFIX}/{object}")
}

fn manifest_key(object: &ActorStorageKey) -> String {
    format!("{}/manifest.json", object_prefix(object))
}

fn commit_key(object: &ActorStorageKey, position: &CommitPosition) -> String {
    format!(
        "{}/commits/e{:016x}/{:016x}.ltxpack",
        object_prefix(object),
        position.epoch,
        position.max_txid
    )
}

fn checkpoint_key(object: &ActorStorageKey, position: &CommitPosition, crc32c: u32) -> String {
    format!(
        "{}/checkpoints/e{:016x}/{:016x}-{crc32c:08x}.sqlite",
        object_prefix(object),
        position.epoch,
        position.max_txid
    )
}

pub(crate) fn archive_log_key(object: &ActorStorageKey, id: &CommitLogId) -> String {
    format!(
        "{}/archive/e{:016x}/g{:016x}.ltxlog",
        object_prefix(object),
        id.epoch,
        id.generation
    )
}

#[cfg(test)]
mod tests;
