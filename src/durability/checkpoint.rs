//! Asynchronous creation of consolidated SQLite recovery images.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, ensure};

use super::{
    ActorDurabilityStore, RecoveryData, RegionalActorStore, restore::restore_sqlite_from_recovery,
};
use crate::actor_state::ActorStorageKey;

/// Builds checkpoints off the request path and publishes them with one manifest CAS.
pub(crate) struct CheckpointCompactor {
    store: Arc<RegionalActorStore>,
    minimum_tail_txids: u64,
}

impl CheckpointCompactor {
    pub(crate) fn new(store: Arc<RegionalActorStore>, minimum_tail_txids: u64) -> Self {
        Self {
            store,
            minimum_tail_txids: minimum_tail_txids.max(1),
        }
    }

    /// Returns the manifest carrying the installed checkpoint. A missing object, empty
    /// history, current checkpoint, short tail, or losing CAS all leave no work installed.
    pub(crate) async fn compact(
        &self,
        object: &ActorStorageKey,
    ) -> Result<Option<super::VersionedActorManifest>> {
        let Some(current) = self.store.manifest(object).await? else {
            return Ok(None);
        };
        let Some(tip) = current.manifest.tip.as_ref() else {
            return Ok(None);
        };
        let checkpoint_txid = current
            .manifest
            .checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.through.max_txid);
        if checkpoint_txid == tip.max_txid {
            return Ok(None);
        }
        let tail_txids = tip.max_txid - checkpoint_txid;
        if tail_txids < self.minimum_tail_txids {
            return Ok(None);
        }

        let recovery = self.store.recovery(object, &current.manifest).await?;
        let snapshot = tokio::task::spawn_blocking(move || build_checkpoint(recovery))
            .await
            .context("checkpoint builder task stopped")??;
        self.store
            .install_checkpoint(
                object,
                &current,
                &snapshot.bytes,
                snapshot.page_size,
                snapshot.post_apply_checksum,
            )
            .await
    }
}

struct BuiltCheckpoint {
    bytes: Vec<u8>,
    page_size: u32,
    post_apply_checksum: u64,
}

fn build_checkpoint(recovery: RecoveryData) -> Result<BuiltCheckpoint> {
    let (page_size, post_apply_checksum, max_txid) = recovery
        .segments
        .last()
        .map(|segment| {
            (
                segment.page_size,
                segment.post_apply_checksum,
                segment.max_txid,
            )
        })
        .or_else(|| {
            recovery.checkpoint.as_ref().map(|checkpoint| {
                (
                    checkpoint.metadata.page_size,
                    checkpoint.metadata.post_apply_checksum,
                    checkpoint.metadata.through.max_txid,
                )
            })
        })
        .context("cannot build a checkpoint without durable state")?;

    let temporary = tempfile::TempDir::new().context("create checkpoint staging directory")?;
    let db_path: PathBuf = temporary.path().join("object").join("db.sqlite");
    restore_sqlite_from_recovery(&db_path, &recovery)?;
    let bytes = std::fs::read(&db_path).context("read consolidated SQLite checkpoint")?;
    ensure!(!bytes.is_empty(), "consolidated SQLite checkpoint is empty");
    ensure!(
        u64::try_from(bytes.len())? % u64::from(page_size) == 0,
        "consolidated SQLite checkpoint is not page aligned at TXID {max_txid}"
    );
    Ok(BuiltCheckpoint {
        bytes,
        page_size,
        post_apply_checksum,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        actor_state::{ActorDatabaseStore, ActorDatabaseTestExt},
        durability::{
            ActorChangeCapture, ActorStateRestorer, LocalActorChangeCapture, LocalArchiveStore,
            LocalCommitStore, LocalManifestStore, LtxActorStateRestorer, OwnershipClaimResult,
        },
        host::HostId,
    };

    #[tokio::test]
    async fn installs_checkpoint_and_recovers_only_the_tail() -> Result<()> {
        let dir = tempfile::TempDir::new()?;
        let durable = dir.path().join("durable");
        let store = Arc::new(RegionalActorStore::with_archive(
            Arc::new(LocalManifestStore::new(&durable)),
            Arc::new(LocalCommitStore::new(&durable)),
            Arc::new(LocalArchiveStore::new(&durable)),
        ));
        let object = ActorStorageKey::new("checkpoint-object");
        let current = match store.claim(&object, None, &HostId::new("node-a")).await? {
            OwnershipClaimResult::Acquired(manifest) => manifest,
            result => panic!("claim failed: {result:?}"),
        };
        let local = dir.path().join("node-a");
        let capture = LocalActorChangeCapture::new(&local);
        capture.prepare(&object).await?;
        let databases = ActorDatabaseStore::new(&local);
        let database = databases.open(&object)?;
        database.set("first", &"one")?;
        database.set("second", &"two")?;
        store
            .publish(&object, &current, &capture.capture(&object).await?)
            .await?;

        let checkpointed = CheckpointCompactor::new(store.clone(), 1)
            .compact(&object)
            .await?
            .context("checkpoint was not installed")?;
        assert_eq!(
            checkpointed
                .manifest
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.through.max_txid),
            Some(2)
        );
        let recovery = store.recovery(&object, &checkpointed.manifest).await?;
        assert!(recovery.checkpoint.is_some());
        assert!(recovery.segments.is_empty());

        database.set("third", &"three")?;
        let published = store
            .publish(&object, &checkpointed, &capture.capture(&object).await?)
            .await?;
        let recovery = store.recovery(&object, &published.manifest).await?;
        assert_eq!(recovery.segments.len(), 1);
        assert_eq!(recovery.segments[0].min_txid, 3);

        let restored_root = dir.path().join("restored");
        let restored_databases = Arc::new(ActorDatabaseStore::new(&restored_root));
        LtxActorStateRestorer::new(store, restored_databases.clone())
            .restore(&object, &published.manifest)
            .await?;
        let restored = restored_databases.open(&object)?;
        assert_eq!(restored.get::<String>("first")?, Some("one".into()));
        assert_eq!(restored.get::<String>("third")?, Some("three".into()));
        Ok(())
    }
}
