//! Rebuild an actor's local SQLite cache from canonical durable LTX history.

use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tracing::{debug, info};

use super::{ActorDurabilityStore, ActorManifest, RecoveredCheckpoint, RecoveryData};
use crate::{
    actor_state::{ActorDatabaseStore, ActorStorageKey},
    ltx::{LtxSegment, install_capture_base},
};

static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[async_trait]
pub trait ActorStateRestorer: Send + Sync {
    async fn restore(&self, object: &ActorStorageKey, manifest: &ActorManifest) -> Result<()>;
}

/// Downloads canonical history and replaces the object's local cache as one object
/// directory swap.
pub struct LtxActorStateRestorer {
    store: Arc<dyn ActorDurabilityStore>,
    databases: Arc<ActorDatabaseStore>,
}

impl LtxActorStateRestorer {
    pub fn new(store: Arc<dyn ActorDurabilityStore>, databases: Arc<ActorDatabaseStore>) -> Self {
        Self { store, databases }
    }
}

#[async_trait]
impl ActorStateRestorer for LtxActorStateRestorer {
    #[tracing::instrument(
        name = "ltx.restore",
        skip(self, manifest),
        fields(
            object = %object,
            owner_epoch = manifest.owner.epoch,
            max_txid = manifest.max_txid()
        )
    )]
    async fn restore(&self, object: &ActorStorageKey, manifest: &ActorManifest) -> Result<()> {
        let recovery = self.store.recovery(object, manifest).await?;
        debug!(
            checkpoint_txid = recovery
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.metadata.through.max_txid),
            tail_segments = recovery.segments.len(),
            "downloaded canonical recovery state"
        );

        restore_sqlite_from_recovery(&self.databases.path_for(object)?, &recovery)?;
        info!(
            checkpoint_txid = recovery
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.metadata.through.max_txid),
            tail_segments = recovery.segments.len(),
            "restored SQLite database from checkpoint and LTX tail"
        );

        Ok(())
    }
}

/// Rebuild `db_path` and its local LTX directory from a verified contiguous history.
/// Staging occurs beside the object directory, so a failure before publication leaves
/// the existing local cache untouched.
#[cfg(test)]
pub(crate) fn restore_sqlite_from_ltx(db_path: &Path, segments: &[LtxSegment]) -> Result<()> {
    restore_sqlite_from_recovery(
        db_path,
        &RecoveryData {
            checkpoint: None,
            segments: segments.to_vec(),
        },
    )
}

/// Rebuild `db_path` from one immutable SQLite checkpoint plus a verified LTX tail.
pub(crate) fn restore_sqlite_from_recovery(db_path: &Path, recovery: &RecoveryData) -> Result<()> {
    validate_history(recovery.checkpoint.as_ref(), &recovery.segments)?;

    let object_dir = db_path
        .parent()
        .with_context(|| format!("object database has no parent: {}", db_path.display()))?;
    let objects_dir = object_dir
        .parent()
        .with_context(|| format!("object directory has no parent: {}", object_dir.display()))?;
    fs::create_dir_all(objects_dir)?;

    let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let object_name = object_dir
        .file_name()
        .and_then(|name| name.to_str())
        .context("object directory has no UTF-8 file name")?;
    let stage_dir = objects_dir.join(format!(
        ".{object_name}.restore.{}.{sequence}",
        std::process::id()
    ));
    let backup_dir = objects_dir.join(format!(
        ".{object_name}.backup.{}.{sequence}",
        std::process::id()
    ));
    fs::create_dir(&stage_dir)?;

    let result = (|| -> Result<()> {
        let staged_db = stage_dir.join(
            db_path
                .file_name()
                .context("object database has no file name")?,
        );
        if let Some(checkpoint) = &recovery.checkpoint {
            install_checkpoint(&staged_db, checkpoint)?;
        }
        apply_segments(&staged_db, &recovery.segments)?;
        install_recovery_base(&staged_db, recovery.checkpoint.as_ref(), &recovery.segments)?;
        fs::File::open(&stage_dir)?.sync_all()?;

        let had_existing = object_dir.exists();
        if had_existing {
            fs::rename(object_dir, &backup_dir)?;
        }

        if let Err(error) = fs::rename(&stage_dir, object_dir) {
            if had_existing {
                let _ = fs::rename(&backup_dir, object_dir);
            }
            return Err(error.into());
        }

        fs::File::open(objects_dir)?.sync_all()?;

        if had_existing {
            fs::remove_dir_all(&backup_dir)?;
        }

        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&stage_dir);
    }

    result
}

fn validate_history(
    checkpoint: Option<&RecoveredCheckpoint>,
    segments: &[LtxSegment],
) -> Result<()> {
    let mut next_txid = checkpoint.map_or(1, |checkpoint| checkpoint.metadata.through.max_txid + 1);
    let mut previous_checksum =
        checkpoint.map(|checkpoint| checkpoint.metadata.post_apply_checksum);
    let mut page_size = checkpoint.map(|checkpoint| checkpoint.metadata.page_size);

    for segment in segments {
        ensure!(
            segment.min_txid == next_txid,
            "LTX restore gap: expected TXID {next_txid}, found {}",
            segment.min_txid
        );
        ensure!(
            segment.max_txid >= segment.min_txid,
            "invalid LTX restore range {}-{}",
            segment.min_txid,
            segment.max_txid
        );
        ensure!(
            segment.pre_apply_checksum == previous_checksum,
            "LTX restore checksum chain breaks before TXID {}",
            segment.min_txid
        );
        if let Some(expected) = page_size {
            ensure!(
                segment.page_size == expected,
                "LTX page size changed at TXID {}",
                segment.min_txid
            );
        } else {
            page_size = Some(segment.page_size);
        }

        previous_checksum = Some(segment.post_apply_checksum);
        next_txid = segment
            .max_txid
            .checked_add(1)
            .context("LTX restore TXID overflow")?;
    }

    Ok(())
}

fn apply_segments(db_path: &Path, segments: &[LtxSegment]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(db_path)?;

    for segment in segments {
        let (mut decoder, header) = litetx::Decoder::new(segment.bytes.as_slice())?;
        let mut page = vec![0u8; header.page_size.into_inner() as usize];

        while let Some(pgno) = decoder.decode_page(&mut page)? {
            let offset = (u64::from(pgno.into_inner()) - 1) * u64::from(segment.page_size);
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&page)?;
        }

        decoder.finish()?;
        file.set_len(u64::from(segment.commit) * u64::from(segment.page_size))?;
    }

    file.sync_all()?;

    Ok(())
}

fn install_checkpoint(db_path: &Path, checkpoint: &RecoveredCheckpoint) -> Result<()> {
    ensure!(
        u64::try_from(checkpoint.bytes.len()).ok() == Some(checkpoint.metadata.byte_len),
        "checkpoint byte length changed before restore"
    );
    ensure!(
        checkpoint.metadata.byte_len % u64::from(checkpoint.metadata.page_size) == 0,
        "checkpoint SQLite image is not page aligned"
    );
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(db_path)?;
    file.write_all(&checkpoint.bytes)?;
    file.sync_all()?;
    Ok(())
}

fn install_recovery_base(
    db_path: &Path,
    checkpoint: Option<&RecoveredCheckpoint>,
    segments: &[LtxSegment],
) -> Result<()> {
    let (through_txid, post_apply_checksum) = match segments.last() {
        Some(last) => (last.max_txid, last.post_apply_checksum),
        None => match checkpoint {
            Some(checkpoint) => (
                checkpoint.metadata.through.max_txid,
                checkpoint.metadata.post_apply_checksum,
            ),
            None => return Ok(()),
        },
    };
    install_capture_base(db_path, through_txid, post_apply_checksum)?;
    Ok(())
}

#[cfg(test)]
mod tests;
