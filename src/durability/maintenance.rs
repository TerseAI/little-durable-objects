use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tracing::{debug, error};

use super::{
    ActorDurabilityStore, ActorManifest, ArchiveStore, CommitLogId, CommitStore, GcsArchiveStore,
    PostgresManifestStore, RapidCommitStore, RegionalActorStore, TieredCommitStore,
    checkpoint::CheckpointCompactor, store::archive_log_key,
};
use crate::{actor_state::ActorStorageKey, postgres::PostgresDatabase};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActorMaintenanceResult {
    pub archived_logs: usize,
    pub deleted_rapid_logs: usize,
    pub checkpoint_installed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceBatchResult {
    pub attempted: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub archived_logs: usize,
    pub checkpoints_installed: usize,
    pub rapid_logs_deleted: usize,
}

pub struct DurabilityMaintenance {
    store: Arc<RegionalActorStore>,
    rapid: HashMap<String, Arc<RapidCommitStore>>,
    archives: HashMap<String, Arc<dyn ArchiveStore>>,
    checkpoint: CheckpointCompactor,
    minimum_checkpoint_tail: u64,
    rapid_gc_grace: Duration,
}

impl DurabilityMaintenance {
    pub async fn connect(
        database_url: &str,
        rapid_buckets: HashMap<String, String>,
        archive_buckets: HashMap<String, String>,
        minimum_checkpoint_tail: u64,
        rapid_gc_grace: Duration,
    ) -> Result<Self> {
        let database = PostgresDatabase::connect(database_url).await?;
        ensure!(
            rapid_buckets.len() == archive_buckets.len()
                && rapid_buckets
                    .keys()
                    .all(|region| archive_buckets.contains_key(region)),
            "Rapid and Standard bucket maps must contain the same actor regions"
        );
        let archives = connect_archive_stores(archive_buckets).await?;
        let mut rapid = HashMap::new();
        let mut commits = HashMap::<String, Arc<dyn CommitStore>>::new();
        for (region, bucket) in rapid_buckets {
            let rapid_store = Arc::new(RapidCommitStore::connect(bucket).await?);
            let hot: Arc<dyn CommitStore> = rapid_store.clone();
            let archive = archives
                .get(&region)
                .cloned()
                .with_context(|| format!("missing Standard bucket for actor region {region:?}"))?;
            commits.insert(
                region.clone(),
                Arc::new(TieredCommitStore::new(hot, archive)),
            );
            rapid.insert(region, rapid_store);
        }
        let store = Arc::new(RegionalActorStore::with_region_stores(
            Arc::new(PostgresManifestStore::from_database(database)),
            commits,
            archives.clone(),
        )?);
        Ok(Self::with_region_stores(
            store,
            rapid,
            archives,
            minimum_checkpoint_tail,
            rapid_gc_grace,
        ))
    }

    fn with_region_stores(
        store: Arc<RegionalActorStore>,
        rapid: HashMap<String, Arc<RapidCommitStore>>,
        archives: HashMap<String, Arc<dyn ArchiveStore>>,
        minimum_checkpoint_tail: u64,
        rapid_gc_grace: Duration,
    ) -> Self {
        let minimum_checkpoint_tail = minimum_checkpoint_tail.max(1);
        Self {
            checkpoint: CheckpointCompactor::new(store.clone(), minimum_checkpoint_tail),
            store,
            rapid,
            archives,
            minimum_checkpoint_tail,
            rapid_gc_grace,
        }
    }

    fn rapid_for(&self, home_region: &str) -> Result<Arc<RapidCommitStore>> {
        self.rapid
            .get(home_region)
            .or_else(|| {
                (self.rapid.len() == 1)
                    .then(|| self.rapid.values().next())
                    .flatten()
            })
            .cloned()
            .with_context(|| {
                format!("no Rapid bucket is configured for actor region {home_region:?}")
            })
    }

    fn archive_for(&self, home_region: &str) -> Result<Arc<dyn ArchiveStore>> {
        self.archives
            .get(home_region)
            .or_else(|| {
                (self.archives.len() == 1)
                    .then(|| self.archives.values().next())
                    .flatten()
            })
            .cloned()
            .with_context(|| {
                format!(
                    "no Standard multi-region bucket is configured for actor region {home_region:?}"
                )
            })
    }

    /// Process up to `limit` indexed PostgreSQL manifests once. Candidate discovery
    /// errors fail the pass; per-object errors are logged and isolated so later
    /// candidates still receive maintenance. The supervisor owns polling cadence,
    /// retries, and horizontal sharding.
    pub async fn run_once(&self, limit: usize) -> Result<MaintenanceBatchResult> {
        let objects = self
            .store
            .maintenance_candidates(self.minimum_checkpoint_tail, limit)
            .await?;
        Ok(maintain_objects(objects, |object| async move {
            self.maintain_object(&object).await
        })
        .await)
    }

    async fn maintain_object(&self, object: &ActorStorageKey) -> Result<ActorMaintenanceResult> {
        let mut pass = ActorMaintenanceResult::default();
        self.archive_closed_logs(object, &mut pass).await?;
        pass.checkpoint_installed = self.checkpoint.compact(object).await?.is_some();
        self.collect_rapid_logs(object, &mut pass).await?;
        Ok(pass)
    }

    async fn archive_closed_logs(
        &self,
        object: &ActorStorageKey,
        pass: &mut ActorMaintenanceResult,
    ) -> Result<()> {
        let Some(manifest) = self.store.manifest(object).await? else {
            return Ok(());
        };
        let positions = self
            .store
            .canonical_positions_after(object, &manifest.manifest, manifest.manifest.archived_txid)
            .await?;
        let rapid = self.rapid_for(&manifest.manifest.home_region)?;
        let archives = self.archive_for(&manifest.manifest.home_region)?;
        for range in closed_log_ranges(&positions) {
            if range.max_txid <= manifest.manifest.archived_txid {
                continue;
            }
            let log = rapid
                .finalize_log(object, &range.id)
                .await?
                .with_context(|| {
                    format!("missing closed Rapid log for {object}: {:?}", range.id)
                })?;
            ensure!(
                log.max_txid >= range.max_txid,
                "Rapid archive log ends before canonical TXID {} for {object}",
                range.max_txid
            );
            archives
                .put_immutable(&archive_log_key(object, &range.id), &log.bytes)
                .await?;
            self.store
                .advance_watermarks(object, range.max_txid, 0)
                .await?;
            pass.archived_logs += 1;
        }
        Ok(())
    }

    async fn collect_rapid_logs(
        &self,
        object: &ActorStorageKey,
        pass: &mut ActorMaintenanceResult,
    ) -> Result<()> {
        let Some(manifest) = self.store.manifest(object).await? else {
            return Ok(());
        };
        let safe_through = archive_recovery_watermark(&manifest.manifest);
        let positions = self
            .store
            .canonical_positions_after(object, &manifest.manifest, manifest.manifest.rapid_gc_txid)
            .await?;
        let rapid = self.rapid_for(&manifest.manifest.home_region)?;
        let archives = self.archive_for(&manifest.manifest.home_region)?;
        for range in closed_log_ranges(&positions) {
            if range.max_txid <= manifest.manifest.rapid_gc_txid || range.max_txid > safe_through {
                continue;
            }
            let archive_key = archive_log_key(object, &range.id);
            if !archives
                .replication_grace_elapsed(&archive_key, self.rapid_gc_grace)
                .await?
            {
                debug!(
                    object = %object,
                    epoch = range.id.epoch,
                    generation = range.id.generation,
                    rapid_gc_txid = range.max_txid,
                    grace_ms = self.rapid_gc_grace.as_millis(),
                    "retaining Rapid log while its Standard copy crosses the replication grace window"
                );
                continue;
            }
            let log = rapid.finalize_log(object, &range.id).await?;
            let rapid = rapid.clone();
            let delete_object = object.clone();
            let store = self.store.clone();
            let advance_object = object.clone();
            let archived_txid = manifest.manifest.archived_txid;
            let rapid_gc_txid = range.max_txid;
            let deleted = finish_rapid_gc_range(
                log,
                move |log| async move { rapid.delete_log(&delete_object, &log).await },
                move || async move {
                    store
                        .advance_watermarks(&advance_object, archived_txid, rapid_gc_txid)
                        .await
                        .map(|_| ())
                },
            )
            .await?;
            if deleted {
                pass.deleted_rapid_logs += 1;
            } else {
                debug!(
                    object = %object,
                    epoch = range.id.epoch,
                    generation = range.id.generation,
                    rapid_gc_txid,
                    "Rapid log was already absent; resumed GC at the watermark update"
                );
            }
        }
        Ok(())
    }
}

async fn connect_archive_stores(
    configured: HashMap<String, String>,
) -> Result<HashMap<String, Arc<dyn ArchiveStore>>> {
    let mut by_bucket = HashMap::<String, Arc<dyn ArchiveStore>>::new();
    let mut stores = HashMap::with_capacity(configured.len());
    for (region, bucket) in configured {
        let store = match by_bucket.get(&bucket) {
            Some(store) => store.clone(),
            None => {
                let store: Arc<dyn ArchiveStore> =
                    Arc::new(GcsArchiveStore::connect(bucket.clone()).await?);
                by_bucket.insert(bucket, store.clone());
                store
            }
        };
        stores.insert(region, store);
    }
    Ok(stores)
}

async fn maintain_objects<Maintain, MaintenanceFuture>(
    objects: Vec<ActorStorageKey>,
    mut maintain: Maintain,
) -> MaintenanceBatchResult
where
    Maintain: FnMut(ActorStorageKey) -> MaintenanceFuture,
    MaintenanceFuture: Future<Output = Result<ActorMaintenanceResult>>,
{
    let attempted = objects.len();
    let mut batch = MaintenanceBatchResult {
        attempted,
        ..MaintenanceBatchResult::default()
    };
    for object in objects {
        match maintain(object.clone()).await {
            Ok(pass) => {
                batch.succeeded += 1;
                batch.archived_logs += pass.archived_logs;
                batch.checkpoints_installed += usize::from(pass.checkpoint_installed);
                batch.rapid_logs_deleted += pass.deleted_rapid_logs;
            }
            Err(maintenance_error) => {
                batch.failed += 1;
                error!(
                    object = %object,
                    error = %format!("{maintenance_error:#}"),
                    "durability maintenance failed for object; continuing batch"
                );
            }
        }
    }
    batch
}

/// Complete one crash-resumable Rapid GC transition. Regional recovery coverage is
/// checked by the caller before this point. A missing log means a previous deletion
/// completed without its watermark update, so the durable update must still run.
async fn finish_rapid_gc_range<Delete, DeleteFuture, Advance, AdvanceFuture>(
    log: Option<super::FinalizedCommitLog>,
    delete: Delete,
    advance: Advance,
) -> Result<bool>
where
    Delete: FnOnce(super::FinalizedCommitLog) -> DeleteFuture,
    DeleteFuture: Future<Output = Result<()>>,
    Advance: FnOnce() -> AdvanceFuture,
    AdvanceFuture: Future<Output = Result<()>>,
{
    let deleted = match log {
        Some(log) => {
            delete(log).await?;
            true
        }
        None => false,
    };
    advance().await?;
    Ok(deleted)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ClosedLogRange {
    id: CommitLogId,
    max_txid: u64,
}

fn closed_log_ranges(positions: &[super::CommitPosition]) -> Vec<ClosedLogRange> {
    let Some(active) = positions.last().map(CommitLogId::from) else {
        return Vec::new();
    };
    let mut ranges = Vec::<ClosedLogRange>::new();
    for position in positions {
        let id = CommitLogId::from(position);
        if id == active {
            continue;
        }
        match ranges.last_mut() {
            Some(range) if range.id == id => range.max_txid = position.max_txid,
            _ => ranges.push(ClosedLogRange {
                id,
                max_txid: position.max_txid,
            }),
        }
    }
    ranges
}

fn archive_recovery_watermark(manifest: &ActorManifest) -> u64 {
    manifest
        .checkpoint
        .as_ref()
        .map_or(manifest.archived_txid, |checkpoint| {
            manifest.archived_txid.max(checkpoint.through.max_txid)
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::durability::CommitPosition;

    #[test]
    fn only_generations_before_the_active_one_are_closed() {
        let positions = [
            position(1, 0, 1),
            position(1, 0, 64),
            position(1, 1, 65),
            position(1, 1, 70),
        ];
        assert_eq!(
            closed_log_ranges(&positions),
            vec![ClosedLogRange {
                id: CommitLogId {
                    epoch: 1,
                    generation: 0,
                },
                max_txid: 64,
            }]
        );
    }

    #[test]
    fn ownership_change_closes_the_previous_epoch() {
        let positions = [position(1, 0, 7), position(2, 0, 8)];
        assert_eq!(closed_log_ranges(&positions)[0].max_txid, 7);
    }

    #[tokio::test]
    async fn missing_rapid_log_resumes_at_the_watermark_update() -> Result<()> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let delete_calls = calls.clone();
        let advance_calls = calls.clone();

        let deleted = finish_rapid_gc_range(
            None,
            move |_| async move {
                delete_calls.lock().expect("calls lock").push("delete");
                Ok(())
            },
            move || async move {
                advance_calls.lock().expect("calls lock").push("advance");
                Ok(())
            },
        )
        .await?;

        assert!(!deleted);
        assert_eq!(*calls.lock().expect("calls lock"), ["advance"]);
        Ok(())
    }

    #[tokio::test]
    async fn present_rapid_log_is_deleted_before_the_watermark_update() -> Result<()> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let delete_calls = calls.clone();
        let advance_calls = calls.clone();
        let log = super::super::FinalizedCommitLog {
            id: CommitLogId {
                epoch: 1,
                generation: 0,
            },
            bytes: Vec::new(),
            source_generation: 1,
            max_txid: 64,
        };

        let deleted = finish_rapid_gc_range(
            Some(log),
            move |_| async move {
                delete_calls.lock().expect("calls lock").push("delete");
                Ok(())
            },
            move || async move {
                advance_calls.lock().expect("calls lock").push("advance");
                Ok(())
            },
        )
        .await?;

        assert!(deleted);
        assert_eq!(*calls.lock().expect("calls lock"), ["delete", "advance"]);
        Ok(())
    }

    #[tokio::test]
    async fn failed_rapid_deletion_does_not_advance_the_watermark() {
        let advanced = Arc::new(Mutex::new(false));
        let advance_state = advanced.clone();
        let log = super::super::FinalizedCommitLog {
            id: CommitLogId {
                epoch: 1,
                generation: 0,
            },
            bytes: Vec::new(),
            source_generation: 1,
            max_txid: 64,
        };

        let error = finish_rapid_gc_range(
            Some(log),
            |_| async { anyhow::bail!("delete failed") },
            move || async move {
                *advance_state.lock().expect("advance lock") = true;
                Ok(())
            },
        )
        .await
        .expect_err("delete failure must stop the transition");

        assert_eq!(error.to_string(), "delete failed");
        assert!(!*advanced.lock().expect("advance lock"));
    }

    #[tokio::test]
    async fn failed_object_does_not_abort_the_maintenance_batch() {
        let attempted = Arc::new(Mutex::new(Vec::new()));
        let attempted_objects = attempted.clone();
        let failed = ActorStorageKey::new("actor-a");
        let successful = ActorStorageKey::new("actor-b");

        let results = maintain_objects(vec![failed.clone(), successful.clone()], move |object| {
            let attempted_objects = attempted_objects.clone();
            let failed = failed.clone();
            async move {
                attempted_objects
                    .lock()
                    .expect("attempted objects lock")
                    .push(object.clone());
                if object == failed {
                    anyhow::bail!("damaged actor storage");
                }
                Ok(ActorMaintenanceResult {
                    archived_logs: 1,
                    ..ActorMaintenanceResult::default()
                })
            }
        })
        .await;

        assert_eq!(
            *attempted.lock().expect("attempted objects lock"),
            [
                ActorStorageKey::new("actor-a"),
                ActorStorageKey::new("actor-b")
            ]
        );
        assert_eq!(results.attempted, 2);
        assert_eq!(results.succeeded, 1);
        assert_eq!(results.failed, 1);
        assert_eq!(results.archived_logs, 1);
        assert_eq!(results.checkpoints_installed, 0);
        assert_eq!(results.rapid_logs_deleted, 0);
    }

    fn position(epoch: u64, log_generation: u64, max_txid: u64) -> CommitPosition {
        CommitPosition {
            epoch,
            log_generation,
            max_txid,
        }
    }
}
