use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tokio_postgres::Row;

use super::{
    ActorManifest, CheckpointMetadata, CommitPosition, MANIFEST_FORMAT_VERSION, ManifestStore,
    ManifestVersion, OwnershipClaimResult, VersionedActorManifest,
};
use crate::{
    actor_state::{ActorOwner, ActorStorageKey},
    host::HostId,
    postgres::PostgresDatabase,
};

const MANIFEST_COLUMNS: &str = "format_version, owner_node, owner_epoch, tip_epoch, tip_log_generation, tip_txid, \
     checkpoint_epoch, checkpoint_log_generation, checkpoint_txid, checkpoint_key, checkpoint_byte_len, \
     checkpoint_crc32c, checkpoint_page_size, checkpoint_checksum, \
     archived_txid, rapid_gc_txid, storage_region, revision";

pub struct PostgresManifestStore {
    database: PostgresDatabase,
}

impl PostgresManifestStore {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::from_database(PostgresDatabase::connect(url).await?))
    }

    pub(crate) fn from_database(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl ManifestStore for PostgresManifestStore {
    async fn manifest(&self, object: &ActorStorageKey) -> Result<Option<VersionedActorManifest>> {
        let statement =
            format!("SELECT {MANIFEST_COLUMNS} FROM durable_object_manifests WHERE object_id = $1");
        self.database
            .client()
            .query_opt(&statement, &[&object.as_str()])
            .await
            .context("load PostgreSQL object manifest")?
            .map(versioned_manifest)
            .transpose()
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
            !home_region.is_empty() && home_region.len() <= 64,
            "storage region is invalid"
        );
        let row = match expected {
            None => {
                let statement = format!(
                    "INSERT INTO durable_object_manifests \
                     (object_id, format_version, owner_node, owner_epoch, tip_epoch, tip_log_generation, tip_txid, \
                      archived_txid, rapid_gc_txid, storage_region, revision) \
                     VALUES ($1, $2, $3, 1, NULL, NULL, NULL, 0, 0, $4, 1) \
                     ON CONFLICT DO NOTHING RETURNING {MANIFEST_COLUMNS}"
                );
                self.database
                    .client()
                    .query_opt(
                        &statement,
                        &[
                            &object.as_str(),
                            &i32::try_from(MANIFEST_FORMAT_VERSION)?,
                            &host.as_str(),
                            &home_region,
                        ],
                    )
                    .await
                    .context("initial PostgreSQL object manifest claim")?
            }
            Some(current) => {
                current.manifest.validate()?;
                ensure!(
                    current.manifest.home_region == home_region,
                    "object home region cannot change from {} to {home_region}",
                    current.manifest.home_region
                );
                let statement = format!(
                    "UPDATE durable_object_manifests \
                     SET owner_node = $2, owner_epoch = owner_epoch + 1, revision = revision + 1 \
                     WHERE object_id = $1 AND revision = $3 \
                       AND owner_node = $4 AND owner_epoch = $5 \
                     RETURNING {MANIFEST_COLUMNS}"
                );
                self.database
                    .client()
                    .query_opt(
                        &statement,
                        &[
                            &object.as_str(),
                            &host.as_str(),
                            &revision(&current.version)?,
                            &current.owner().host.as_str(),
                            &database_i64(current.owner().epoch, "owner epoch")?,
                        ],
                    )
                    .await
                    .context("take over PostgreSQL object manifest")?
            }
        };

        if let Some(row) = row {
            return Ok(OwnershipClaimResult::Acquired(versioned_manifest(row)?));
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
        let (tip_epoch, tip_log_generation, tip_txid) = database_tip(&next.tip)?;
        let checkpoint = database_checkpoint(next.checkpoint.as_ref())?;
        let statement = format!(
            "UPDATE durable_object_manifests \
             SET tip_epoch = $2, tip_log_generation = $3, tip_txid = $4, \
                 checkpoint_epoch = $5, checkpoint_log_generation = $6, checkpoint_txid = $7, checkpoint_key = $8, \
                 checkpoint_byte_len = $9, checkpoint_crc32c = $10, \
                 checkpoint_page_size = $11, checkpoint_checksum = $12, \
                 archived_txid = $13, rapid_gc_txid = $14, revision = revision + 1 \
             WHERE object_id = $1 AND revision = $15 \
               AND owner_node = $16 AND owner_epoch = $17 \
             RETURNING {MANIFEST_COLUMNS}"
        );
        self.database
            .client()
            .query_opt(
                &statement,
                &[
                    &object.as_str(),
                    &tip_epoch,
                    &tip_log_generation,
                    &tip_txid,
                    &checkpoint.epoch,
                    &checkpoint.log_generation,
                    &checkpoint.txid,
                    &checkpoint.key,
                    &checkpoint.byte_len,
                    &checkpoint.crc32c,
                    &checkpoint.page_size,
                    &checkpoint.checksum,
                    &database_i64(next.archived_txid, "archived TXID")?,
                    &database_i64(next.rapid_gc_txid, "Rapid GC TXID")?,
                    &revision(&current.version)?,
                    &current.owner().host.as_str(),
                    &database_i64(current.owner().epoch, "owner epoch")?,
                ],
            )
            .await
            .context("advance PostgreSQL object manifest")?
            .map(versioned_manifest)
            .transpose()
    }

    async fn advance_tip(
        &self,
        object: &ActorStorageKey,
        current: &VersionedActorManifest,
        next: &ActorManifest,
    ) -> Result<Option<VersionedActorManifest>> {
        current.manifest.validate()?;
        next.validate()?;
        ensure!(
            current.manifest.owner == next.owner,
            "commit-tip advance cannot change the object owner"
        );
        ensure!(
            current.manifest.checkpoint == next.checkpoint
                && current.manifest.archived_txid == next.archived_txid
                && current.manifest.rapid_gc_txid == next.rapid_gc_txid,
            "commit-tip advance cannot mutate maintenance metadata"
        );
        let (tip_epoch, tip_log_generation, tip_txid) = database_tip(&next.tip)?;
        let (expected_epoch, expected_log_generation, expected_txid) =
            database_tip(&current.manifest.tip)?;
        let statement = format!(
            "UPDATE durable_object_manifests \
             SET tip_epoch = $2, tip_log_generation = $3, tip_txid = $4, revision = revision + 1 \
             WHERE object_id = $1 \
               AND owner_node = $5 AND owner_epoch = $6 \
               AND tip_epoch IS NOT DISTINCT FROM $7 \
               AND tip_log_generation IS NOT DISTINCT FROM $8 \
               AND tip_txid IS NOT DISTINCT FROM $9 \
             RETURNING {MANIFEST_COLUMNS}"
        );
        self.database
            .client()
            .query_opt(
                &statement,
                &[
                    &object.as_str(),
                    &tip_epoch,
                    &tip_log_generation,
                    &tip_txid,
                    &current.owner().host.as_str(),
                    &database_i64(current.owner().epoch, "owner epoch")?,
                    &expected_epoch,
                    &expected_log_generation,
                    &expected_txid,
                ],
            )
            .await
            .context("advance PostgreSQL object commit tip")?
            .map(versioned_manifest)
            .transpose()
    }

    async fn maintenance_candidates(
        &self,
        minimum_checkpoint_tail: u64,
        limit: usize,
    ) -> Result<Vec<ActorStorageKey>> {
        let rows = self
            .database
            .client()
            .query(
                "SELECT object_id FROM durable_object_manifests \
                 WHERE tip_txid IS NOT NULL AND (\
                    tip_txid - COALESCE(checkpoint_txid, 0) >= $1 \
                    OR archived_txid < tip_txid \
                    OR rapid_gc_txid < GREATEST(archived_txid, COALESCE(checkpoint_txid, 0))\
                 ) \
                 ORDER BY object_id LIMIT $2",
                &[
                    &database_i64(minimum_checkpoint_tail, "checkpoint threshold")?,
                    &i64::try_from(limit).context("maintenance candidate limit is too large")?,
                ],
            )
            .await
            .context("list PostgreSQL durability maintenance candidates")?;
        Ok(rows
            .into_iter()
            .map(|row| ActorStorageKey::new(row.get::<_, String>(0)))
            .collect())
    }
}

fn versioned_manifest(row: Row) -> Result<VersionedActorManifest> {
    let format_version = u32::try_from(row.get::<_, i32>(0))
        .context("negative PostgreSQL manifest format version")?;
    let owner_epoch = application_u64(row.get(2), "owner epoch")?;
    let tip_epoch = row
        .get::<_, Option<i64>>(3)
        .map(|value| application_u64(value, "tip epoch"))
        .transpose()?;
    let tip_log_generation = row
        .get::<_, Option<i64>>(4)
        .map(|value| application_u64(value, "tip log generation"))
        .transpose()?;
    let tip_txid = row
        .get::<_, Option<i64>>(5)
        .map(|value| application_u64(value, "tip TXID"))
        .transpose()?;
    ensure!(
        tip_epoch.is_some() == tip_log_generation.is_some()
            && tip_epoch.is_some() == tip_txid.is_some(),
        "PostgreSQL manifest has a partial commit tip"
    );
    let checkpoint = checkpoint_from_row(&row)?;
    let manifest = ActorManifest {
        format_version,
        owner: ActorOwner {
            host: HostId::new(row.get::<_, String>(1)),
            epoch: owner_epoch,
        },
        tip: tip_epoch.zip(tip_log_generation).zip(tip_txid).map(
            |((epoch, log_generation), max_txid)| CommitPosition {
                epoch,
                log_generation,
                max_txid,
            },
        ),
        checkpoint,
        archived_txid: application_u64(row.get(14), "archived TXID")?,
        rapid_gc_txid: application_u64(row.get(15), "Rapid GC TXID")?,
        home_region: row.get(16),
    };
    manifest.validate()?;
    Ok(VersionedActorManifest {
        manifest,
        version: postgres_version(row.get(17)),
    })
}

fn checkpoint_from_row(row: &Row) -> Result<Option<CheckpointMetadata>> {
    let checkpoint_epoch = optional_u64(row, 6, "checkpoint epoch")?;
    let checkpoint_log_generation = optional_u64(row, 7, "checkpoint log generation")?;
    let checkpoint_txid = optional_u64(row, 8, "checkpoint TXID")?;
    let checkpoint_key = row.get::<_, Option<String>>(9);
    let checkpoint_byte_len = optional_u64(row, 10, "checkpoint byte length")?;
    let checkpoint_crc32c = optional_u64(row, 11, "checkpoint CRC32C")?;
    let checkpoint_page_size = optional_u64(row, 12, "checkpoint page size")?;
    let checkpoint_checksum = row.get::<_, Option<String>>(13);
    let checkpoint_presence = [
        checkpoint_epoch.is_some(),
        checkpoint_log_generation.is_some(),
        checkpoint_txid.is_some(),
        checkpoint_key.is_some(),
        checkpoint_byte_len.is_some(),
        checkpoint_crc32c.is_some(),
        checkpoint_page_size.is_some(),
        checkpoint_checksum.is_some(),
    ];
    ensure!(
        checkpoint_presence.iter().all(|present| *present)
            || checkpoint_presence.iter().all(|present| !*present),
        "PostgreSQL manifest has partial checkpoint metadata"
    );
    match (
        checkpoint_epoch,
        checkpoint_log_generation,
        checkpoint_txid,
        checkpoint_key,
        checkpoint_byte_len,
        checkpoint_crc32c,
        checkpoint_page_size,
        checkpoint_checksum,
    ) {
        (
            Some(epoch),
            Some(log_generation),
            Some(max_txid),
            Some(object_key),
            Some(byte_len),
            Some(crc32c),
            Some(page_size),
            Some(checksum),
        ) => Ok(Some(CheckpointMetadata {
            through: CommitPosition {
                epoch,
                log_generation,
                max_txid,
            },
            object_key,
            byte_len,
            crc32c: u32::try_from(crc32c).context("checkpoint CRC32C exceeds u32")?,
            page_size: u32::try_from(page_size).context("checkpoint page size exceeds u32")?,
            post_apply_checksum: u64::from_str_radix(&checksum, 16)
                .context("invalid checkpoint checksum")?,
        })),
        _ => Ok(None),
    }
}

fn postgres_version(revision: i64) -> ManifestVersion {
    ManifestVersion::from_bytes(revision.to_string().into_bytes())
}

fn revision(version: &ManifestVersion) -> Result<i64> {
    let revision = std::str::from_utf8(version.as_bytes())?
        .parse::<i64>()
        .context("invalid PostgreSQL manifest revision")?;
    ensure!(
        revision > 0,
        "PostgreSQL manifest revision must be positive"
    );
    Ok(revision)
}

fn database_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{name} exceeds PostgreSQL BIGINT"))
}

fn application_u64(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("PostgreSQL {name} is negative"))
}

fn database_tip(tip: &Option<CommitPosition>) -> Result<(Option<i64>, Option<i64>, Option<i64>)> {
    match tip {
        Some(tip) => Ok((
            Some(database_i64(tip.epoch, "tip epoch")?),
            Some(database_i64(tip.log_generation, "tip log generation")?),
            Some(database_i64(tip.max_txid, "tip TXID")?),
        )),
        None => Ok((None, None, None)),
    }
}

struct DatabaseCheckpoint {
    epoch: Option<i64>,
    log_generation: Option<i64>,
    txid: Option<i64>,
    key: Option<String>,
    byte_len: Option<i64>,
    crc32c: Option<i64>,
    page_size: Option<i64>,
    checksum: Option<String>,
}

fn database_checkpoint(checkpoint: Option<&CheckpointMetadata>) -> Result<DatabaseCheckpoint> {
    match checkpoint {
        Some(checkpoint) => Ok(DatabaseCheckpoint {
            epoch: Some(database_i64(checkpoint.through.epoch, "checkpoint epoch")?),
            log_generation: Some(database_i64(
                checkpoint.through.log_generation,
                "checkpoint log generation",
            )?),
            txid: Some(database_i64(
                checkpoint.through.max_txid,
                "checkpoint TXID",
            )?),
            key: Some(checkpoint.object_key.clone()),
            byte_len: Some(database_i64(checkpoint.byte_len, "checkpoint byte length")?),
            crc32c: Some(i64::from(checkpoint.crc32c)),
            page_size: Some(i64::from(checkpoint.page_size)),
            checksum: Some(format!("{:016x}", checkpoint.post_apply_checksum)),
        }),
        None => Ok(DatabaseCheckpoint {
            epoch: None,
            log_generation: None,
            txid: None,
            key: None,
            byte_len: None,
            crc32c: None,
            page_size: None,
            checksum: None,
        }),
    }
}

fn optional_u64(row: &Row, index: usize, name: &str) -> Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)
        .map(|value| application_u64(value, name))
        .transpose()
}
