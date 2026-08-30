//! Zonal LTX persistence in a GCS Rapid appendable object.
//!
//! Every ownership epoch is split into bounded append-only log generations.
//! Individual commit bundles remain logically immutable records inside a log, while one warm
//! `BidiWriteObject` stream avoids an object creation for every SQLite commit.
//!
//! PostgreSQL remains authoritative: callers advance the manifest only after
//! `flush()` returns the persisted offset. Bytes beyond the PostgreSQL tip are
//! therefore harmless after an ambiguous write or a fenced manifest CAS.
//!
//! A separately supervised durability worker finalizes closed generations, copies
//! them to multi-region archive storage, and advances PostgreSQL watermarks before garbage
//! collection.

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_storage::{
    appendable_object_writer::AppendableObjectWriter,
    client::{Storage, StorageControl},
};
use tokio::sync::Mutex;
use tracing::{debug, trace};

use super::{CommitLogId, CommitPosition, CommitStore};
use crate::actor_state::ActorStorageKey;

const RAPID_FRAME_MAGIC: &[u8; 8] = b"TDORAP02";
const RAPID_FRAME_HEADER_BYTES: usize = RAPID_FRAME_MAGIC.len() + 8 + 8 + 8 + 8 + 4;
const RAPID_READ_CACHE_LOGS: usize = 32;
/// Maximum warm writer streams retained after commit calls finish.
const RAPID_WRITER_LOGS: usize = 64;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct RapidLogId {
    object: String,
    epoch: u64,
    generation: u64,
}

impl RapidLogId {
    fn new(object: &ActorStorageKey, position: &CommitPosition) -> Self {
        Self {
            object: object.as_str().into(),
            epoch: position.epoch,
            generation: position.log_generation,
        }
    }

    fn from_log(object: &ActorStorageKey, id: &CommitLogId) -> Self {
        Self {
            object: object.as_str().into(),
            epoch: id.epoch,
            generation: id.generation,
        }
    }

    fn key(&self) -> String {
        format!(
            "objects/{}/rapid/e{:016x}/g{:016x}.ltxlog",
            self.object, self.epoch, self.generation
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecordFingerprint {
    byte_len: u64,
    crc32c: u32,
}

impl RecordFingerprint {
    fn new(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            byte_len: u64::try_from(bytes.len()).context("Rapid commit bundle is too large")?,
            crc32c: crc32c::crc32c(bytes),
        })
    }
}

#[derive(Clone, Debug)]
struct RapidRecord {
    payload: Range<usize>,
    fingerprint: RecordFingerprint,
}

#[derive(Debug)]
struct ParsedRapidLog {
    bytes: Arc<Vec<u8>>,
    records: HashMap<u64, RapidRecord>,
    complete_len: usize,
}

impl ParsedRapidLog {
    fn trailing_bytes(&self) -> Vec<u8> {
        self.bytes[self.complete_len..].to_vec()
    }

    fn payload(&self, max_txid: u64) -> Option<Vec<u8>> {
        self.records
            .get(&max_txid)
            .map(|record| self.bytes[record.payload.clone()].to_vec())
    }
}

#[derive(Default)]
struct RapidReadCache {
    entries: HashMap<RapidLogId, Arc<ParsedRapidLog>>,
    order: VecDeque<RapidLogId>,
}

impl RapidReadCache {
    fn get(&mut self, id: &RapidLogId) -> Option<Arc<ParsedRapidLog>> {
        let value = self.entries.get(id)?.clone();
        self.order.retain(|candidate| candidate != id);
        self.order.push_back(id.clone());
        Some(value)
    }

    fn insert(&mut self, id: RapidLogId, log: Arc<ParsedRapidLog>) {
        self.order.retain(|candidate| candidate != &id);
        self.entries.insert(id.clone(), log);
        self.order.push_back(id);
        while self.order.len() > RAPID_READ_CACHE_LOGS {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }

    fn remove(&mut self, id: &RapidLogId) {
        self.entries.remove(id);
        self.order.retain(|candidate| candidate != id);
    }
}

#[derive(Default)]
struct RapidWriterCache {
    entries: HashMap<RapidLogId, Arc<Mutex<RapidLogWriterState>>>,
    order: VecDeque<RapidLogId>,
}

impl RapidWriterCache {
    fn get_or_insert(&mut self, id: &RapidLogId) -> Arc<Mutex<RapidLogWriterState>> {
        if let Some(state) = self.entries.get(id).cloned() {
            self.touch(id);
            return state;
        }

        // Only the current generation for an object is useful as a warm writer.
        let older: Vec<_> = self
            .entries
            .keys()
            .filter(|candidate| candidate.object == id.object && *candidate != id)
            .cloned()
            .collect();
        for candidate in older {
            self.remove(&candidate);
        }

        let state = Arc::new(Mutex::new(RapidLogWriterState::default()));
        self.entries.insert(id.clone(), state.clone());
        self.touch(id);
        while self.order.len() > RAPID_WRITER_LOGS {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        state
    }

    fn touch(&mut self, id: &RapidLogId) {
        self.order.retain(|candidate| candidate != id);
        self.order.push_back(id.clone());
    }

    fn remove(&mut self, id: &RapidLogId) -> Option<Arc<Mutex<RapidLogWriterState>>> {
        self.order.retain(|candidate| candidate != id);
        self.entries.remove(id)
    }
}

#[derive(Default)]
struct RapidLogWriterState {
    writer: Option<AppendableObjectWriter>,
    records: HashMap<u64, RecordFingerprint>,
    persisted_size: usize,
    trailing: Vec<u8>,
    initialized: bool,
}

impl RapidLogWriterState {
    fn invalidate(&mut self) {
        self.writer = None;
        self.records.clear();
        self.persisted_size = 0;
        self.trailing.clear();
        self.initialized = false;
    }
}

struct LoadedRapidLog {
    generation: i64,
    parsed: ParsedRapidLog,
}

pub struct FinalizedCommitLog {
    pub id: CommitLogId,
    pub bytes: Vec<u8>,
    pub source_generation: i64,
    pub max_txid: u64,
}

/// Stores logically immutable LTX commit bundles in bounded GCS Rapid appendable
/// objects partitioned by ownership epoch and log generation.
pub struct RapidCommitStore {
    client: Storage,
    control: StorageControl,
    bucket: String,
    writers: Mutex<RapidWriterCache>,
    read_cache: Mutex<RapidReadCache>,
}

impl RapidCommitStore {
    pub async fn connect(bucket: impl Into<String>) -> Result<Self> {
        let bucket = bucket.into();
        let control = StorageControl::builder().build().await?;
        let metadata = control
            .get_bucket()
            .set_name(bucket_name(&bucket))
            .send()
            .await
            .with_context(|| format!("read GCS Rapid bucket metadata for {bucket}"))?;
        validate_rapid_bucket(&bucket, &metadata)?;
        Ok(Self {
            client: Storage::builder().build().await?,
            control,
            bucket,
            writers: Mutex::new(RapidWriterCache::default()),
            read_cache: Mutex::new(RapidReadCache::default()),
        })
    }

    async fn writer_state(&self, id: &RapidLogId) -> Arc<Mutex<RapidLogWriterState>> {
        let mut writers = self.writers.lock().await;
        // Dropping an evicted local stream does not finalize its object. A later
        // commit can reopen it, while finalization remains the durability worker's job.
        writers.get_or_insert(id)
    }

    async fn initialize_writer(
        &self,
        id: &RapidLogId,
        state: &mut RapidLogWriterState,
    ) -> Result<()> {
        if state.initialized {
            return Ok(());
        }

        let bucket = bucket_name(&self.bucket);
        let key = id.key();
        let (writer, records, persisted_size, trailing) = match self.load_log(id).await? {
            Some(loaded) => {
                let expected_size = loaded.parsed.bytes.len();
                let writer = self
                    .client
                    .reopen_appendable_object(&bucket, &key, loaded.generation)
                    .send()
                    .await
                    .with_context(|| format!("reopen Rapid append log {key}"))?;
                ensure!(
                    usize::try_from(writer.persisted_size()).ok() == Some(expected_size),
                    "Rapid append log changed while reopening {key}: read {expected_size} bytes, server reports {}",
                    writer.persisted_size()
                );
                let records = loaded
                    .parsed
                    .records
                    .iter()
                    .map(|(txid, record)| (*txid, record.fingerprint))
                    .collect();
                (
                    writer,
                    records,
                    expected_size,
                    loaded.parsed.trailing_bytes(),
                )
            }
            None => {
                debug!(
                    bucket = %self.bucket,
                    key,
                    "opening new GCS Rapid append log"
                );
                let writer = self
                    .client
                    .open_appendable_object(&bucket, &key)
                    .set_if_generation_match(0)
                    .send()
                    .await
                    .with_context(|| format!("open new Rapid append log {key}"))?;
                ensure!(
                    writer.persisted_size() == 0,
                    "new Rapid append log {key} is unexpectedly non-empty"
                );
                (writer, HashMap::new(), 0, Vec::new())
            }
        };

        state.writer = Some(writer);
        state.records = records;
        state.persisted_size = persisted_size;
        state.trailing = trailing;
        state.initialized = true;
        Ok(())
    }

    async fn load_log(&self, id: &RapidLogId) -> Result<Option<LoadedRapidLog>> {
        let key = id.key();
        trace!(bucket = %self.bucket, key, "reading GCS Rapid append log");
        let mut response = match self
            .client
            .read_object(bucket_name(&self.bucket), &key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.http_status_code() == Some(404) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let generation = response.object().generation;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await.transpose()? {
            bytes.extend_from_slice(&chunk);
        }
        let parsed = parse_rapid_log(bytes, id.epoch, id.generation, &key)?;
        Ok(Some(LoadedRapidLog { generation, parsed }))
    }

    async fn cached_or_load_log(&self, id: &RapidLogId) -> Result<Option<Arc<ParsedRapidLog>>> {
        if let Some(log) = self.read_cache.lock().await.get(id) {
            return Ok(Some(log));
        }
        let Some(loaded) = self.load_log(id).await? else {
            return Ok(None);
        };
        let log = Arc::new(loaded.parsed);
        self.read_cache.lock().await.insert(id.clone(), log.clone());
        Ok(Some(log))
    }

    /// Finalize a closed Rapid generation and return its verified bytes for archive
    /// archival. Callers must never pass the manifest's active generation.
    pub async fn finalize_log(
        &self,
        object: &ActorStorageKey,
        id: &CommitLogId,
    ) -> Result<Option<FinalizedCommitLog>> {
        let rapid_id = RapidLogId::from_log(object, id);
        let key = rapid_id.key();

        let local = self.writers.lock().await.remove(&rapid_id);
        if let Some(local) = local {
            let mut state = local.lock().await;
            if let Some(writer) = state.writer.take() {
                writer
                    .finalize()
                    .await
                    .with_context(|| format!("finalize Rapid commit log {key}"))?;
            }
            state.invalidate();
        }

        let Some(loaded) = self.load_log(&rapid_id).await? else {
            return Ok(None);
        };
        ensure!(
            loaded.parsed.trailing_bytes().is_empty(),
            "cannot archive Rapid log with an incomplete frame: {key}"
        );
        if !self.log_is_finalized(&rapid_id).await? {
            let expected_size = loaded.parsed.bytes.len();
            let writer = self
                .client
                .reopen_appendable_object(bucket_name(&self.bucket), &key, loaded.generation)
                .send()
                .await
                .with_context(|| format!("reopen closed Rapid log for finalization {key}"))?;
            ensure!(
                usize::try_from(writer.persisted_size()).ok() == Some(expected_size),
                "Rapid log changed while finalizing {key}"
            );
            writer
                .finalize()
                .await
                .with_context(|| format!("finalize Rapid commit log {key}"))?;
        }
        ensure!(
            self.log_is_finalized(&rapid_id).await?,
            "Rapid log did not finalize: {key}"
        );
        let max_txid = loaded
            .parsed
            .records
            .keys()
            .copied()
            .max()
            .with_context(|| format!("Rapid log contains no commit records: {key}"))?;
        self.read_cache.lock().await.remove(&rapid_id);
        Ok(Some(FinalizedCommitLog {
            id: id.clone(),
            bytes: loaded.parsed.bytes.as_ref().clone(),
            source_generation: loaded.generation,
            max_txid,
        }))
    }

    /// Delete one already archived/finalized Rapid generation using its exact GCS
    /// generation as a precondition.
    pub async fn delete_log(
        &self,
        object: &ActorStorageKey,
        log: &FinalizedCommitLog,
    ) -> Result<()> {
        let id = RapidLogId::from_log(object, &log.id);
        let key = id.key();
        match self
            .control
            .delete_object()
            .set_bucket(bucket_name(&self.bucket))
            .set_object(&key)
            .set_if_generation_match(log.source_generation)
            .send()
            .await
        {
            Ok(_) => {
                self.read_cache.lock().await.remove(&id);
                Ok(())
            }
            Err(error) if error.http_status_code() == Some(404) => Ok(()),
            Err(error) => Err(error).with_context(|| format!("delete archived Rapid log {key}")),
        }
    }

    async fn log_is_finalized(&self, id: &RapidLogId) -> Result<bool> {
        let key = id.key();
        let object = self
            .control
            .get_object()
            .set_bucket(bucket_name(&self.bucket))
            .set_object(&key)
            .send()
            .await
            .with_context(|| format!("read Rapid log metadata {key}"))?;
        Ok(object.finalize_time.is_some())
    }
}

fn validate_rapid_bucket(name: &str, bucket: &google_cloud_storage::model::Bucket) -> Result<()> {
    ensure!(
        bucket.storage_class.eq_ignore_ascii_case("RAPID"),
        "GCS Rapid bucket {name:?} must use RAPID storage, found {:?}",
        bucket.storage_class
    );
    ensure!(
        bucket.location_type.eq_ignore_ascii_case("zone"),
        "GCS Rapid bucket {name:?} must be zonal, found location type {:?}",
        bucket.location_type
    );
    Ok(())
}

#[async_trait]
impl CommitStore for RapidCommitStore {
    async fn put_immutable(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
        bytes: &[u8],
    ) -> Result<()> {
        let id = RapidLogId::new(object, position);
        let key = id.key();
        let fingerprint = RecordFingerprint::new(bytes)?;
        let frame = encode_rapid_frame(position, bytes)?;
        let state = self.writer_state(&id).await;
        let mut state = state.lock().await;
        self.initialize_writer(&id, &mut state).await?;

        if let Some(existing) = state.records.get(&position.max_txid) {
            ensure!(
                *existing == fingerprint,
                "Rapid commit TXID {} already exists with different bytes in {key}",
                position.max_txid
            );
            return Ok(());
        }

        let suffix = if state.trailing.is_empty() {
            frame.as_slice()
        } else {
            ensure!(
                frame.starts_with(&state.trailing),
                "incomplete Rapid frame in {key} does not match retried TXID {}",
                position.max_txid
            );
            &frame[state.trailing.len()..]
        };
        let expected_size = state
            .persisted_size
            .checked_add(suffix.len())
            .context("Rapid append log size overflow")?;

        debug!(
            bucket = %self.bucket,
            key,
            owner_epoch = position.epoch,
            max_txid = position.max_txid,
            bundle_bytes = bytes.len(),
            append_bytes = suffix.len(),
            "appending GCS Rapid commit frame"
        );
        let write_result = async {
            let writer = state.writer.as_mut().expect("initialized Rapid writer");
            writer.append(Bytes::copy_from_slice(suffix)).await?;
            writer.flush().await
        }
        .await;
        let persisted_size = match write_result {
            Ok(size) => size,
            Err(error) => {
                state.invalidate();
                return Err(error).with_context(|| format!("append and flush Rapid log {key}"));
            }
        };
        let expected_size_i64 = i64::try_from(expected_size).context("Rapid log is too large")?;
        if persisted_size != expected_size_i64 {
            state.invalidate();
            bail!(
                "Rapid persisted offset mismatch for {key}: expected {expected_size}, got {persisted_size}"
            );
        }

        state.persisted_size = expected_size;
        state.trailing.clear();
        state.records.insert(position.max_txid, fingerprint);
        self.read_cache.lock().await.remove(&id);
        debug!(
            bucket = %self.bucket,
            key,
            owner_epoch = position.epoch,
            max_txid = position.max_txid,
            persisted_size,
            "persisted GCS Rapid commit frame"
        );
        Ok(())
    }

    async fn get(
        &self,
        object: &ActorStorageKey,
        position: &CommitPosition,
    ) -> Result<Option<Vec<u8>>> {
        let id = RapidLogId::new(object, position);
        let Some(log) = self.cached_or_load_log(&id).await? else {
            return Ok(None);
        };
        Ok(log.payload(position.max_txid))
    }
}

fn bucket_name(bucket: &str) -> String {
    format!("projects/_/buckets/{bucket}")
}

pub(super) fn encode_rapid_frame(position: &CommitPosition, payload: &[u8]) -> Result<Vec<u8>> {
    let fingerprint = RecordFingerprint::new(payload)?;
    let mut frame = Vec::with_capacity(
        RAPID_FRAME_HEADER_BYTES
            .checked_add(payload.len())
            .context("Rapid frame size overflow")?,
    );
    frame.extend_from_slice(RAPID_FRAME_MAGIC);
    frame.extend_from_slice(&position.epoch.to_be_bytes());
    frame.extend_from_slice(&position.log_generation.to_be_bytes());
    frame.extend_from_slice(&position.max_txid.to_be_bytes());
    frame.extend_from_slice(&fingerprint.byte_len.to_be_bytes());
    frame.extend_from_slice(&fingerprint.crc32c.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn parse_rapid_log(
    bytes: Vec<u8>,
    expected_epoch: u64,
    expected_generation: u64,
    key: &str,
) -> Result<ParsedRapidLog> {
    let bytes = Arc::new(bytes);
    let mut records = HashMap::new();
    let mut offset = 0usize;
    let mut previous_txid = 0u64;

    loop {
        let remaining = bytes.len() - offset;
        if remaining == 0 || remaining < RAPID_FRAME_HEADER_BYTES {
            break;
        }
        ensure!(
            &bytes[offset..offset + RAPID_FRAME_MAGIC.len()] == RAPID_FRAME_MAGIC,
            "invalid Rapid frame magic at byte {offset} in {key}"
        );
        let epoch = read_u64(&bytes, offset + 8);
        let generation = read_u64(&bytes, offset + 16);
        let max_txid = read_u64(&bytes, offset + 24);
        let payload_len = read_u64(&bytes, offset + 32);
        let expected_crc = read_u32(&bytes, offset + 40);
        ensure!(
            epoch == expected_epoch,
            "Rapid frame epoch {epoch} does not match log epoch {expected_epoch} in {key}"
        );
        ensure!(
            generation == expected_generation,
            "Rapid frame generation {generation} does not match log generation {expected_generation} in {key}"
        );
        ensure!(
            max_txid > previous_txid,
            "Rapid commit TXIDs are not strictly increasing in {key}"
        );
        let payload_len = usize::try_from(payload_len).context("Rapid frame is too large")?;
        let payload_start = offset + RAPID_FRAME_HEADER_BYTES;
        let Some(payload_end) = payload_start.checked_add(payload_len) else {
            bail!("Rapid frame length overflow in {key}");
        };
        if payload_end > bytes.len() {
            break;
        }
        let actual_crc = crc32c::crc32c(&bytes[payload_start..payload_end]);
        ensure!(
            actual_crc == expected_crc,
            "Rapid frame checksum mismatch for TXID {max_txid} in {key}"
        );
        ensure!(
            records
                .insert(
                    max_txid,
                    RapidRecord {
                        payload: payload_start..payload_end,
                        fingerprint: RecordFingerprint {
                            byte_len: u64::try_from(payload_len)?,
                            crc32c: actual_crc,
                        },
                    },
                )
                .is_none(),
            "duplicate Rapid commit TXID {max_txid} in {key}"
        );
        previous_txid = max_txid;
        offset = payload_end;
    }

    Ok(ParsedRapidLog {
        bytes,
        records,
        complete_len: offset,
    })
}

pub(super) fn decode_commit_from_log(
    bytes: &[u8],
    position: &CommitPosition,
    key: &str,
) -> Result<Option<Vec<u8>>> {
    let parsed = parse_rapid_log(bytes.to_vec(), position.epoch, position.log_generation, key)?;
    ensure!(
        parsed.trailing_bytes().is_empty(),
        "archived Rapid log contains an incomplete frame: {key}"
    );
    Ok(parsed.payload(position.max_txid))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated Rapid u64 field"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("validated Rapid u32 field"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_rapid_zonal_bucket_metadata() -> Result<()> {
        let rapid = google_cloud_storage::model::Bucket::new()
            .set_storage_class("RAPID")
            .set_location_type("zone")
            .set_location("US-EAST4-A");
        validate_rapid_bucket("rapid-us-east", &rapid)?;

        let standard = google_cloud_storage::model::Bucket::new()
            .set_storage_class("STANDARD")
            .set_location_type("multi-region")
            .set_location("US");
        assert!(
            validate_rapid_bucket("not-rapid", &standard)
                .unwrap_err()
                .to_string()
                .contains("must use RAPID")
        );
        Ok(())
    }

    fn rapid_id(object: impl Into<String>, generation: u64) -> RapidLogId {
        RapidLogId {
            object: object.into(),
            epoch: 1,
            generation,
        }
    }

    #[test]
    fn writer_cache_is_bounded_and_evicts_least_recently_used() {
        let mut cache = RapidWriterCache::default();
        for index in 0..RAPID_WRITER_LOGS {
            cache.get_or_insert(&rapid_id(format!("object-{index}"), 0));
        }

        let recently_used = rapid_id("object-0", 0);
        cache.get_or_insert(&recently_used);
        cache.get_or_insert(&rapid_id("new-object", 0));

        assert_eq!(cache.entries.len(), RAPID_WRITER_LOGS);
        assert!(cache.entries.contains_key(&recently_used));
        assert!(!cache.entries.contains_key(&rapid_id("object-1", 0)));
    }

    #[test]
    fn writer_cache_replaces_an_older_generation_for_the_same_object() {
        let mut cache = RapidWriterCache::default();
        let old = rapid_id("object", 0);
        let current = rapid_id("object", 1);

        cache.get_or_insert(&old);
        cache.get_or_insert(&current);

        assert_eq!(cache.entries.len(), 1);
        assert!(!cache.entries.contains_key(&old));
        assert!(cache.entries.contains_key(&current));
    }

    #[test]
    fn frames_round_trip_and_retain_commit_boundaries() -> Result<()> {
        let first = CommitPosition {
            epoch: 7,
            log_generation: 0,
            max_txid: 3,
        };
        let second = CommitPosition {
            epoch: 7,
            log_generation: 0,
            max_txid: 4,
        };
        let mut bytes = encode_rapid_frame(&first, b"first-bundle")?;
        bytes.extend_from_slice(&encode_rapid_frame(&second, b"second-bundle")?);

        let parsed = parse_rapid_log(bytes, 7, 0, "test-log")?;
        assert_eq!(
            parsed.payload(3).as_deref(),
            Some(b"first-bundle".as_slice())
        );
        assert_eq!(
            parsed.payload(4).as_deref(),
            Some(b"second-bundle".as_slice())
        );
        assert!(parsed.trailing_bytes().is_empty());
        Ok(())
    }

    #[test]
    fn partial_last_frame_is_available_for_exact_resume() -> Result<()> {
        let position = CommitPosition {
            epoch: 2,
            log_generation: 3,
            max_txid: 9,
        };
        let frame = encode_rapid_frame(&position, b"commit-bundle")?;
        let partial = frame[..frame.len() - 3].to_vec();
        let parsed = parse_rapid_log(partial.clone(), 2, 3, "test-log")?;

        assert!(parsed.records.is_empty());
        assert_eq!(parsed.trailing_bytes(), partial);
        assert!(frame.starts_with(&parsed.trailing_bytes()));
        Ok(())
    }

    #[test]
    fn corrupt_complete_frame_is_rejected() -> Result<()> {
        let position = CommitPosition {
            epoch: 1,
            log_generation: 0,
            max_txid: 1,
        };
        let mut frame = encode_rapid_frame(&position, b"commit-bundle")?;
        *frame.last_mut().expect("frame byte") ^= 0xff;

        let error = parse_rapid_log(frame, 1, 0, "test-log").expect_err("checksum must fail");
        assert!(error.to_string().contains("checksum mismatch"));
        Ok(())
    }
}
