use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    actor_state::{ActorDatabaseStore, ActorStorageKey, SqliteActorDatabase},
    ltx::{LtxSegment, SqliteLtxCapture},
};

/// The durable LTX segments produced by one capture pass.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CapturedActorChanges {
    segments: Vec<LtxSegment>,
}

impl CapturedActorChanges {
    pub fn new(segments: Vec<LtxSegment>) -> Self {
        Self { segments }
    }

    pub fn segments(&self) -> &[LtxSegment] {
        &self.segments
    }

    pub(crate) fn segments_mut(&mut self) -> &mut [LtxSegment] {
        &mut self.segments
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }
}

#[async_trait]
pub trait ActorChangeCapture: Send + Sync {
    /// Prepare capture before the object performs its first write.
    ///
    /// Implementations that do not require preparation can use this default. The local
    /// SQLite implementation uses it to enable WAL mode and retain its capture state.
    async fn prepare(&self, _object: &ActorStorageKey) -> Result<()> {
        Ok(())
    }

    /// Drop any process-local capture state before replacing an object's SQLite cache.
    async fn reset(&self, _object: &ActorStorageKey) -> Result<()> {
        Ok(())
    }

    async fn capture(&self, object: &ActorStorageKey) -> Result<CapturedActorChanges>;

    /// Recycle WAL frames that are covered by the canonical durable manifest.
    /// Implementations without a WAL can use this default no-op.
    async fn checkpoint_durable(
        &self,
        _object: &ActorStorageKey,
        _durable_txid: u64,
    ) -> Result<()> {
        Ok(())
    }
}

/// Captures each object's SQLite WAL into durable local LTX segments.
pub struct LocalActorChangeCapture {
    databases: ActorDatabaseStore,
    actors: Mutex<HashMap<ActorStorageKey, Arc<ActorCaptureState>>>,
}

struct ActorCaptureState {
    // Keep a connection open for the lifetime of capture. Closing the last SQLite
    // connection checkpoints the WAL, which can discard frames before the next sync.
    database: SqliteActorDatabase,
    capture: SqliteLtxCapture,
}

impl LocalActorChangeCapture {
    pub fn new(database_root: impl Into<PathBuf>) -> Self {
        Self {
            databases: ActorDatabaseStore::new(database_root),
            actors: Mutex::new(HashMap::new()),
        }
    }

    fn actor(&self, storage_key: &ActorStorageKey) -> Result<Arc<ActorCaptureState>> {
        let mut actors = self
            .actors
            .lock()
            .map_err(|_| anyhow!("local LTX capture lock poisoned"))?;

        if let Some(capture) = actors.get(storage_key) {
            return Ok(Arc::clone(capture));
        }

        let database = self.databases.open(storage_key)?;
        let capture = SqliteLtxCapture::attach(&database)?;
        let captured = Arc::new(ActorCaptureState { database, capture });

        actors.insert(storage_key.clone(), Arc::clone(&captured));

        Ok(captured)
    }
}

#[async_trait]
impl ActorChangeCapture for LocalActorChangeCapture {
    async fn prepare(&self, object: &ActorStorageKey) -> Result<()> {
        self.actor(object)?;
        debug!(object = %object, "prepared local LTX capture");

        Ok(())
    }

    async fn reset(&self, object: &ActorStorageKey) -> Result<()> {
        self.actors
            .lock()
            .map_err(|_| anyhow!("local LTX capture lock poisoned"))?
            .remove(object);
        debug!(object = %object, "reset local LTX capture state");

        Ok(())
    }

    async fn capture(&self, object: &ActorStorageKey) -> Result<CapturedActorChanges> {
        let captured = CapturedActorChanges::new(self.actor(object)?.capture.sync()?);
        debug!(
            object = %object,
            segment_count = captured.len(),
            max_txid = ?captured.segments().last().map(|segment| segment.max_txid),
            "completed local LTX capture"
        );

        Ok(captured)
    }

    async fn checkpoint_durable(&self, object: &ActorStorageKey, durable_txid: u64) -> Result<()> {
        let captured = self.actor(object)?;
        let checkpointed = captured
            .capture
            .checkpoint_durable(&captured.database, durable_txid)?;
        debug!(
            object = %object,
            durable_txid,
            checkpointed,
            "checkpointed remotely durable SQLite WAL frames"
        );

        Ok(())
    }
}
