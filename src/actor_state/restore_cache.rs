use std::{collections::HashMap, sync::Mutex};

use anyhow::{Result, anyhow};

use super::ActorStorageKey;

pub struct ActorRestoreCache {
    epochs: Mutex<HashMap<ActorStorageKey, u64>>,
}

impl ActorRestoreCache {
    pub fn new() -> Self {
        Self {
            epochs: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_ready(&self, storage_key: &ActorStorageKey, epoch: u64) -> Result<bool> {
        Ok(self
            .epochs
            .lock()
            .map_err(|_| anyhow!("actor restore cache lock poisoned"))?
            .get(storage_key)
            .is_some_and(|ready| *ready == epoch))
    }

    pub fn mark_ready(&self, storage_key: &ActorStorageKey, epoch: u64) -> Result<()> {
        self.epochs
            .lock()
            .map_err(|_| anyhow!("actor restore cache lock poisoned"))?
            .insert(storage_key.clone(), epoch);

        Ok(())
    }

    pub fn forget(&self, storage_key: &ActorStorageKey) -> Result<()> {
        self.epochs
            .lock()
            .map_err(|_| anyhow!("actor restore cache lock poisoned"))?
            .remove(storage_key);

        Ok(())
    }
}
