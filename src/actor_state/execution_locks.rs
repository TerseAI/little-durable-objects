use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use anyhow::Result;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use super::ActorStorageKey;

pub struct ActorExecutionLocks {
    locks: Mutex<HashMap<ActorStorageKey, Weak<AsyncMutex<()>>>>,
}

impl ActorExecutionLocks {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }

    pub async fn acquire(&self, storage_key: &ActorStorageKey) -> Result<OwnedMutexGuard<()>> {
        let execution = {
            let mut locks = self
                .locks
                .lock()
                .map_err(|_| anyhow::anyhow!("actor execution-lock map poisoned"))?;

            // The map holds only weak references, so inactive actor keys do not retain
            // mutexes forever. Clear their stale entries while the map is already locked.
            locks.retain(|_, execution| execution.strong_count() > 0);

            match locks.get(storage_key).and_then(Weak::upgrade) {
                Some(execution) => execution,
                None => {
                    let execution = Arc::new(AsyncMutex::new(()));
                    locks.insert(storage_key.clone(), Arc::downgrade(&execution));
                    execution
                }
            }
        };

        Ok(execution.lock_owned().await)
    }
}
