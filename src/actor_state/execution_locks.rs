use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use anyhow::Result;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use super::ActorStorageKey;

const MAX_ADMITTED_INVOCATIONS_PER_ACTOR: usize = 33;

pub struct ActorExecutionLocks {
    locks: Mutex<HashMap<ActorStorageKey, Weak<ActorExecutionGate>>>,
}

pub enum ActorExecutionAdmission {
    Acquired(ActorExecutionGuard),
    Full,
}

pub struct ActorExecutionGuard {
    _gate: Arc<ActorExecutionGate>,
    _execution: OwnedMutexGuard<()>,
    _admission: OwnedSemaphorePermit,
}

struct ActorExecutionGate {
    execution: Arc<AsyncMutex<()>>,
    admissions: Arc<Semaphore>,
}

impl ActorExecutionLocks {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }

    pub async fn admit(&self, storage_key: &ActorStorageKey) -> Result<ActorExecutionAdmission> {
        let gate = self.gate(storage_key)?;
        let admission = match gate.admissions.clone().try_acquire_owned() {
            Ok(admission) => admission,
            Err(_) => return Ok(ActorExecutionAdmission::Full),
        };
        let execution = gate.execution.clone().lock_owned().await;
        Ok(ActorExecutionAdmission::Acquired(ActorExecutionGuard {
            _gate: gate,
            _execution: execution,
            _admission: admission,
        }))
    }

    fn gate(&self, storage_key: &ActorStorageKey) -> Result<Arc<ActorExecutionGate>> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| anyhow::anyhow!("actor execution-lock map poisoned"))?;

        locks.retain(|_, gate| gate.strong_count() > 0);
        Ok(match locks.get(storage_key).and_then(Weak::upgrade) {
            Some(gate) => gate,
            None => {
                let gate = Arc::new(ActorExecutionGate {
                    execution: Arc::new(AsyncMutex::new(())),
                    admissions: Arc::new(Semaphore::new(MAX_ADMITTED_INVOCATIONS_PER_ACTOR)),
                });
                locks.insert(storage_key.clone(), Arc::downgrade(&gate));
                gate
            }
        })
    }
}
