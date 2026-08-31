use std::time::Duration;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;

use crate::actor_state::ActorStorageKey;

const MAX_NAMESPACE_ID_BYTES: usize = 96;
const MAX_ACTOR_TYPE_BYTES: usize = 48;
const MAX_ACTOR_ID_BYTES: usize = 128;
const MAX_METHOD_BYTES: usize = 128;
const MAX_ACTOR_INVOCATION_TIMEOUT_MS: u64 = i32::MAX as u64;

/// The authenticated tenant boundary shared by a collection of actors.
#[derive(Clone, Debug)]
pub struct ActorScope {
    pub namespace_id: String,
}

impl ActorScope {
    pub fn validate(&self) -> Result<()> {
        validate_component("namespace ID", &self.namespace_id, MAX_NAMESPACE_ID_BYTES)
    }

    pub fn contains(&self, actor: &ActorKey) -> bool {
        actor.namespace_id == self.namespace_id
    }
}

/// The namespace-scoped identity of one actor instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ActorKey {
    pub namespace_id: String,
    pub actor_type: String,
    pub actor_id: String,
}

impl ActorKey {
    pub fn validate(&self) -> Result<()> {
        validate_component("namespace ID", &self.namespace_id, MAX_NAMESPACE_ID_BYTES)?;
        validate_component("actor type", &self.actor_type, MAX_ACTOR_TYPE_BYTES)?;
        validate_component("actor ID", &self.actor_id, MAX_ACTOR_ID_BYTES)?;
        self.storage_key().validate()
    }

    /// Stable storage identity. Components are deliberately restricted instead of
    /// escaped so the mapping is readable in SQLite, PostgreSQL, and Rapid keys.
    pub fn storage_key(&self) -> ActorStorageKey {
        ActorStorageKey::new(format!(
            "object.v1.{}.{}.{}",
            self.namespace_id, self.actor_type, self.actor_id
        ))
    }
}

fn validate_component(name: &str, value: &str, max_bytes: usize) -> Result<()> {
    ensure!(!value.is_empty(), "{name} must not be empty");
    ensure!(
        value.len() <= max_bytes,
        "{name} must be at most {max_bytes} bytes"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "{name} may contain only ASCII letters, digits, '.', '-', and '_'"
    );
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActorInvocation {
    /// Actor-scoped idempotency key. Retries must preserve this value.
    pub request_id: String,
    pub actor: ActorKey,
    pub method: String,
    pub args: Vec<Value>,
    /// Remaining caller wait budget. Execution may continue after this caller's
    /// deadline expires so the actor gate stays held until the task terminates.
    pub timeout_ms: u64,
}

impl ActorInvocation {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.request_id.is_empty(), "request ID must not be empty");
        ensure!(
            self.request_id.len() <= 255,
            "request ID must be at most 255 bytes"
        );
        self.actor.validate()?;
        validate_component("actor method", &self.method, MAX_METHOD_BYTES)?;
        ensure!(self.timeout_ms > 0, "actor timeout must be positive");
        ensure!(
            self.timeout_ms <= MAX_ACTOR_INVOCATION_TIMEOUT_MS,
            "actor timeout must be at most {MAX_ACTOR_INVOCATION_TIMEOUT_MS}ms"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ActorInvocationDeadline {
    at: Instant,
}

impl ActorInvocationDeadline {
    pub(crate) fn from_timeout_ms(timeout_ms: u64) -> Self {
        debug_assert!((1..=MAX_ACTOR_INVOCATION_TIMEOUT_MS).contains(&timeout_ms));
        let at = Instant::now()
            .checked_add(Duration::from_millis(timeout_ms))
            .expect("validated actor timeout must fit the monotonic clock range");
        Self { at }
    }

    pub(crate) fn at(self) -> Instant {
        self.at
    }

    pub(crate) fn remaining_ms(self) -> u64 {
        let remaining = self.at.saturating_duration_since(Instant::now());
        u64::try_from(remaining.as_nanos().div_ceil(1_000_000))
            .expect("validated actor deadline must fit in milliseconds")
    }

    pub(crate) fn is_elapsed(self) -> bool {
        Instant::now() >= self.at
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorInvocationFailure {
    pub code: String,
    pub message: String,
}

impl ActorInvocationFailure {
    pub(crate) fn cancelled_before_execution(reason: &str) -> Self {
        Self {
            code: "cancelled".into(),
            message: format!("actor invocation was cancelled before execution because {reason}"),
        }
    }

    pub(crate) fn cancelled_during_execution(reason: &str) -> Self {
        Self {
            code: "cancelled".into(),
            message: format!(
                "actor invocation was cancelled during execution because {reason}; the state transition was rolled back"
            ),
        }
    }

    pub(crate) fn deadline_exceeded_before_execution() -> Self {
        Self {
            code: "deadline_exceeded".into(),
            message: "actor invocation deadline exceeded before execution started".into(),
        }
    }

    pub(crate) fn resource_exhausted_before_execution() -> Self {
        Self {
            code: "resource_exhausted".into(),
            message: "actor already has 32 queued invocations".into(),
        }
    }

    pub(crate) fn deadline_exceeded_while_waiting() -> Self {
        Self {
            code: "deadline_exceeded".into(),
            message: "actor invocation deadline exceeded; execution may still complete".into(),
        }
    }

    pub(crate) fn deadline_exceeded_during_execution() -> Self {
        Self {
            code: "deadline_exceeded".into(),
            message: "actor invocation deadline exceeded during execution; the state transition was rolled back".into(),
        }
    }

    pub(crate) fn outcome_unknown_after_execution() -> Self {
        Self {
            code: "outcome_unknown".into(),
            message: "actor execution completed, but its durable publication outcome could not be confirmed".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ActorExecutionResult {
    Completed { result: Value },
    Failed { failure: ActorInvocationFailure },
    Reroute,
    HostUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_a_tenant_scoped_actor_to_one_safe_object_id() {
        let key = ActorKey {
            namespace_id: "namespace-1".into(),
            actor_type: "counter".into(),
            actor_id: "customer.123".into(),
        };

        key.validate().expect("actor key");
        assert_eq!(
            key.storage_key().as_str(),
            "object.v1.namespace-1.counter.customer.123"
        );
    }

    #[test]
    fn rejects_components_that_can_reshape_storage_paths() {
        let mut key = ActorKey {
            namespace_id: "namespace-1".into(),
            actor_type: "counter".into(),
            actor_id: "../other".into(),
        };
        assert!(key.validate().is_err());

        key.actor_id = "valid".into();
        key.actor_type = "counter/type".into();
        assert!(key.validate().is_err());
    }
}
