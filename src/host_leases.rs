//! Authoritative storage for actor-host leases.

mod local;
mod postgres;

use crate::host::HostId;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use self::{local::LocalHostLeaseStore, postgres::PostgresHostLeaseStore};

pub const MAX_HOST_LEASE_DURATION_MS: u64 = 60_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLease {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLeaseRequest {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub duration_ms: u64,
}

impl HostLeaseRequest {
    pub fn validate_duration(&self) -> Result<()> {
        ensure!(self.duration_ms > 0, "host lease duration must be positive");
        ensure!(
            self.duration_ms <= MAX_HOST_LEASE_DURATION_MS,
            "host lease duration must not exceed {MAX_HOST_LEASE_DURATION_MS}ms"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLeaseStatus {
    pub lease: Option<HostLease>,
    #[serde(rename = "registry_now_ms")]
    pub store_now_ms: u64,
}

impl HostLeaseStatus {
    pub fn is_active(&self) -> bool {
        self.lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at_ms > self.store_now_ms)
    }
}

#[async_trait]
pub trait HostLeaseStore: Send + Sync {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease>;

    async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus>;

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_status_uses_the_store_clock() {
        let lease = HostLease {
            id: HostId::new("node-a"),
            session_id: "session-a".into(),
            route: "node-a".into(),
            expires_at_ms: 1_000,
        };

        let live = HostLeaseStatus {
            lease: Some(lease.clone()),
            store_now_ms: 999,
        };
        let expired = HostLeaseStatus {
            lease: Some(lease),
            store_now_ms: 1_000,
        };
        let absent = HostLeaseStatus {
            lease: None,
            store_now_ms: 0,
        };

        assert!(live.is_active());
        assert!(!expired.is_active());
        assert!(!absent.is_active());
        let encoded = serde_json::to_value(live).expect("serialize lease status");
        assert_eq!(encoded["registry_now_ms"], 999);
        assert!(encoded.get("store_now_ms").is_none());
    }
}
