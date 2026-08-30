//! Last host lease confirmed by this process, used for local self-fencing.

use std::sync::atomic::{AtomicU64, Ordering};

use super::HostId;

pub(crate) struct ConfirmedLeaseState {
    host: HostId,
    lease_valid_until_ms: AtomicU64,
}

impl ConfirmedLeaseState {
    pub(crate) fn new(host: HostId) -> Self {
        Self {
            host,
            lease_valid_until_ms: AtomicU64::new(0),
        }
    }

    /// Record a confirmed window only after its lease-store write succeeds.
    pub(crate) fn record_renewal(&self, valid_until_ms: u64) {
        self.lease_valid_until_ms
            .store(valid_until_ms, Ordering::SeqCst);
    }

    /// `None` means this process has not locally confirmed a lease and the durable
    /// lease store remains the fallback. `Some(false)` is an explicit local fence.
    pub(crate) fn lease_is_active(&self, host: &HostId, now_ms: u64) -> Option<bool> {
        if host != &self.host {
            return None;
        }

        let valid_until_ms = self.lease_valid_until_ms.load(Ordering::SeqCst);
        (valid_until_ms != 0).then_some(valid_until_ms > now_ms)
    }
}
