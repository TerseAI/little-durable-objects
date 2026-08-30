//! Actor-host execution paired with its renewable host lease.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::sync::watch;
use tracing::info;

use super::{
    ActorDrainReason, ActorHost, ActorHostDependencies, ConfirmedLeaseState, HostEndpoint,
    HostLeaseMaintainer, LeaseRenewalTask,
};

pub(crate) const HOST_ACTOR_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const HOST_UNREGISTER_TIMEOUT: Duration = Duration::from_secs(2);

/// A running actor host plus the renewal task that self-fences it when its registered
/// lease can no longer be kept alive.
pub(crate) struct LeasedActorHost {
    host: Arc<ActorHost>,
    lease: Arc<HostLeaseMaintainer>,
    renewal: LeaseRenewalTask,
}

impl LeasedActorHost {
    pub(crate) async fn start(
        endpoint: HostEndpoint,
        session_id: String,
        dependencies: ActorHostDependencies,
        lease_duration: Duration,
        renew_every: Duration,
    ) -> Result<Self> {
        info!(
            host_id = %endpoint.id,
            route = %endpoint.route,
            lease_ms = lease_duration.as_millis(),
            renew_ms = renew_every.as_millis(),
            "starting leased actor host"
        );
        let confirmed_lease = Arc::new(ConfirmedLeaseState::new(endpoint.id.clone()));
        let lease = Arc::new(
            HostLeaseMaintainer::new(
                endpoint.clone(),
                session_id,
                dependencies.lease_store(),
                dependencies.clock(),
                lease_duration,
                renew_every,
            )?
            .with_confirmed_lease(confirmed_lease.clone()),
        );
        let renewal = lease.clone().start().await?;
        let dependencies = dependencies.with_confirmed_lease(confirmed_lease);

        Ok(Self {
            host: Arc::new(ActorHost::new(endpoint, dependencies)),
            lease,
            renewal,
        })
    }

    pub(crate) fn host(&self) -> Arc<ActorHost> {
        self.host.clone()
    }

    pub(crate) fn consecutive_lease_failures(&self) -> u64 {
        self.lease.consecutive_failures()
    }

    pub(crate) fn lease_lost(&self) -> watch::Receiver<bool> {
        self.renewal.lease_lost()
    }

    pub(crate) async fn drain(&self, reason: ActorDrainReason) -> Result<()> {
        self.host
            .drain_actor_invocations(reason, HOST_ACTOR_DRAIN_TIMEOUT)
            .await
    }

    pub(crate) async fn shutdown(self) -> Result<()> {
        let renewal_result = self.renewal.shutdown().await;
        let unregister_result = match tokio::time::timeout(
            HOST_UNREGISTER_TIMEOUT,
            self.lease.unregister(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "host lease unregister did not finish within {}ms; the existing lease will expire naturally",
                HOST_UNREGISTER_TIMEOUT.as_millis()
            )),
        };
        renewal_result?;
        unregister_result
    }
}

#[cfg(test)]
mod tests;
