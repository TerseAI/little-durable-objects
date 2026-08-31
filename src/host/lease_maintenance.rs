use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use tracing::{debug, info, warn};

use crate::{
    clock::Clock,
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
};

use super::HostEndpoint;

const LEASE_RENEWAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct HostLeaseMaintainer {
    endpoint: HostEndpoint,
    session_id: String,
    store: Arc<dyn HostLeaseRegistry>,
    clock: Arc<dyn Clock>,
    lease_duration_ms: u64,
    renew_every: Duration,
    consecutive_failures: AtomicU64,
}

impl HostLeaseMaintainer {
    pub(crate) fn new(
        endpoint: HostEndpoint,
        session_id: String,
        store: Arc<dyn HostLeaseRegistry>,
        clock: Arc<dyn Clock>,
        lease_duration: Duration,
        renew_every: Duration,
    ) -> Result<Self> {
        let lease_duration_ms = u64::try_from(lease_duration.as_millis())?;
        ensure!(
            lease_duration_ms > 0,
            "host lease duration must be positive"
        );
        ensure!(
            !renew_every.is_zero(),
            "host lease renewal interval must be positive"
        );
        ensure!(
            renew_every < lease_duration,
            "host lease renewal interval must be shorter than its duration"
        );
        ensure!(!session_id.is_empty(), "host session ID must not be empty");

        Ok(Self {
            endpoint,
            session_id,
            store,
            clock,
            lease_duration_ms,
            renew_every,
            consecutive_failures: AtomicU64::new(0),
        })
    }

    async fn renew_once_with_deadline(&self) -> Result<ConfirmedHostLease> {
        // The store stamps the durable expiration with its own clock. The locally
        // confirmed window is anchored to this host's clock, sampled before the
        // store round trip, so it always lapses at or before the stamped expiry
        // regardless of the absolute offset between the two clocks.
        let local_now_ms = self.clock.now_ms()?;
        let local_valid_until_ms = local_now_ms
            .checked_add(self.lease_duration_ms)
            .ok_or_else(|| anyhow::anyhow!("host lease expiration overflow"))?;
        let request = HostLeaseRequest {
            id: self.endpoint.id.clone(),
            session_id: self.session_id.clone(),
            route: self.endpoint.route.clone(),
            duration_ms: self.lease_duration_ms,
        };

        let lease = self.store.register(&request).await?;
        self.consecutive_failures.store(0, Ordering::Relaxed);

        debug!(
            host_id = %lease.id,
            route = %lease.route,
            expires_at_ms = lease.expires_at_ms,
            "host lease renewed"
        );

        // Anchor the Tokio timer before sampling the same monotonic clock used
        // above. This makes the process fence no later than the locally confirmed
        // lease window, even when a renewal RPC consumed most of that window.
        let deadline_anchor = Instant::now();
        let remaining_ms = local_valid_until_ms.saturating_sub(self.clock.now_ms()?);
        ensure!(
            remaining_ms > 0,
            "host lease expired before its registration response arrived"
        );
        let local_deadline = deadline_anchor
            .checked_add(Duration::from_millis(remaining_ms))
            .ok_or_else(|| anyhow::anyhow!("local host lease deadline overflow"))?;

        Ok(ConfirmedHostLease {
            lease,
            local_deadline,
        })
    }

    pub(crate) async fn unregister(&self) -> Result<()> {
        self.store
            .unregister(&self.endpoint.id, &self.session_id)
            .await?;
        info!(host_id = %self.endpoint.id, "host lease unregistered");
        Ok(())
    }

    pub(crate) async fn start(self: Arc<Self>) -> Result<LeaseRenewalTask> {
        let initial = self.renew_once_with_deadline().await?;
        info!(
            host_id = %initial.lease.id,
            route = %initial.lease.route,
            expires_at_ms = initial.lease.expires_at_ms,
            "host lease registered"
        );
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let (lease_lost_tx, lease_lost) = watch::channel(false);
        let manager = self.clone();
        let task = tokio::spawn(async move {
            let mut local_deadline = initial.local_deadline;
            loop {
                let renewal_due = tokio::time::sleep(manager.renew_every);
                tokio::pin!(renewal_due);
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    _ = tokio::time::sleep_until(local_deadline) => {
                        warn!(
                            host_id = %manager.endpoint.id,
                            "locally confirmed host lease expired; permanently self-fencing this process"
                        );
                        let _ = lease_lost_tx.send(true);
                        break;
                    }
                    _ = &mut renewal_due => {}
                }

                let renewal = manager.renew_once_with_deadline();
                tokio::pin!(renewal);
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    _ = tokio::time::sleep_until(local_deadline) => {
                        warn!(
                            host_id = %manager.endpoint.id,
                            "host lease expired while its renewal request was still pending; permanently self-fencing this process"
                        );
                        let _ = lease_lost_tx.send(true);
                        break;
                    }
                    result = &mut renewal => {
                        match result {
                            Ok(confirmed) => local_deadline = confirmed.local_deadline,
                            Err(error) => {
                                warn!(
                                    host_id = %manager.endpoint.id,
                                    error = %format!("{error:#}"),
                                    "host lease renewal failed; ownership checks will self-fence after expiry"
                                );
                                let _ = manager.consecutive_failures.fetch_update(
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                    |failures| Some(failures.saturating_add(1)),
                                );
                            }
                        }
                    }
                }
            }
        });

        Ok(LeaseRenewalTask {
            shutdown: Some(shutdown_tx),
            task,
            lease_lost,
        })
    }
}

struct ConfirmedHostLease {
    lease: HostLease,
    local_deadline: Instant,
}

pub(crate) struct LeaseRenewalTask {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    lease_lost: watch::Receiver<bool>,
}

impl LeaseRenewalTask {
    pub(crate) fn lease_lost(&self) -> watch::Receiver<bool> {
        self.lease_lost.clone()
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match tokio::time::timeout(LEASE_RENEWAL_SHUTDOWN_TIMEOUT, &mut self.task).await {
            Ok(result) => result?,
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                anyhow::bail!(
                    "host lease renewal did not stop within {}ms",
                    LEASE_RENEWAL_SHUTDOWN_TIMEOUT.as_millis()
                );
            }
        }
        info!("host lease renewal stopped");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        clock::{Clock, SystemClock},
        host::HostId,
        host_leases::HostLease,
    };
    use async_trait::async_trait;
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    struct ManualClock(AtomicU64);

    impl ManualClock {
        fn new(now_ms: u64) -> Self {
            Self(AtomicU64::new(now_ms))
        }

        fn set(&self, now_ms: u64) {
            self.0.store(now_ms, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_ms(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    struct FlakyLeaseStore {
        calls: AtomicUsize,
        lease: Mutex<Option<HostLease>>,
        changed: Notify,
        clock: Arc<ManualClock>,
    }

    struct HangingLeaseRenewalStore {
        calls: AtomicUsize,
        lease: Mutex<Option<HostLease>>,
    }

    #[async_trait]
    impl HostLeaseRegistry for HangingLeaseRenewalStore {
        async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
            if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
                return std::future::pending().await;
            }
            let lease = HostLease {
                id: request.id.clone(),
                session_id: request.session_id.clone(),
                route: request.route.clone(),
                expires_at_ms: SystemClock.now_ms()?.saturating_add(request.duration_ms),
            };
            *self.lease.lock().expect("test store lock") = Some(lease.clone());
            Ok(lease)
        }

        async fn unregister(&self, _id: &HostId, _session_id: &str) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl HostLeaseRegistry for FlakyLeaseStore {
        async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.changed.notify_one();
            if call == 2 {
                anyhow::bail!("temporary store failure");
            }
            let lease = HostLease {
                id: request.id.clone(),
                session_id: request.session_id.clone(),
                route: request.route.clone(),
                expires_at_ms: self.clock.now_ms()?.saturating_add(request.duration_ms),
            };
            *self.lease.lock().expect("test store lock") = Some(lease.clone());

            Ok(lease)
        }

        async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()> {
            let mut lease = self.lease.lock().expect("test store lock");
            if lease
                .as_ref()
                .is_some_and(|lease| &lease.id == id && lease.session_id == session_id)
            {
                *lease = None;
            }
            Ok(())
        }
    }

    impl FlakyLeaseStore {
        async fn get(&self, id: &HostId) -> Result<Option<HostLease>> {
            Ok(self
                .lease
                .lock()
                .expect("test store lock")
                .clone()
                .filter(|lease| &lease.id == id))
        }
    }

    async fn wait_for_calls(store: &FlakyLeaseStore, expected: usize) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.calls.load(Ordering::SeqCst) < expected {
                store.changed.notified().await;
            }
        })
        .await?;

        Ok(())
    }

    // ========================================================================
    // Host identity and lease renewal
    // ========================================================================

    #[test]
    fn new_hosts_receive_unique_session_ids() {
        let first = HostEndpoint {
            id: HostId::new(uuid::Uuid::new_v4().to_string()),
            route: "sandbox-route".into(),
        };
        let second = HostEndpoint {
            id: HostId::new(uuid::Uuid::new_v4().to_string()),
            route: "sandbox-route".into(),
        };

        assert_ne!(first.id, second.id);
        assert_eq!(first.route, "sandbox-route");
    }

    #[tokio::test]
    async fn registers_immediately_retries_failure_and_stops_cleanly() -> Result<()> {
        let clock = Arc::new(ManualClock::new(1_000));
        let store = Arc::new(FlakyLeaseStore {
            calls: AtomicUsize::new(0),
            lease: Mutex::new(None),
            changed: Notify::new(),
            clock: clock.clone(),
        });
        let node = HostEndpoint {
            id: HostId::new("node-a"),
            route: "sandbox-session-a".into(),
        };
        let manager = Arc::new(HostLeaseMaintainer::new(
            node.clone(),
            "session-a".into(),
            store.clone(),
            clock.clone(),
            Duration::from_millis(1_000),
            Duration::from_millis(10),
        )?);

        let renewal = manager.clone().start().await?;
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .get(&node.id)
                .await?
                .expect("initial lease")
                .expires_at_ms,
            2_000
        );

        clock.set(1_500);
        wait_for_calls(&store, 3).await?;
        assert_eq!(
            store
                .get(&node.id)
                .await?
                .expect("renewed lease")
                .expires_at_ms,
            2_500
        );
        assert_eq!(manager.consecutive_failures.load(Ordering::Relaxed), 0);

        renewal.shutdown().await?;
        let calls_after_shutdown = store.calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(store.calls.load(Ordering::SeqCst), calls_after_shutdown);

        Ok(())
    }

    #[tokio::test]
    async fn pending_renewal_cannot_outlive_the_confirmed_lease_window() -> Result<()> {
        let store = Arc::new(HangingLeaseRenewalStore {
            calls: AtomicUsize::new(0),
            lease: Mutex::new(None),
        });
        let manager = Arc::new(HostLeaseMaintainer::new(
            HostEndpoint {
                id: HostId::new("node-a"),
                route: "sandbox-session-a".into(),
            },
            "session-a".into(),
            store.clone(),
            Arc::new(SystemClock),
            Duration::from_millis(100),
            Duration::from_millis(10),
        )?);

        let renewal = manager.start().await?;
        let mut lease_lost = renewal.lease_lost();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !*lease_lost.borrow() {
                lease_lost.changed().await?;
            }
            Ok::<(), watch::error::RecvError>(())
        })
        .await??;

        assert!(*lease_lost.borrow());
        assert_eq!(store.calls.load(Ordering::SeqCst), 2);
        renewal.shutdown().await?;
        Ok(())
    }
}
