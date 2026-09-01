use super::{HostLease, HostLeaseRegistry, HostLeaseRequest, HostLeaseStatus, HostLeaseStore};
use crate::{
    clock::{Clock, SystemClock},
    host::HostId,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

pub struct LocalHostLeaseStore {
    root: PathBuf,
    clock: Arc<dyn Clock>,
    mutations: Mutex<()>,
}

impl LocalHostLeaseStore {
    pub async fn new<P>(root: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let root = root.into();

        tokio::fs::create_dir_all(&root).await?;

        Ok(Self {
            root,
            clock: Arc::new(SystemClock),
            mutations: Mutex::new(()),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[cfg(test)]
    pub(crate) async fn get(&self, id: &HostId) -> Result<Option<HostLease>> {
        let _mutation = self.mutations.lock().await;
        self.load(id).await
    }
}

#[async_trait]
impl HostLeaseRegistry for LocalHostLeaseStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        request.validate_duration()?;
        let _mutation = self.mutations.lock().await;
        let now_ms = self.clock.now_ms()?;
        if let Some(current) = self.load(&request.id).await? {
            ensure!(
                current.session_id == request.session_id || current.expires_at_ms <= now_ms,
                "host lease is held by a different active session"
            );
        }
        let lease = HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: now_ms
                .checked_add(request.duration_ms)
                .context("host lease expiration overflow")?,
        };
        let body = serde_json::to_string(&lease)?;
        let path = self.path_for(&lease.id);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(path, body).await?;

        Ok(lease)
    }

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()> {
        let _mutation = self.mutations.lock().await;
        let Some(lease) = self.load(id).await? else {
            return Ok(());
        };
        if lease.session_id != session_id {
            return Ok(());
        }
        match tokio::fs::remove_file(self.path_for(id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl HostLeaseStore for LocalHostLeaseStore {
    async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus> {
        let _mutation = self.mutations.lock().await;
        Ok(HostLeaseStatus {
            lease: self.load(id).await?,
            store_now_ms: self.clock.now_ms()?,
        })
    }
}

impl LocalHostLeaseStore {
    fn path_for(&self, id: &HostId) -> PathBuf {
        self.root.join(format!("{id}.json"))
    }

    async fn load(&self, id: &HostId) -> Result<Option<HostLease>> {
        let body = match tokio::fs::read(self.path_for(id)).await {
            Ok(body) => body,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        Ok(Some(serde_json::from_slice(&body)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_leases::MAX_HOST_LEASE_DURATION_MS;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    pub(crate) struct ManualClock(AtomicU64);

    impl ManualClock {
        pub(crate) fn new(now_ms: u64) -> Self {
            Self(AtomicU64::new(now_ms))
        }

        pub(crate) fn set(&self, now_ms: u64) {
            self.0.store(now_ms, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_ms(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    #[tokio::test]
    async fn creates_the_lease_store_directory() -> Result<()> {
        let dir = TempDir::new()?;
        let root = dir.path().join("store");

        LocalHostLeaseStore::new(&root).await?;

        assert!(root.is_dir());

        Ok(())
    }

    #[tokio::test]
    async fn registers_a_lease_stamped_with_the_lease_store_clock() -> Result<()> {
        let dir = TempDir::new()?;
        let store = LocalHostLeaseStore::new(dir.path())
            .await?
            .with_clock(Arc::new(ManualClock::new(1_000)));
        let request = request("node-a");

        let lease = store.register(&request).await?;

        assert_eq!(lease.expires_at_ms, 31_000);
        assert_eq!(
            store.get(&request.id).await?.expect("registered lease"),
            lease
        );

        Ok(())
    }

    #[tokio::test]
    async fn replaces_an_existing_lease_for_the_same_host() -> Result<()> {
        let dir = TempDir::new()?;
        let clock = Arc::new(ManualClock::new(1_000));
        let store = LocalHostLeaseStore::new(dir.path())
            .await?
            .with_clock(clock.clone());
        let first = request("node-a");
        let mut second = first.clone();
        second.route = "127.0.0.1:7001".into();

        store.register(&first).await?;
        clock.set(2_000);
        let renewed = store.register(&second).await?;

        let stored = store.get(&second.id).await?.expect("replacement lease");
        assert_eq!(stored, renewed);
        assert_eq!(stored.route, "127.0.0.1:7001");
        assert_eq!(stored.expires_at_ms, 32_000);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_a_different_session_while_the_lease_is_active() -> Result<()> {
        let dir = TempDir::new()?;
        let store = LocalHostLeaseStore::new(dir.path())
            .await?
            .with_clock(Arc::new(ManualClock::new(1_000)));
        let first = request("node-a");
        let mut replacement = first.clone();
        replacement.session_id = "replacement-session".into();

        store.register(&first).await?;
        let error = store
            .register(&replacement)
            .await
            .expect_err("an active lease must fence a different session");

        assert_eq!(
            error.to_string(),
            "host lease is held by a different active session"
        );
        assert_eq!(
            store.get(&first.id).await?.as_ref(),
            Some(&HostLease {
                id: first.id,
                session_id: first.session_id,
                route: first.route,
                expires_at_ms: 31_000,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn permits_a_different_session_after_the_lease_expires() -> Result<()> {
        let dir = TempDir::new()?;
        let clock = Arc::new(ManualClock::new(1_000));
        let store = LocalHostLeaseStore::new(dir.path())
            .await?
            .with_clock(clock.clone());
        let first = request("node-a");
        let mut replacement = first.clone();
        replacement.session_id = "replacement-session".into();

        store.register(&first).await?;
        clock.set(31_000);
        let replacement = store.register(&replacement).await?;

        assert_eq!(replacement.session_id, "replacement-session");
        assert_eq!(replacement.expires_at_ms, 61_000);
        Ok(())
    }

    #[tokio::test]
    async fn lease_status_pairs_the_lease_with_the_lease_store_clock() -> Result<()> {
        let dir = TempDir::new()?;
        let clock = Arc::new(ManualClock::new(1_000));
        let store = LocalHostLeaseStore::new(dir.path())
            .await?
            .with_clock(clock.clone());
        let request = request("node-a");
        store.register(&request).await?;

        let live = store.lease_status(&request.id).await?;
        assert!(live.is_active());
        assert_eq!(live.store_now_ms, 1_000);

        clock.set(31_000);
        let expired = store.lease_status(&request.id).await?;
        assert!(!expired.is_active());

        let absent = store.lease_status(&HostId::new("missing-node")).await?;
        assert!(!absent.is_active());
        assert!(absent.lease.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn local_root_is_the_equivalent_of_the_bucket_root() -> Result<()> {
        let dir = TempDir::new()?;
        let store = LocalHostLeaseStore::new(dir.path()).await?;

        assert_eq!(
            store.path_for(&HostId::new("node-a")),
            dir.path().join("node-a.json")
        );

        Ok(())
    }

    #[tokio::test]
    async fn get_returns_none_for_an_unknown_host() -> Result<()> {
        let dir = TempDir::new()?;
        let store = LocalHostLeaseStore::new(dir.path()).await?;

        assert!(store.get(&HostId::new("missing-node")).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn unregisters_a_host_lease() -> Result<()> {
        let dir = TempDir::new()?;
        let store = LocalHostLeaseStore::new(dir.path()).await?;
        let request = request("node-a");
        store.register(&request).await?;

        store.unregister(&request.id, &request.session_id).await?;

        assert!(store.get(&request.id).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn stale_session_cannot_unregister_a_successor() -> Result<()> {
        let dir = TempDir::new()?;
        let clock = Arc::new(ManualClock::new(1_000));
        let store = LocalHostLeaseStore::new(dir.path())
            .await?
            .with_clock(clock.clone());
        let first = request("node-a");
        let mut successor = first.clone();
        successor.session_id = "successor-session".into();

        store.register(&first).await?;
        clock.set(31_000);
        let successor = store.register(&successor).await?;
        store.unregister(&first.id, &first.session_id).await?;

        assert_eq!(store.get(&first.id).await?, Some(successor));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_a_lease_above_the_maximum_duration() -> Result<()> {
        let dir = TempDir::new()?;
        let store = LocalHostLeaseStore::new(dir.path()).await?;
        let mut oversized = request("node-a");
        oversized.duration_ms = MAX_HOST_LEASE_DURATION_MS + 1;

        let error = store
            .register(&oversized)
            .await
            .expect_err("oversized leases must be rejected");

        assert_eq!(
            error.to_string(),
            format!("host lease duration must not exceed {MAX_HOST_LEASE_DURATION_MS}ms")
        );
        assert!(store.get(&oversized.id).await?.is_none());
        Ok(())
    }

    fn request(id: &str) -> HostLeaseRequest {
        HostLeaseRequest {
            id: HostId::new(id),
            session_id: format!("session-{id}"),
            route: "127.0.0.1:7000".into(),
            duration_ms: 30_000,
        }
    }
}
