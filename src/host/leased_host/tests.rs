use super::*;
use crate::{
    actor_state::ActorDatabaseStore,
    durability::{LocalActorChangeCapture, LocalActorStore, LtxActorStateRestorer},
    host::{HostEndpoint, HostId},
    host_leases::LocalHostLeaseStore,
};
use tempfile::TempDir;

// ========================================================================
// Lease-managed sandbox endpoint lifecycle
// ========================================================================

#[tokio::test]
async fn starts_with_a_lease_and_shuts_down_cleanly() -> Result<()> {
    let root = TempDir::new()?;
    let coordination = root.path().join("coordination");
    let local_root = root.path().join("sandbox");
    let leases = Arc::new(LocalHostLeaseStore::new(&coordination).await?);
    let durability = Arc::new(LocalActorStore::new(root.path().join("object-store")));
    let databases = Arc::new(ActorDatabaseStore::new(&local_root));
    let restore = Arc::new(LtxActorStateRestorer::new(
        durability.clone(),
        databases.clone(),
    ));
    let dependencies = ActorHostDependencies::new(
        durability.clone(),
        leases.clone(),
        databases,
        Arc::new(LocalActorChangeCapture::new(&local_root)),
        restore,
    );
    let endpoint = HostEndpoint {
        id: HostId::new(uuid::Uuid::new_v4().to_string()),
        route: "sandbox-session".into(),
    };
    let host_id = endpoint.id.clone();

    let leased_host = LeasedActorHost::start(
        endpoint,
        "test-session".into(),
        dependencies,
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .await?;

    let lease = leases.get(&host_id).await?.expect("initial lease");
    assert_eq!(lease.route, "sandbox-session");
    assert_eq!(leased_host.consecutive_lease_failures(), 0);

    leased_host.shutdown().await?;
    assert!(leases.get(&host_id).await?.is_none());
    Ok(())
}
