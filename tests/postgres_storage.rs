use std::env;

use anyhow::Result;
use durable_object_runtime::{
    actor_state::ActorStorageKey,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest, HostLeaseStore, PostgresHostLeaseStore},
    placement::{ObjectPlacementStore, PlacementClaim, PostgresObjectPlacementStore},
};

fn postgres_url() -> Option<String> {
    env::var("DURABLE_OBJECT_TEST_POSTGRES_URL").ok()
}

#[tokio::test]
async fn postgres_placement_keeps_home_region_and_increments_owner_epoch() -> Result<()> {
    let Some(url) = postgres_url() else {
        return Ok(());
    };
    let placements = PostgresObjectPlacementStore::connect(&url).await?;
    let object = ActorStorageKey::new(format!("object.v1.test.Counter.{}", uuid::Uuid::new_v4()));
    let first = match placements
        .claim(&object, None, &HostId::new("host-a"), "us-east")
        .await?
    {
        PlacementClaim::Acquired(placement) => placement,
        claim => anyhow::bail!("unexpected initial placement: {claim:?}"),
    };
    let second = match placements
        .claim(&object, Some(&first), &HostId::new("host-b"), "us-east")
        .await?
    {
        PlacementClaim::Acquired(placement) => placement,
        claim => anyhow::bail!("unexpected takeover: {claim:?}"),
    };
    assert_eq!(second.owner_epoch, 2);
    assert_eq!(second.home_region, "us-east");
    assert!(
        placements
            .claim(&object, Some(&second), &HostId::new("host-c"), "eu-west")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn postgres_host_lease_store_round_trips_and_fences_sessions() -> Result<()> {
    let Some(url) = postgres_url() else {
        return Ok(());
    };
    let leases = PostgresHostLeaseStore::connect(&url).await?;
    let host_id = HostId::new(format!("host-{}", uuid::Uuid::new_v4()));
    let first = HostLeaseRequest {
        id: host_id.clone(),
        session_id: uuid::Uuid::new_v4().to_string(),
        route: "http://127.0.0.1:7000".into(),
        duration_ms: 30_000,
    };
    let mut successor = first.clone();
    successor.session_id = uuid::Uuid::new_v4().to_string();
    assert!(leases.register(&first).await.is_ok());
    assert!(leases.register(&successor).await.is_err());
    leases.unregister(&host_id, &first.session_id).await?;
    let successor = leases.register(&successor).await?;
    assert_eq!(
        leases.lease_status(&host_id).await?.lease,
        Some(successor.clone())
    );
    leases.unregister(&host_id, &successor.session_id).await?;
    Ok(())
}
