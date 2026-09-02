use std::env;

use anyhow::Result;
use little_durable_objects::{
    actor_state::ActorStorageKey,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest, HostLeaseStore, PostgresHostLeaseStore},
    placement::{
        ObjectPlacementStore, PlacementClaim, PostgresObjectPlacementStore, StateCommit,
        StateCommitRequest,
    },
};

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

#[tokio::test]
async fn postgres_state_commit_is_fenced_by_owner_version_and_active_lease() -> Result<()> {
    let Some(url) = postgres_url() else {
        return Ok(());
    };
    let leases = PostgresHostLeaseStore::connect(&url).await?;
    let placements = PostgresObjectPlacementStore::connect(&url).await?;
    let object = ActorStorageKey::new(format!("object.v1.test.Counter.{}", uuid::Uuid::new_v4()));
    let host = HostId::new(format!("host-{}", uuid::Uuid::new_v4()));
    let session_id = uuid::Uuid::new_v4().to_string();
    leases
        .register(&HostLeaseRequest {
            id: host.clone(),
            session_id: session_id.clone(),
            route: "http://127.0.0.1:7000".into(),
            duration_ms: 30_000,
        })
        .await?;
    let PlacementClaim::Acquired(placement) =
        placements.claim(&object, None, &host, "us-east").await?
    else {
        anyhow::bail!("initial placement was not acquired")
    };
    let commit = StateCommitRequest {
        object: object.clone(),
        owner: host.clone(),
        session_id: session_id.clone(),
        owner_epoch: placement.owner_epoch,
        expected_version: 0,
        state_object: "snapshots/01/0123456789abcdef0123456789abcdef/test/Counter/one/1.json"
            .into(),
        request_id: "request-1".into(),
    };

    let StateCommit::Committed(committed) = placements.commit_state(&commit).await? else {
        anyhow::bail!("state head did not advance")
    };
    assert_eq!(committed.state_version, 1);
    assert!(matches!(
        placements.commit_state(&commit).await?,
        StateCommit::Committed(_)
    ));

    leases.unregister(&host, &session_id).await?;
    let mut after_expiry = commit;
    after_expiry.expected_version = 1;
    after_expiry.state_object =
        "snapshots/02/023456789abcdef0123456789abcdef0/test/Counter/one/2.json".into();
    after_expiry.request_id = "request-2".into();
    assert!(matches!(
        placements.commit_state(&after_expiry).await?,
        StateCommit::Current(_)
    ));
    Ok(())
}

fn postgres_url() -> Option<String> {
    env::var("DURABLE_OBJECT_TEST_POSTGRES_URL").ok()
}
