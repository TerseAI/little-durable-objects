use std::{env, sync::Arc};

use anyhow::Result;
use durable_object_runtime::{
    actor_state::{ActorDatabaseStore, ActorStorageKey, SqliteActorDatabase},
    durability::{
        ActorChangeCapture, ActorDurabilityStore, LocalActorChangeCapture, LocalCommitStore,
        ManifestStore, OwnershipClaimResult, PostgresManifestStore, RegionalActorStore,
    },
    host::HostId,
    host_leases::{HostLeaseRequest, HostLeaseStore, PostgresHostLeaseStore},
};
use tempfile::TempDir;

fn set_json<T: serde::Serialize + ?Sized>(
    database: &SqliteActorDatabase,
    key: &str,
    value: &T,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    database.set_bytes_batch(&[(key, &bytes)])?;
    Ok(())
}

fn postgres_url() -> Option<String> {
    env::var("DURABLE_OBJECT_TEST_POSTGRES_URL").ok()
}

#[tokio::test]
async fn postgres_manifest_store_composes_with_an_independent_commit_store() -> Result<()> {
    let Some(url) = postgres_url() else {
        return Ok(());
    };
    let dir = TempDir::new()?;
    let manifests = Arc::new(PostgresManifestStore::connect(&url).await?);
    let commits = Arc::new(LocalCommitStore::new(dir.path().join("commits")));
    let store = RegionalActorStore::new(manifests.clone(), commits);
    let object = ActorStorageKey::new(format!("pg-split-{}", uuid::Uuid::new_v4()));
    let node_a = HostId::new(format!("node-a-{}", uuid::Uuid::new_v4()));
    let node_b = HostId::new(format!("node-b-{}", uuid::Uuid::new_v4()));

    let claimed = match store
        .claim_in_home_region(&object, None, &node_a, "us-east")
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("initial PostgreSQL claim should succeed: {result:?}"),
    };
    assert_eq!(claimed.manifest.home_region, "us-east");
    let capture = LocalActorChangeCapture::new(dir.path().join("sqlite"));
    capture.prepare(&object).await?;
    let database = ActorDatabaseStore::new(dir.path().join("sqlite")).open(&object)?;
    set_json(&database, "key", &"value")?;
    let captured = capture.capture(&object).await?;
    let published = store.publish(&object, &claimed, &captured).await?;

    assert_eq!(published.max_txid(), 1);

    // A maintenance CAS changes the manifest revision but not its owner or canonical
    // tip. A writer holding the older revision must still be able to publish, and it
    // must retain the newer maintenance metadata returned by PostgreSQL.
    let mut maintained = published.manifest.clone();
    maintained.archived_txid = 1;
    let maintained = manifests
        .advance(&object, &published, &maintained)
        .await?
        .expect("maintenance manifest advance");
    assert_eq!(maintained.manifest.archived_txid, 1);
    set_json(&database, "second", &"value")?;
    let captured = capture.capture(&object).await?;
    let published = store.publish(&object, &published, &captured).await?;
    assert_eq!(published.max_txid(), 2);
    assert_eq!(published.manifest.archived_txid, 1);
    assert_eq!(
        store
            .canonical_segments(&object, &published.manifest)
            .await?
            .len(),
        2
    );
    let wrong_region = store
        .claim_in_home_region(&object, Some(&published), &node_b, "eu-west")
        .await
        .expect_err("PostgreSQL must reject an ownership handoff that moves the home region");
    assert!(
        wrong_region
            .to_string()
            .contains("home region cannot change")
    );

    let takeover = match store
        .claim_in_home_region(&object, Some(&published), &node_b, "us-east")
        .await?
    {
        OwnershipClaimResult::Acquired(manifest) => manifest,
        result => panic!("PostgreSQL takeover should succeed: {result:?}"),
    };
    assert_eq!(takeover.owner().epoch, 2);
    assert_eq!(takeover.owner().host, node_b);
    assert_eq!(takeover.manifest.home_region, "us-east");
    Ok(())
}

#[tokio::test]
async fn postgres_host_lease_store_round_trips_leases() -> Result<()> {
    let Some(url) = postgres_url() else {
        return Ok(());
    };
    let leases = PostgresHostLeaseStore::connect(&url).await?;
    let request = HostLeaseRequest {
        id: HostId::new(format!("node-{}", uuid::Uuid::new_v4())),
        session_id: uuid::Uuid::new_v4().to_string(),
        route: "http://127.0.0.1:7000".into(),
        duration_ms: 30_000,
    };
    let lease = leases.register(&request).await?;
    assert_eq!(
        leases.lease_status(&lease.id).await?.lease,
        Some(lease.clone())
    );
    leases.unregister(&lease.id, &lease.session_id).await?;
    assert!(leases.lease_status(&lease.id).await?.lease.is_none());
    Ok(())
}

#[tokio::test]
async fn postgres_host_lease_store_fences_sessions_atomically() -> Result<()> {
    let Some(url) = postgres_url() else {
        return Ok(());
    };
    let leases = PostgresHostLeaseStore::connect(&url).await?;
    let host_id = HostId::new(format!("node-{}", uuid::Uuid::new_v4()));
    let first = HostLeaseRequest {
        id: host_id.clone(),
        session_id: uuid::Uuid::new_v4().to_string(),
        route: "http://127.0.0.1:7000".into(),
        duration_ms: 30_000,
    };
    let mut successor = first.clone();
    successor.session_id = uuid::Uuid::new_v4().to_string();
    successor.route = "http://127.0.0.1:7001".into();

    leases.register(&first).await?;
    assert!(leases.register(&successor).await.is_err());

    let expiring = HostLeaseRequest {
        duration_ms: 1,
        ..first.clone()
    };
    leases.register(&expiring).await?;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let successor = leases.register(&successor).await?;

    leases.unregister(&host_id, &first.session_id).await?;
    assert_eq!(
        leases.lease_status(&host_id).await?.lease,
        Some(successor.clone())
    );
    leases.unregister(&host_id, &successor.session_id).await?;
    assert!(leases.lease_status(&host_id).await?.lease.is_none());
    Ok(())
}
