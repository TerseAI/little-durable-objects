use anyhow::Result;
use little_durable_objects::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    host::HostId,
    placement::{
        LocalObjectPlacementStore, ObjectPlacementStore, PlacementClaim, StateCommit,
        StateCommitRequest,
    },
    state_log::StateSnapshot,
};
use serde_json::json;

#[test]
fn immutable_snapshot_round_trips_the_result_for_request_replay() -> Result<()> {
    let snapshot = StateSnapshot::new(7, 3, "request-7".into(), json!({ "count": 7 }), json!(7))?;

    let decoded = StateSnapshot::decode(&snapshot.encode()?)?;

    assert_eq!(decoded, snapshot);
    assert_eq!(decoded.replay("request-7"), Some(&json!(7)));
    assert_eq!(decoded.replay("request-8"), None);
    Ok(())
}

#[tokio::test]
async fn state_head_advances_once_and_replays_the_same_commit() -> Result<()> {
    let store = LocalObjectPlacementStore::default();
    let actor = actor();
    let host = HostId::new("host-a");
    let PlacementClaim::Acquired(placement) = store
        .claim(&actor.storage_key(), None, &host, "us-east")
        .await?
    else {
        anyhow::bail!("first claim was not acquired")
    };
    let request = commit(&actor.storage_key(), &host, placement.owner_epoch);

    let StateCommit::Committed(committed) = store.commit_state(&request).await? else {
        anyhow::bail!("first state commit did not succeed")
    };
    assert_eq!(committed.state_version, 1);
    assert_eq!(
        committed.state_object.as_deref(),
        Some("snapshots/a/state-1.json")
    );

    let StateCommit::Committed(replayed) = store.commit_state(&request).await? else {
        anyhow::bail!("identical state commit was not idempotent")
    };
    assert_eq!(replayed, committed);

    let mut conflicting = request;
    conflicting.state_object = "snapshots/b/state-1.json".into();
    assert!(matches!(
        store.commit_state(&conflicting).await?,
        StateCommit::Current(_)
    ));
    Ok(())
}

#[tokio::test]
async fn transferred_ownership_fences_an_old_state_commit() -> Result<()> {
    let store = LocalObjectPlacementStore::default();
    let actor = actor();
    let old_host = HostId::new("host-a");
    let PlacementClaim::Acquired(first) = store
        .claim(&actor.storage_key(), None, &old_host, "us-east")
        .await?
    else {
        anyhow::bail!("first claim was not acquired")
    };
    let PlacementClaim::Acquired(_) = store
        .claim(
            &actor.storage_key(),
            Some(&first),
            &HostId::new("host-b"),
            "us-east",
        )
        .await?
    else {
        anyhow::bail!("ownership transfer was not acquired")
    };

    assert!(matches!(
        store
            .commit_state(&commit(&actor.storage_key(), &old_host, first.owner_epoch,))
            .await?,
        StateCommit::Current(_)
    ));
    Ok(())
}

fn actor() -> ActorKey {
    ActorKey {
        namespace_id: "project-1".into(),
        actor_type: "Counter".into(),
        actor_id: "counter-1".into(),
    }
}

fn commit(object: &ActorStorageKey, host: &HostId, owner_epoch: u64) -> StateCommitRequest {
    StateCommitRequest {
        object: object.clone(),
        owner: host.clone(),
        session_id: "session-a".into(),
        owner_epoch,
        expected_version: 0,
        state_object: "snapshots/a/state-1.json".into(),
        request_id: "request-1".into(),
    }
}
