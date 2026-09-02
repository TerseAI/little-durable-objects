use std::{collections::HashMap, sync::Mutex};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{actor_state::ActorStorageKey, host::HostId, postgres::PostgresDatabase};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPlacement {
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub owner_epoch: u64,
    pub home_region: String,
    pub state_version: u64,
    pub state_object: Option<String>,
    pub last_request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementClaim {
    Acquired(ObjectPlacement),
    Current(ObjectPlacement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateCommitRequest {
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub session_id: String,
    pub owner_epoch: u64,
    pub expected_version: u64,
    pub state_object: String,
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateCommit {
    Committed(ObjectPlacement),
    Current(ObjectPlacement),
}

#[async_trait]
pub trait ObjectPlacementStore: Send + Sync {
    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>>;

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        home_region: &str,
    ) -> Result<PlacementClaim>;

    async fn commit_state(&self, request: &StateCommitRequest) -> Result<StateCommit>;
}

#[derive(Default)]
pub struct LocalObjectPlacementStore {
    placements: Mutex<HashMap<ActorStorageKey, ObjectPlacement>>,
}

#[async_trait]
impl ObjectPlacementStore for LocalObjectPlacementStore {
    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?
            .get(object)
            .cloned())
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        home_region: &str,
    ) -> Result<PlacementClaim> {
        validate_region(home_region)?;
        let mut placements = self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?;
        match placements.get(object) {
            None if expected.is_none() => {
                let placement = ObjectPlacement {
                    object: object.clone(),
                    owner: owner.clone(),
                    owner_epoch: 1,
                    home_region: home_region.to_owned(),
                    state_version: 0,
                    state_object: None,
                    last_request_id: None,
                };
                placements.insert(object.clone(), placement.clone());
                Ok(PlacementClaim::Acquired(placement))
            }
            Some(current) if expected == Some(current) => {
                ensure!(
                    current.home_region == home_region,
                    "object home region cannot change"
                );
                if &current.owner == owner {
                    return Ok(PlacementClaim::Current(current.clone()));
                }
                let placement = ObjectPlacement {
                    object: object.clone(),
                    owner: owner.clone(),
                    owner_epoch: current
                        .owner_epoch
                        .checked_add(1)
                        .context("object owner epoch overflow")?,
                    home_region: home_region.to_owned(),
                    state_version: current.state_version,
                    state_object: current.state_object.clone(),
                    last_request_id: current.last_request_id.clone(),
                };
                placements.insert(object.clone(), placement.clone());
                Ok(PlacementClaim::Acquired(placement))
            }
            Some(current) => Ok(PlacementClaim::Current(current.clone())),
            None => anyhow::bail!("expected object placement no longer exists"),
        }
    }

    async fn commit_state(&self, request: &StateCommitRequest) -> Result<StateCommit> {
        validate_state_commit(request)?;
        let mut placements = self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?;
        let current = placements
            .get(&request.object)
            .cloned()
            .context("actor placement does not exist")?;
        if is_replayed_commit(&current, request) {
            return Ok(StateCommit::Committed(current));
        }
        if current.owner != request.owner
            || current.owner_epoch != request.owner_epoch
            || current.state_version != request.expected_version
        {
            return Ok(StateCommit::Current(current));
        }
        let mut committed = current;
        committed.state_version = committed
            .state_version
            .checked_add(1)
            .context("actor state version overflow")?;
        committed.state_object = Some(request.state_object.clone());
        committed.last_request_id = Some(request.request_id.clone());
        placements.insert(request.object.clone(), committed.clone());
        Ok(StateCommit::Committed(committed))
    }
}

pub struct PostgresObjectPlacementStore {
    database: PostgresDatabase,
}

impl PostgresObjectPlacementStore {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::from_database(PostgresDatabase::connect(url).await?))
    }

    pub(crate) fn from_database(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl ObjectPlacementStore for PostgresObjectPlacementStore {
    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        let row = self
            .database
            .client()
            .query_opt(
                "SELECT owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id \
                 FROM durable_object_placements WHERE object_id = $1",
                &[&object.as_str()],
            )
            .await
            .context("load PostgreSQL object placement")?;
        row.map(|row| placement_from_row(object, &row)).transpose()
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        home_region: &str,
    ) -> Result<PlacementClaim> {
        object.validate()?;
        validate_region(home_region)?;
        if expected.is_none() {
            if let Some(row) = self
                .database
                .client()
                .query_opt(
                    "INSERT INTO durable_object_placements \
                     (object_id, owner_host_id, owner_epoch, home_region) \
                     VALUES ($1, $2, 1, $3) ON CONFLICT DO NOTHING \
                     RETURNING owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id",
                    &[&object.as_str(), &owner.as_str(), &home_region],
                )
                .await
                .context("insert PostgreSQL object placement")?
            {
                return Ok(PlacementClaim::Acquired(placement_from_row(object, &row)?));
            }
            return self.current_claim(object).await;
        }

        let expected = expected.expect("checked above");
        ensure!(
            expected.object == *object && expected.home_region == home_region,
            "expected object placement does not match the claim"
        );
        if &expected.owner == owner {
            return self.current_claim(object).await;
        }
        let expected_epoch = i64::try_from(expected.owner_epoch)
            .context("object owner epoch exceeds PostgreSQL BIGINT")?;
        if let Some(row) = self
            .database
            .client()
            .query_opt(
                "UPDATE durable_object_placements \
                 SET owner_host_id = $2, owner_epoch = owner_epoch + 1, updated_at = clock_timestamp() \
                 WHERE object_id = $1 AND owner_host_id = $3 AND owner_epoch = $4 AND home_region = $5 \
                 RETURNING owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id",
                &[
                    &object.as_str(),
                    &owner.as_str(),
                    &expected.owner.as_str(),
                    &expected_epoch,
                    &home_region,
                ],
            )
            .await
            .context("claim PostgreSQL object placement")?
        {
            return Ok(PlacementClaim::Acquired(placement_from_row(object, &row)?));
        }
        self.current_claim(object).await
    }

    async fn commit_state(&self, request: &StateCommitRequest) -> Result<StateCommit> {
        validate_state_commit(request)?;
        let expected_epoch = i64::try_from(request.owner_epoch)
            .context("object owner epoch exceeds PostgreSQL BIGINT")?;
        let expected_version = i64::try_from(request.expected_version)
            .context("actor state version exceeds PostgreSQL BIGINT")?;
        let row = self
            .database
            .client()
            .query_opt(
                "UPDATE durable_object_placements AS placement \
                 SET state_version = state_version + 1, state_object = $6, last_request_id = $7, updated_at = clock_timestamp() \
                 FROM durable_object_host_leases AS lease \
                 WHERE placement.object_id = $1 \
                   AND placement.owner_host_id = $2 \
                   AND placement.owner_epoch = $3 \
                   AND placement.state_version = $4 \
                   AND lease.host_id = placement.owner_host_id \
                   AND lease.session_id = $5 \
                   AND lease.expires_at_ms > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT \
                 RETURNING placement.owner_host_id, placement.owner_epoch, placement.home_region, \
                           placement.state_version, placement.state_object, placement.last_request_id",
                &[
                    &request.object.as_str(),
                    &request.owner.as_str(),
                    &expected_epoch,
                    &expected_version,
                    &request.session_id,
                    &request.state_object,
                    &request.request_id,
                ],
            )
            .await
            .context("commit PostgreSQL actor state head")?;
        if let Some(row) = row {
            return Ok(StateCommit::Committed(placement_from_row(
                &request.object,
                &row,
            )?));
        }
        let current = self
            .get(&request.object)
            .await?
            .context("actor placement disappeared during state commit")?;
        Ok(if is_replayed_commit(&current, request) {
            StateCommit::Committed(current)
        } else {
            StateCommit::Current(current)
        })
    }
}

impl PostgresObjectPlacementStore {
    async fn current_claim(&self, object: &ActorStorageKey) -> Result<PlacementClaim> {
        self.get(object)
            .await?
            .map(PlacementClaim::Current)
            .context("object placement disappeared during claim")
    }
}

fn placement_from_row(
    object: &ActorStorageKey,
    row: &tokio_postgres::Row,
) -> Result<ObjectPlacement> {
    Ok(ObjectPlacement {
        object: object.clone(),
        owner: HostId::new(row.get::<_, String>(0)),
        owner_epoch: u64::try_from(row.get::<_, i64>(1))
            .context("PostgreSQL object owner epoch is negative")?,
        home_region: row.get(2),
        state_version: u64::try_from(row.get::<_, i64>(3))
            .context("PostgreSQL actor state version is negative")?,
        state_object: row.get(4),
        last_request_id: row.get(5),
    })
}

fn validate_state_commit(request: &StateCommitRequest) -> Result<()> {
    request.object.validate()?;
    ensure!(
        !request.owner.as_str().is_empty(),
        "state commit owner is empty"
    );
    ensure!(
        !request.session_id.is_empty(),
        "state commit session is empty"
    );
    ensure!(
        request.owner_epoch > 0,
        "state commit owner epoch must be positive"
    );
    ensure!(
        !request.state_object.is_empty() && request.state_object.len() <= 1024,
        "state commit object name is invalid"
    );
    ensure!(
        !request.request_id.is_empty() && request.request_id.len() <= 255,
        "state commit request ID is invalid"
    );
    Ok(())
}

fn is_replayed_commit(current: &ObjectPlacement, request: &StateCommitRequest) -> bool {
    current.state_version == request.expected_version.saturating_add(1)
        && current.state_object.as_deref() == Some(&request.state_object)
        && current.last_request_id.as_deref() == Some(&request.request_id)
}

pub fn validate_region(region: &str) -> Result<()> {
    ensure!(
        !region.is_empty()
            && region.len() <= 64
            && region.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            }),
        "sandbox region is invalid"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn claims_once_and_increments_epoch_on_transfer() -> Result<()> {
        let store = LocalObjectPlacementStore::default();
        let object = ActorStorageKey::new("object.v1.project.Counter.one");
        let first = match store
            .claim(&object, None, &HostId::new("host-a"), "us-east")
            .await?
        {
            PlacementClaim::Acquired(placement) => placement,
            claim => anyhow::bail!("unexpected claim: {claim:?}"),
        };
        assert_eq!(first.owner_epoch, 1);

        let second = match store
            .claim(&object, Some(&first), &HostId::new("host-b"), "us-east")
            .await?
        {
            PlacementClaim::Acquired(placement) => placement,
            claim => anyhow::bail!("unexpected claim: {claim:?}"),
        };
        assert_eq!(second.owner, HostId::new("host-b"));
        assert_eq!(second.owner_epoch, 2);
        assert_eq!(second.home_region, "us-east");
        Ok(())
    }

    #[tokio::test]
    async fn stale_claim_observes_the_current_owner() -> Result<()> {
        let store = LocalObjectPlacementStore::default();
        let object = ActorStorageKey::new("object.v1.project.Counter.one");
        let PlacementClaim::Acquired(first) = store
            .claim(&object, None, &HostId::new("host-a"), "us-east")
            .await?
        else {
            anyhow::bail!("first claim was not acquired")
        };
        let PlacementClaim::Acquired(second) = store
            .claim(&object, Some(&first), &HostId::new("host-b"), "us-east")
            .await?
        else {
            anyhow::bail!("second claim was not acquired")
        };
        assert_eq!(
            store
                .claim(&object, Some(&first), &HostId::new("host-c"), "us-east",)
                .await?,
            PlacementClaim::Current(second)
        );
        Ok(())
    }
}
