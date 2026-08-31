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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementClaim {
    Acquired(ObjectPlacement),
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
                };
                placements.insert(object.clone(), placement.clone());
                Ok(PlacementClaim::Acquired(placement))
            }
            Some(current) => Ok(PlacementClaim::Current(current.clone())),
            None => anyhow::bail!("expected object placement no longer exists"),
        }
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
                "SELECT owner_host_id, owner_epoch, home_region \
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
                     RETURNING owner_host_id, owner_epoch, home_region",
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
                 RETURNING owner_host_id, owner_epoch, home_region",
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
    })
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
