use anyhow::{Context, Result};
use async_trait::async_trait;

use super::{HostLease, HostLeaseRequest, HostLeaseStatus, HostLeaseStore};
use crate::{host::HostId, postgres::PostgresDatabase};

const REGISTRY_NOW_MS: &str = "(extract(epoch FROM clock_timestamp()) * 1000)::bigint";

pub struct PostgresHostLeaseStore {
    database: PostgresDatabase,
}

impl PostgresHostLeaseStore {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::from_database(PostgresDatabase::connect(url).await?))
    }

    pub(crate) fn from_database(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl HostLeaseStore for PostgresHostLeaseStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        request.validate_duration()?;
        let duration_ms = i64::try_from(request.duration_ms)
            .expect("validated host lease duration must fit PostgreSQL BIGINT");
        let row = self
            .database
            .client()
            .query_opt(
                &format!(
                    "INSERT INTO durable_object_host_leases (host_id, session_id, route, expires_at_ms) \
                     VALUES ($1, $2, $3, {REGISTRY_NOW_MS} + $4) \
                     ON CONFLICT (host_id) DO UPDATE \
                     SET session_id = EXCLUDED.session_id, route = EXCLUDED.route, \
                         expires_at_ms = EXCLUDED.expires_at_ms \
                     WHERE durable_object_host_leases.session_id = EXCLUDED.session_id \
                        OR durable_object_host_leases.expires_at_ms <= {REGISTRY_NOW_MS} \
                     RETURNING expires_at_ms"
                ),
                &[
                    &request.id.as_str(),
                    &request.session_id,
                    &request.route,
                    &duration_ms,
                ],
            )
            .await
            .context("register PostgreSQL host lease")?
            .context("host lease is held by a different active session")?;
        Ok(HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: u64::try_from(row.get::<_, i64>(0))
                .context("PostgreSQL host lease expiration is negative")?,
        })
    }

    async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus> {
        let row = self
            .database
            .client()
            .query_one(
                &format!(
                    "SELECT {REGISTRY_NOW_MS} AS store_now_ms, lease.session_id, \
                            lease.route, lease.expires_at_ms \
                     FROM (SELECT 1) AS clock_row \
                     LEFT JOIN durable_object_host_leases AS lease ON lease.host_id = $1"
                ),
                &[&id.as_str()],
            )
            .await
            .context("read PostgreSQL host lease status")?;
        let store_now_ms = u64::try_from(row.get::<_, i64>(0))
            .context("PostgreSQL lease-store clock is before the Unix epoch")?;
        let lease = row
            .get::<_, Option<String>>(1)
            .map(|session_id| {
                Ok::<_, anyhow::Error>(HostLease {
                    id: id.clone(),
                    session_id,
                    route: row
                        .get::<_, Option<String>>(2)
                        .context("PostgreSQL host lease row is missing its route")?,
                    expires_at_ms: u64::try_from(
                        row.get::<_, Option<i64>>(3)
                            .context("PostgreSQL host lease row is missing its expiration")?,
                    )
                    .context("PostgreSQL host lease expiration is negative")?,
                })
            })
            .transpose()?;
        Ok(HostLeaseStatus {
            lease,
            store_now_ms,
        })
    }

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()> {
        self.database
            .client()
            .execute(
                "DELETE FROM durable_object_host_leases WHERE host_id = $1 AND session_id = $2",
                &[&id.as_str(), &session_id],
            )
            .await
            .context("unregister PostgreSQL host lease")?;
        Ok(())
    }
}
