use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::{credentials, signer::Signer};
use google_cloud_storage::{builder::storage::SignedUrlBuilder, http::Method};
use serde::{Deserialize, Serialize};

use crate::{actor::ActorKey, placement::validate_region};

const SIGNED_URL_TTL: Duration = Duration::from_secs(60);
pub const STATE_CONTENT_TYPE: &str = "application/json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateWriteTicket {
    pub state_version: u64,
    pub object_name: String,
    pub url: String,
    pub expires_at_ms: i64,
}

#[async_trait]
pub trait StorageUrlSigner: Send + Sync {
    async fn read_url(&self, region: &str, object_name: &str) -> Result<String>;

    async fn write_ticket(
        &self,
        region: &str,
        actor: &ActorKey,
        state_version: u64,
    ) -> Result<StateWriteTicket>;

    fn regions(&self) -> Vec<String>;
}

#[derive(Clone)]
pub struct GcsStorageUrlSigner {
    buckets: HashMap<String, String>,
    signer: Signer,
}

impl GcsStorageUrlSigner {
    pub fn from_adc(buckets: HashMap<String, String>) -> Result<Self> {
        Self::new(buckets, credentials::Builder::default().build_signer()?)
    }

    pub fn new(buckets: HashMap<String, String>, signer: Signer) -> Result<Self> {
        validate_buckets(&buckets)?;
        Ok(Self { buckets, signer })
    }
}

#[async_trait]
impl StorageUrlSigner for GcsStorageUrlSigner {
    async fn read_url(&self, region: &str, object_name: &str) -> Result<String> {
        validate_object_name(object_name)?;
        SignedUrlBuilder::for_object(self.bucket(region)?, object_name)
            .with_method(Method::GET)
            .with_expiration(SIGNED_URL_TTL)
            .sign_with(&self.signer)
            .await
            .context("sign GCS actor-state read URL")
    }

    async fn write_ticket(
        &self,
        region: &str,
        actor: &ActorKey,
        state_version: u64,
    ) -> Result<StateWriteTicket> {
        actor.validate()?;
        ensure!(state_version > 0, "actor state version must be positive");
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let object_name = snapshot_object_name(actor, state_version, &nonce)?;
        let expires_at_ms = unix_millis()?
            .checked_add(i64::try_from(SIGNED_URL_TTL.as_millis())?)
            .context("signed state-write URL expiration overflow")?;
        let url = SignedUrlBuilder::for_object(self.bucket(region)?, &object_name)
            .with_method(Method::PUT)
            .with_expiration(SIGNED_URL_TTL)
            .with_header("content-type", STATE_CONTENT_TYPE)
            .with_query_param("ifGenerationMatch", "0")
            .sign_with(&self.signer)
            .await
            .context("sign GCS actor-state write URL")?;
        Ok(StateWriteTicket {
            state_version,
            object_name,
            url,
            expires_at_ms,
        })
    }

    fn regions(&self) -> Vec<String> {
        let mut regions = self.buckets.keys().cloned().collect::<Vec<_>>();
        regions.sort();
        regions
    }
}

impl GcsStorageUrlSigner {
    fn bucket(&self, region: &str) -> Result<String> {
        validate_region(region)?;
        self.buckets
            .get(region)
            .map(|bucket| format!("projects/_/buckets/{bucket}"))
            .with_context(|| format!("sandbox region {region:?} has no Standard bucket"))
    }
}

pub fn snapshot_object_name(actor: &ActorKey, state_version: u64, nonce: &str) -> Result<String> {
    actor.validate()?;
    ensure!(state_version > 0, "actor state version must be positive");
    ensure!(
        nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "actor state object nonce is invalid"
    );
    Ok(format!(
        "snapshots/{}/{}/{}/{}/{}/{}.json",
        &nonce[..2],
        nonce,
        actor.namespace_id,
        actor.actor_type,
        actor.actor_id,
        state_version,
    ))
}

pub fn validate_snapshot_object_name(
    actor: &ActorKey,
    state_version: u64,
    object_name: &str,
) -> Result<()> {
    validate_object_name(object_name)?;
    let mut parts = object_name.split('/');
    let valid = matches!(parts.next(), Some("snapshots"))
        && matches!((parts.next(), parts.next()), (Some(prefix), Some(nonce)) if nonce.starts_with(prefix) && prefix.len() == 2 && nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit()))
        && parts.next() == Some(actor.namespace_id.as_str())
        && parts.next() == Some(actor.actor_type.as_str())
        && parts.next() == Some(actor.actor_id.as_str())
        && parts.next() == Some(format!("{state_version}.json").as_str())
        && parts.next().is_none();
    ensure!(valid, "actor state object name does not match its commit");
    Ok(())
}

pub fn validate_buckets(buckets: &HashMap<String, String>) -> Result<()> {
    ensure!(!buckets.is_empty(), "Standard bucket map must not be empty");
    for (region, bucket) in buckets {
        validate_region(region)?;
        ensure!(
            !bucket.is_empty()
                && bucket.trim() == bucket
                && bucket.len() <= 222
                && !bucket.contains('/'),
            "Standard bucket for region {region:?} is invalid"
        );
    }
    Ok(())
}

fn validate_object_name(object_name: &str) -> Result<()> {
    ensure!(
        object_name.starts_with("snapshots/")
            && object_name.len() <= 1024
            && !object_name.chars().any(char::is_control),
        "actor state object name is invalid"
    );
    Ok(())
}

fn unix_millis() -> Result<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis(),
    )
    .context("system clock exceeds supported state-write timestamp range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_a_randomly_distributed_immutable_snapshot_path() -> Result<()> {
        assert_eq!(
            snapshot_object_name(
                &ActorKey {
                    namespace_id: "project-1".into(),
                    actor_type: "Counter".into(),
                    actor_id: "account.42".into(),
                },
                7,
                "0123456789abcdef0123456789abcdef",
            )?,
            "snapshots/01/0123456789abcdef0123456789abcdef/project-1/Counter/account.42/7.json"
        );
        validate_snapshot_object_name(
            &ActorKey {
                namespace_id: "project-1".into(),
                actor_type: "Counter".into(),
                actor_id: "account.42".into(),
            },
            7,
            "snapshots/01/0123456789abcdef0123456789abcdef/project-1/Counter/account.42/7.json",
        )?;
        Ok(())
    }

    #[test]
    fn validates_the_only_storage_configuration() {
        assert!(
            validate_buckets(&HashMap::from([(
                "us-east".into(),
                "objects-us-east".into()
            )]))
            .is_ok()
        );
        assert!(validate_buckets(&HashMap::new()).is_err());
        assert!(
            validate_buckets(&HashMap::from([("bad/region".into(), "objects".into())])).is_err()
        );
    }
}
