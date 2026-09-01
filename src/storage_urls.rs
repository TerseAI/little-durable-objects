use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::{credentials, signer::Signer};
use google_cloud_storage::{builder::storage::SignedUrlBuilder, http::Method};

use crate::{actor::ActorKey, placement::validate_region};

const SIGNED_URL_TTL: Duration = Duration::from_secs(60);
pub const STATE_CONTENT_TYPE: &str = "application/x-ndjson";

#[async_trait]
pub trait StorageUrlSigner: Send + Sync {
    async fn read_url(&self, region: &str, actor: &ActorKey) -> Result<String>;

    async fn write_url(
        &self,
        region: &str,
        actor: &ActorKey,
        expected_generation: &str,
    ) -> Result<String>;

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
    async fn read_url(&self, region: &str, actor: &ActorKey) -> Result<String> {
        actor.validate()?;
        SignedUrlBuilder::for_object(self.bucket(region)?, object_name(actor))
            .with_method(Method::GET)
            .with_expiration(SIGNED_URL_TTL)
            .sign_with(&self.signer)
            .await
            .context("sign GCS actor-state read URL")
    }

    async fn write_url(
        &self,
        region: &str,
        actor: &ActorKey,
        expected_generation: &str,
    ) -> Result<String> {
        actor.validate()?;
        validate_generation(expected_generation)?;
        SignedUrlBuilder::for_object(self.bucket(region)?, object_name(actor))
            .with_method(Method::PUT)
            .with_expiration(SIGNED_URL_TTL)
            .with_header("content-type", STATE_CONTENT_TYPE)
            .with_query_param("ifGenerationMatch", expected_generation)
            .sign_with(&self.signer)
            .await
            .context("sign GCS actor-state write URL")
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

pub fn object_name(actor: &ActorKey) -> String {
    format!(
        "objects/{}/{}/{}.ndjson",
        actor.namespace_id, actor.actor_type, actor.actor_id
    )
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

fn validate_generation(generation: &str) -> Result<()> {
    ensure!(
        !generation.is_empty()
            && generation.len() <= 32
            && generation.bytes().all(|byte| byte.is_ascii_digit()),
        "GCS generation is invalid"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_one_readable_object_path() {
        assert_eq!(
            object_name(&ActorKey {
                namespace_id: "project-1".into(),
                actor_type: "Counter".into(),
                actor_id: "account.42".into(),
            }),
            "objects/project-1/Counter/account.42.ndjson"
        );
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
