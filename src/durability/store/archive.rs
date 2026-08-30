//! Immutable SQLite checkpoints in Standard multi-region Cloud Storage.
//!
//! This store is intentionally absent from the synchronous commit path. The
//! durability worker uploads a complete SQLite image and only then installs its
//! metadata through the PostgreSQL manifest CAS.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model::Bucket,
};
use tracing::debug;

use super::ArchiveStore;

pub struct GcsArchiveStore {
    client: Storage,
    control: StorageControl,
    bucket: String,
}

impl GcsArchiveStore {
    pub async fn connect(bucket: impl Into<String>) -> Result<Self> {
        let bucket = bucket.into();
        let control = StorageControl::builder().build().await?;
        let metadata = control
            .get_bucket()
            .set_name(bucket_name(&bucket))
            .send()
            .await
            .with_context(|| format!("read GCS Standard bucket metadata for {bucket}"))?;
        validate_standard_bucket(&bucket, &metadata)?;
        Ok(Self {
            client: Storage::builder().build().await?,
            control,
            bucket,
        })
    }

    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut response = match self
            .client
            .read_object(bucket_name(&self.bucket), key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.http_status_code() == Some(404) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await.transpose()? {
            bytes.extend_from_slice(&chunk);
        }
        Ok(Some(bytes))
    }
}

#[async_trait]
impl ArchiveStore for GcsArchiveStore {
    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        debug!(
            bucket = %self.bucket,
            key,
            byte_count = bytes.len(),
            "writing immutable Standard multi-region GCS artifact"
        );
        let result = self
            .client
            .write_object(
                bucket_name(&self.bucket),
                key,
                Bytes::copy_from_slice(bytes),
            )
            .set_if_generation_match(0)
            .send_buffered()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if error.http_status_code() == Some(412) => {
                let existing = self
                    .load(key)
                    .await?
                    .with_context(|| format!("checkpoint disappeared after conflict: {key}"))?;
                ensure!(
                    existing == bytes,
                    "immutable checkpoint already exists with different bytes: {key}"
                );
                Ok(())
            }
            Err(error) => Err(error).with_context(|| format!("write GCS checkpoint {key}")),
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.load(key).await
    }

    async fn replication_grace_elapsed(&self, key: &str, minimum_age: Duration) -> Result<bool> {
        if minimum_age.is_zero() {
            return Ok(true);
        }
        let object = self
            .control
            .get_object()
            .set_bucket(bucket_name(&self.bucket))
            .set_object(key)
            .send()
            .await
            .with_context(|| format!("read Standard artifact metadata {key}"))?;
        let created = object
            .create_time
            .with_context(|| format!("Standard artifact {key} has no creation time"))?;
        let seconds = u64::try_from(created.seconds())
            .with_context(|| format!("Standard artifact {key} has an invalid creation time"))?;
        let nanos = u32::try_from(created.nanos())
            .with_context(|| format!("Standard artifact {key} has an invalid creation time"))?;
        let created_at = UNIX_EPOCH
            .checked_add(Duration::new(seconds, nanos))
            .with_context(|| format!("Standard artifact {key} creation time overflow"))?;
        Ok(replication_grace_has_elapsed(
            created_at,
            SystemTime::now(),
            minimum_age,
        ))
    }
}

fn bucket_name(bucket: &str) -> String {
    format!("projects/_/buckets/{bucket}")
}

fn validate_standard_bucket(name: &str, bucket: &Bucket) -> Result<()> {
    ensure!(
        bucket.storage_class.eq_ignore_ascii_case("STANDARD"),
        "GCS checkpoint bucket {name:?} must use STANDARD storage, found {:?}",
        bucket.storage_class
    );
    ensure!(
        normalized_location_type(&bucket.location_type) == "multiregion",
        "GCS checkpoint bucket {name:?} must be multi-region, found location type {:?}",
        bucket.location_type
    );
    ensure!(
        matches!(
            bucket.location.to_ascii_uppercase().as_str(),
            "US" | "EU" | "ASIA"
        ),
        "GCS checkpoint bucket {name:?} must use the US, EU, or ASIA multi-region, found {:?}",
        bucket.location
    );
    Ok(())
}

fn normalized_location_type(value: &str) -> String {
    value
        .chars()
        .filter(|character| !matches!(character, '-' | '_'))
        .flat_map(char::to_lowercase)
        .collect()
}

fn replication_grace_has_elapsed(
    created_at: SystemTime,
    now: SystemTime,
    minimum_age: Duration,
) -> bool {
    now.duration_since(created_at).unwrap_or_default() >= minimum_age
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_standard_multi_region_bucket_metadata() -> Result<()> {
        let bucket = Bucket::new()
            .set_storage_class("STANDARD")
            .set_location_type("multi-region")
            .set_location("EU");

        validate_standard_bucket("checkpoints-eu", &bucket)
    }

    #[test]
    fn rejects_regional_or_non_standard_checkpoint_buckets() {
        let regional = Bucket::new()
            .set_storage_class("STANDARD")
            .set_location_type("region")
            .set_location("EUROPE-WEST1");
        assert!(
            validate_standard_bucket("regional", &regional)
                .unwrap_err()
                .to_string()
                .contains("must be multi-region")
        );

        let nearline = Bucket::new()
            .set_storage_class("NEARLINE")
            .set_location_type("multi-region")
            .set_location("US");
        assert!(
            validate_standard_bucket("nearline", &nearline)
                .unwrap_err()
                .to_string()
                .contains("must use STANDARD")
        );
    }

    #[test]
    fn replication_grace_retains_fresh_standard_objects() {
        let created_at = UNIX_EPOCH + Duration::from_secs(1_000);

        assert!(!replication_grace_has_elapsed(
            created_at,
            created_at + Duration::from_secs(60),
            Duration::from_secs(12 * 60 * 60),
        ));
        assert!(replication_grace_has_elapsed(
            created_at,
            created_at + Duration::from_secs(12 * 60 * 60),
            Duration::from_secs(12 * 60 * 60),
        ));
    }
}
