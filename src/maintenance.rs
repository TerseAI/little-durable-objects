use std::{
    collections::HashMap,
    env,
    future::Future,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, error, info};

const DEFAULT_RAPID_GC_GRACE_MS: u64 = 12 * 60 * 60 * 1_000;

use crate::durability::DurabilityMaintenance;
use crate::telemetry::{
    ACTOR_TELEMETRY_SHUTDOWN_TIMEOUT, ActorProcessHealthTelemetry, ActorSystemRole, ActorTelemetry,
    ActorTelemetryEvent, ActorTelemetryScope, DurabilityMaintenanceTelemetry,
    actor_telemetry_from_env, elapsed_ms,
};

pub struct DurabilityMaintenanceConfig {
    pub postgres_url: String,
    pub rapid_buckets: HashMap<String, String>,
    pub archive_buckets: HashMap<String, String>,
    pub poll_every: Duration,
    pub minimum_checkpoint_tail: u64,
    pub max_actors_per_pass: usize,
    pub rapid_gc_grace: Duration,
}

impl DurabilityMaintenanceConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let rapid_buckets = regional_buckets(required(&mut get, "DURABLE_OBJECT_RAPID_BUCKETS")?)?;
        let archive_buckets = archive_buckets(
            required(&mut get, "DURABLE_OBJECT_STANDARD_BUCKETS")?,
            &rapid_buckets,
        )?;
        let config = Self {
            postgres_url: required(&mut get, "DURABLE_OBJECT_POSTGRES_URL")?,
            rapid_buckets,
            archive_buckets,
            poll_every: Duration::from_millis(number(
                &mut get,
                "DURABLE_OBJECT_DURABILITY_POLL_MS",
                5_000,
            )?),
            minimum_checkpoint_tail: number(&mut get, "DURABLE_OBJECT_CHECKPOINT_TAIL_TXIDS", 64)?,
            max_actors_per_pass: usize::try_from(number(
                &mut get,
                "DURABLE_OBJECT_DURABILITY_BATCH_SIZE",
                100,
            )?)
            .context("DURABLE_OBJECT_DURABILITY_BATCH_SIZE is too large")?,
            rapid_gc_grace: Duration::from_millis(nonnegative_number(
                &mut get,
                "DURABLE_OBJECT_RAPID_GC_GRACE_MS",
                DEFAULT_RAPID_GC_GRACE_MS,
            )?),
        };
        ensure!(
            !config.postgres_url.is_empty()
                && !config.rapid_buckets.is_empty()
                && !config.archive_buckets.is_empty(),
            "durability storage configuration must not be empty"
        );
        Ok(config)
    }
}

pub async fn serve_durability_maintenance<F>(
    config: DurabilityMaintenanceConfig,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()>,
{
    let process_started = Instant::now();
    let telemetry = actor_telemetry_from_env()?;
    let maintenance = DurabilityMaintenance::connect(
        &config.postgres_url,
        config.rapid_buckets,
        config.archive_buckets,
        config.minimum_checkpoint_tail,
        config.rapid_gc_grace,
    )
    .await?;
    let mut ticks = interval(config.poll_every);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tokio::pin!(shutdown);
    info!(
        poll_ms = config.poll_every.as_millis(),
        minimum_checkpoint_tail = config.minimum_checkpoint_tail,
        max_actors_per_pass = config.max_actors_per_pass,
        rapid_gc_grace_ms = config.rapid_gc_grace.as_millis(),
        "durability maintenance is ready"
    );
    publish_maintenance_health(telemetry.as_ref(), process_started, true, 0, None);

    let mut consecutive_failures = 0;
    let mut last_success = None;

    loop {
        tokio::select! {
            _ = ticks.tick() => {
                if run_and_report_maintenance(
                    &maintenance,
                    config.max_actors_per_pass,
                    telemetry.as_ref(),
                ).await {
                    consecutive_failures = 0;
                    last_success = Some(Instant::now());
                } else {
                    consecutive_failures += 1;
                }
                publish_maintenance_health(
                    telemetry.as_ref(),
                    process_started,
                    true,
                    consecutive_failures,
                    last_success,
                );
            }
            () = &mut shutdown => {
                info!("durability-maintenance shutdown signal received");
                break;
            }
        }
    }
    publish_maintenance_health(
        telemetry.as_ref(),
        process_started,
        false,
        consecutive_failures,
        last_success,
    );
    telemetry.shutdown(ACTOR_TELEMETRY_SHUTDOWN_TIMEOUT).await
}

async fn run_and_report_maintenance(
    maintenance: &DurabilityMaintenance,
    max_actors_per_pass: usize,
    telemetry: &dyn ActorTelemetry,
) -> bool {
    let started = Instant::now();
    match maintenance.run_once(max_actors_per_pass).await {
        Ok(batch) => {
            let success = batch.failed == 0;
            telemetry.publish(ActorTelemetryEvent::DurabilityMaintenanceFinished(
                DurabilityMaintenanceTelemetry {
                    scope: ActorTelemetryScope::default(),
                    role: ActorSystemRole::Maintenance,
                    total_ms: elapsed_ms(started),
                    success,
                    objects_attempted: batch.attempted,
                    objects_succeeded: batch.succeeded,
                    objects_failed: batch.failed,
                    archived_logs: batch.archived_logs,
                    checkpoints_installed: batch.checkpoints_installed,
                    rapid_logs_deleted: batch.rapid_logs_deleted,
                    batch_full: batch.attempted >= max_actors_per_pass,
                    error_code: (!success).then(|| "object_maintenance_failed".into()),
                },
            ));
            debug!(
                object_count = batch.succeeded,
                failed_objects = batch.failed,
                archived_logs = batch.archived_logs,
                checkpoints = batch.checkpoints_installed,
                deleted_logs = batch.rapid_logs_deleted,
                "durability maintenance pass completed"
            );
            success
        }
        Err(error) => {
            telemetry.publish(ActorTelemetryEvent::DurabilityMaintenanceFinished(
                DurabilityMaintenanceTelemetry {
                    scope: ActorTelemetryScope::default(),
                    role: ActorSystemRole::Maintenance,
                    total_ms: elapsed_ms(started),
                    success: false,
                    objects_attempted: 0,
                    objects_succeeded: 0,
                    objects_failed: 0,
                    archived_logs: 0,
                    checkpoints_installed: 0,
                    rapid_logs_deleted: 0,
                    batch_full: false,
                    error_code: Some("candidate_discovery_failed".into()),
                },
            ));
            error!(error = %format!("{error:#}"), "durability maintenance pass failed");
            false
        }
    }
}

fn publish_maintenance_health(
    telemetry: &dyn ActorTelemetry,
    process_started: Instant,
    ready: bool,
    consecutive_failures: u64,
    last_success: Option<Instant>,
) {
    telemetry.publish(ActorTelemetryEvent::ActorProcessHealth(
        ActorProcessHealthTelemetry {
            scope: ActorTelemetryScope::default(),
            role: ActorSystemRole::Maintenance,
            uptime_ms: u64::try_from(process_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            ready,
            consecutive_failures,
            telemetry_dropped_events: telemetry.dropped_events(),
            last_success_age_ms: last_success.map(|last_success| {
                u64::try_from(last_success.elapsed().as_millis()).unwrap_or(u64::MAX)
            }),
        },
    ));
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    get(name).with_context(|| format!("{name} is required"))
}

fn number(get: &mut impl FnMut(&str) -> Option<String>, name: &str, default: u64) -> Result<u64> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be a positive integer"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(value)
}

fn nonnegative_number(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<u64> {
    get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be a non-negative integer"))
        .map(|value| value.unwrap_or(default))
}

fn regional_buckets(configured: String) -> Result<HashMap<String, String>> {
    let buckets = serde_json::from_str::<HashMap<String, String>>(&configured)
        .context("DURABLE_OBJECT_RAPID_BUCKETS must be a JSON object of region to bucket name")?;
    ensure!(
        !buckets.is_empty(),
        "DURABLE_OBJECT_RAPID_BUCKETS must not be empty"
    );
    ensure!(
        buckets.iter().all(|(region, bucket)| {
            valid_region_name(region) && !bucket.trim().is_empty() && bucket.trim() == bucket
        }),
        "DURABLE_OBJECT_RAPID_BUCKETS contains an invalid region or bucket"
    );
    Ok(buckets)
}

fn valid_region_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn archive_buckets(
    configured: String,
    rapid: &HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let buckets = serde_json::from_str::<HashMap<String, String>>(&configured).context(
        "DURABLE_OBJECT_STANDARD_BUCKETS must be a JSON object of actor region to bucket name",
    )?;
    ensure!(
        buckets.len() == rapid.len() && rapid.keys().all(|region| buckets.contains_key(region)),
        "DURABLE_OBJECT_STANDARD_BUCKETS must contain exactly the same actor regions as DURABLE_OBJECT_RAPID_BUCKETS"
    );
    ensure!(
        buckets
            .values()
            .all(|bucket| { !bucket.trim().is_empty() && bucket.trim() == bucket }),
        "DURABLE_OBJECT_STANDARD_BUCKETS contains an invalid bucket"
    );
    Ok(buckets)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn parses_maintenance_defaults() -> Result<()> {
        let values = HashMap::from([
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://database/durable_objects",
            ),
            ("DURABLE_OBJECT_RAPID_BUCKETS", r#"{"default":"rapid"}"#),
            (
                "DURABLE_OBJECT_STANDARD_BUCKETS",
                r#"{"default":"standard"}"#,
            ),
        ]);
        let config = DurabilityMaintenanceConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).to_owned())
        })?;
        assert_eq!(config.poll_every, Duration::from_secs(5));
        assert_eq!(config.minimum_checkpoint_tail, 64);
        assert_eq!(config.max_actors_per_pass, 100);
        assert_eq!(config.rapid_gc_grace, Duration::from_secs(12 * 60 * 60));
        assert_eq!(
            config.archive_buckets,
            HashMap::from([("default".into(), "standard".into())])
        );
        Ok(())
    }

    #[test]
    fn allows_disabling_the_rapid_gc_grace_for_an_end_to_end_test() -> Result<()> {
        let values = HashMap::from([
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://database/durable_objects",
            ),
            ("DURABLE_OBJECT_RAPID_BUCKETS", r#"{"default":"rapid"}"#),
            (
                "DURABLE_OBJECT_STANDARD_BUCKETS",
                r#"{"default":"standard"}"#,
            ),
            ("DURABLE_OBJECT_RAPID_GC_GRACE_MS", "0"),
        ]);
        let config = DurabilityMaintenanceConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).to_owned())
        })?;

        assert!(config.rapid_gc_grace.is_zero());
        Ok(())
    }

    #[test]
    fn parses_a_standard_multi_region_bucket_for_each_actor_region() -> Result<()> {
        let values = HashMap::from([
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://database/durable_objects",
            ),
            (
                "DURABLE_OBJECT_RAPID_BUCKETS",
                r#"{"us-east":"rapid-us-east","eu-west":"rapid-eu-west"}"#,
            ),
            (
                "DURABLE_OBJECT_STANDARD_BUCKETS",
                r#"{"us-east":"checkpoints-us","eu-west":"checkpoints-eu"}"#,
            ),
        ]);

        let config = DurabilityMaintenanceConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).to_owned())
        })?;

        assert_eq!(config.archive_buckets["us-east"], "checkpoints-us");
        assert_eq!(config.archive_buckets["eu-west"], "checkpoints-eu");
        Ok(())
    }

    #[test]
    fn rejects_a_standard_map_that_cannot_cover_every_rapid_region() {
        let values = HashMap::from([
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://database/durable_objects",
            ),
            (
                "DURABLE_OBJECT_RAPID_BUCKETS",
                r#"{"us-east":"rapid-us-east","eu-west":"rapid-eu-west"}"#,
            ),
            (
                "DURABLE_OBJECT_STANDARD_BUCKETS",
                r#"{"us-east":"checkpoints-us"}"#,
            ),
        ]);

        let error = DurabilityMaintenanceConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).to_owned())
        })
        .err()
        .expect("incomplete Standard map must fail");

        assert!(error.to_string().contains("exactly the same actor regions"));
    }

    #[test]
    fn rejects_removed_single_bucket_settings() {
        let values = HashMap::from([
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://database/durable_objects",
            ),
            ("TERSE_RAPID_BUCKET", "rapid"),
            ("TERSE_STANDARD_BUCKET", "standard"),
        ]);

        let error = DurabilityMaintenanceConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).to_owned())
        })
        .err()
        .expect("single-bucket settings must no longer be accepted");

        assert_eq!(
            error.to_string(),
            "DURABLE_OBJECT_RAPID_BUCKETS is required"
        );
    }
}
