use std::{
    collections::HashMap,
    env,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tonic::transport::Server;
use tracing::{info, warn};

use crate::{
    durability::{
        ActorDurabilityStore, ArchiveStore, CommitStore, GcsArchiveStore, LocalActorStore,
        PostgresManifestStore, RapidCommitStore, RegionalActorStore, TieredCommitStore,
    },
    host_leases::{HostLeaseStore, LocalHostLeaseStore, PostgresHostLeaseStore},
    postgres::PostgresDatabase,
    telemetry::{
        ACTOR_TELEMETRY_SHUTDOWN_TIMEOUT, ActorProcessHealthTelemetry, ActorSystemRole,
        ActorTelemetry, ActorTelemetryEvent, ActorTelemetryScope, actor_telemetry_from_env,
    },
};

use super::{ActorJwtVerifier, CONTROL_PLANE_REQUEST_TIMEOUT, ControlPlaneService};

const DEFAULT_ACTOR_JWT_ISSUER: &str = "durable-object-control-plane";
const DEFAULT_ACTOR_JWT_AUDIENCE: &str = "durable-object-authority";
const DEFAULT_ACTOR_JWT_MAX_TTL_SECONDS: u64 = 1_800;
const JWKS_INITIAL_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const JWKS_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(250);
const JWKS_MAX_RETRY_DELAY: Duration = Duration::from_secs(2);
const JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
const CONTROL_PLANE_HEALTH_INTERVAL: Duration = Duration::from_secs(60);

pub struct ControlPlaneProcessConfig {
    pub bind: SocketAddr,
    pub jwt_jwks_url: String,
    pub jwt_issuer: String,
    pub jwt_audience: String,
    pub jwt_max_lifetime: std::time::Duration,
    pub storage: ControlPlaneStorageConfig,
    pub sandbox_provider: Option<SandboxProviderConfig>,
}

pub struct SandboxProviderConfig {
    pub url: String,
    pub secret: String,
}

pub enum ControlPlaneStorageConfig {
    Local {
        root: PathBuf,
    },
    Distributed {
        postgres_url: String,
        rapid_buckets: HashMap<String, String>,
        archive_buckets: HashMap<String, String>,
    },
}

impl ControlPlaneProcessConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind = get("DURABLE_OBJECT_CONTROL_PLANE_BIND")
            .unwrap_or_else(|| "127.0.0.1:7100".into())
            .parse()
            .context("DURABLE_OBJECT_CONTROL_PLANE_BIND must be a socket address")?;
        let jwt_jwks_url = validated_http_url(
            &required(&mut get, "DURABLE_OBJECT_JWKS_URL")?,
            "DURABLE_OBJECT_JWKS_URL",
        )?;
        let jwt_issuer =
            get("DURABLE_OBJECT_JWT_ISSUER").unwrap_or_else(|| DEFAULT_ACTOR_JWT_ISSUER.into());
        let jwt_audience = get("DURABLE_OBJECT_AUTHORITY_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_ACTOR_JWT_AUDIENCE.into());
        let jwt_max_lifetime = std::time::Duration::from_secs(
            get("DURABLE_OBJECT_JWT_MAX_TTL_SECONDS")
                .map(|value| value.parse::<u64>())
                .transpose()
                .context("DURABLE_OBJECT_JWT_MAX_TTL_SECONDS must be an integer number of seconds")?
                .unwrap_or(DEFAULT_ACTOR_JWT_MAX_TTL_SECONDS),
        );
        ensure!(
            !jwt_max_lifetime.is_zero(),
            "DURABLE_OBJECT_JWT_MAX_TTL_SECONDS must be positive"
        );
        let storage = match get("DURABLE_OBJECT_STORAGE").as_deref().unwrap_or("rapid") {
            "local" => ControlPlaneStorageConfig::Local {
                root: get("DURABLE_OBJECT_LOCAL_STORE_ROOT")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(".local/durable-objects/control-plane")),
            },
            "rapid" => {
                let rapid_buckets =
                    regional_buckets(required(&mut get, "DURABLE_OBJECT_RAPID_BUCKETS")?)?;
                let archive_buckets = archive_buckets(
                    required(&mut get, "DURABLE_OBJECT_STANDARD_BUCKETS")?,
                    &rapid_buckets,
                )?;
                ControlPlaneStorageConfig::Distributed {
                    postgres_url: required(&mut get, "DURABLE_OBJECT_POSTGRES_URL")?,
                    rapid_buckets,
                    archive_buckets,
                }
            }
            backend => anyhow::bail!("unsupported DURABLE_OBJECT_STORAGE {backend:?}"),
        };
        let sandbox_provider = match (
            get("DURABLE_OBJECT_SANDBOX_PROVIDER_URL"),
            get("DURABLE_OBJECT_SANDBOX_PROVIDER_TOKEN"),
        ) {
            (Some(url), Some(secret)) => {
                ensure!(
                    !secret.trim().is_empty() && secret.trim() == secret,
                    "DURABLE_OBJECT_SANDBOX_PROVIDER_TOKEN must not be empty or contain surrounding whitespace"
                );
                Some(SandboxProviderConfig {
                    url: validated_http_url(&url, "DURABLE_OBJECT_SANDBOX_PROVIDER_URL")?,
                    secret,
                })
            }
            (None, None) => None,
            _ => anyhow::bail!(
                "DURABLE_OBJECT_SANDBOX_PROVIDER_URL and DURABLE_OBJECT_SANDBOX_PROVIDER_TOKEN must both be set"
            ),
        };
        Ok(Self {
            bind,
            jwt_jwks_url,
            jwt_issuer,
            jwt_audience,
            jwt_max_lifetime,
            storage,
            sandbox_provider,
        })
    }
}

pub async fn serve_control_plane(
    config: ControlPlaneProcessConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let process_started = Instant::now();
    let telemetry = actor_telemetry_from_env()?;
    let auth = configure_actor_jwt_verifier(
        &config.jwt_jwks_url,
        &config.jwt_issuer,
        &config.jwt_audience,
        config.jwt_max_lifetime,
    )
    .await?;
    let (durability, leases) = connect_control_plane_stores(config.storage).await?;
    let jwks_refresh_auth = auth.clone();
    let mut service =
        ControlPlaneService::new(durability, leases, auth).with_telemetry(telemetry.clone());
    if let Some(provider) = config.sandbox_provider {
        service = service.with_sandbox_provider(Arc::new(
            crate::sandbox::HttpSandboxProvider::new(provider.url, provider.secret)?,
        ));
    }
    let service = service.into_service();
    let mut server = Server::builder().timeout(CONTROL_PLANE_REQUEST_TIMEOUT);
    info!(bind = %config.bind, "actor control plane is ready");
    publish_control_plane_health(telemetry.as_ref(), process_started, true, 0);
    let (background_shutdown, background_shutdown_rx) = tokio::sync::watch::channel(false);
    let health_task = spawn_control_plane_health(
        telemetry.clone(),
        process_started,
        background_shutdown_rx.clone(),
    );
    let jwks_refresh_task = spawn_jwks_refresh(jwks_refresh_auth, background_shutdown_rx);
    let serve_result = server
        .add_service(service)
        .serve_with_shutdown(config.bind, shutdown)
        .await
        .context("serve actor control plane");
    let _ = background_shutdown.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), health_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), jwks_refresh_task).await;
    publish_control_plane_health(
        telemetry.as_ref(),
        process_started,
        false,
        u64::from(serve_result.is_err()),
    );
    let telemetry_result = telemetry.shutdown(ACTOR_TELEMETRY_SHUTDOWN_TIMEOUT).await;
    serve_result?;
    telemetry_result
}

async fn connect_control_plane_stores(
    config: ControlPlaneStorageConfig,
) -> Result<(Arc<dyn ActorDurabilityStore>, Arc<dyn HostLeaseStore>)> {
    match config {
        ControlPlaneStorageConfig::Local { root } => Ok((
            Arc::new(LocalActorStore::new(root.join("objects"))),
            Arc::new(LocalHostLeaseStore::new(root.join("nodes")).await?),
        )),
        ControlPlaneStorageConfig::Distributed {
            postgres_url,
            rapid_buckets,
            archive_buckets,
        } => {
            let database = PostgresDatabase::connect(&postgres_url).await?;
            let leases = Arc::new(PostgresHostLeaseStore::from_database(database.clone()));
            let archives = connect_archive_stores(archive_buckets).await?;
            let mut commits = HashMap::<String, Arc<dyn CommitStore>>::new();
            for (region, bucket) in rapid_buckets {
                let rapid: Arc<dyn CommitStore> =
                    Arc::new(RapidCommitStore::connect(bucket).await?);
                let archive = archives.get(&region).cloned().with_context(|| {
                    format!("missing Standard bucket for actor region {region:?}")
                })?;
                commits.insert(region, Arc::new(TieredCommitStore::new(rapid, archive)));
            }
            let durability = Arc::new(RegionalActorStore::with_region_stores(
                Arc::new(PostgresManifestStore::from_database(database)),
                commits,
                archives,
            )?);
            Ok((durability, leases))
        }
    }
}

fn spawn_control_plane_health(
    telemetry: Arc<dyn ActorTelemetry>,
    process_started: Instant,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval_at(
            tokio::time::Instant::now() + CONTROL_PLANE_HEALTH_INTERVAL,
            CONTROL_PLANE_HEALTH_INTERVAL,
        );
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = ticks.tick() => {
                    publish_control_plane_health(telemetry.as_ref(), process_started, true, 0);
                }
            }
        }
    })
}

fn spawn_jwks_refresh(
    auth: ActorJwtVerifier,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval_at(
            tokio::time::Instant::now() + JWKS_REFRESH_INTERVAL,
            JWKS_REFRESH_INTERVAL,
        );
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = ticks.tick() => {
                    if let Err(error) = auth.refresh_jwks().await {
                        warn!(
                            error = %format!("{error:#}"),
                            "failed to refresh actor JWKS; retaining the last verified key set"
                        );
                    }
                }
            }
        }
    })
}

async fn configure_actor_jwt_verifier(
    url: &str,
    issuer: &str,
    audience: &str,
    max_lifetime: Duration,
) -> Result<ActorJwtVerifier> {
    let started = Instant::now();
    let mut retry_delay = JWKS_INITIAL_RETRY_DELAY;
    loop {
        match ActorJwtVerifier::from_jwks_url(url, issuer, audience, max_lifetime).await {
            Ok(verifier) => {
                info!(
                    jwks_url = url,
                    "loaded actor JWT verification keys from JWKS"
                );
                return Ok(verifier);
            }
            Err(error) if started.elapsed() >= JWKS_INITIAL_RETRY_TIMEOUT => {
                return Err(error).with_context(|| {
                    format!(
                        "load actor JWT verification keys from {url} within {} seconds",
                        JWKS_INITIAL_RETRY_TIMEOUT.as_secs()
                    )
                });
            }
            Err(error) => {
                warn!(
                    jwks_url = url,
                    retry_in_ms = retry_delay.as_millis(),
                    error = %format!("{error:#}"),
                    "actor JWKS is not available yet"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = retry_delay.saturating_mul(2).min(JWKS_MAX_RETRY_DELAY);
            }
        }
    }
}

fn publish_control_plane_health(
    telemetry: &dyn ActorTelemetry,
    process_started: Instant,
    ready: bool,
    consecutive_failures: u64,
) {
    telemetry.publish(ActorTelemetryEvent::ActorProcessHealth(
        ActorProcessHealthTelemetry {
            scope: ActorTelemetryScope::default(),
            role: ActorSystemRole::ControlPlane,
            uptime_ms: u64::try_from(process_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            ready,
            consecutive_failures,
            telemetry_dropped_events: telemetry.dropped_events(),
            last_success_age_ms: None,
        },
    ));
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn validated_http_url(value: &str, name: &str) -> Result<String> {
    let url = reqwest::Url::parse(value).with_context(|| format!("{name} must be a valid URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "{name} must use HTTP or HTTPS"
    );
    ensure!(url.host_str().is_some(), "{name} must include a host");
    Ok(url.to_string())
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
            !region.is_empty()
                && region.len() <= 64
                && region.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'-')
                })
                && !bucket.trim().is_empty()
                && bucket.trim() == bucket
        }),
        "DURABLE_OBJECT_RAPID_BUCKETS contains an invalid region or bucket"
    );
    Ok(buckets)
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

async fn connect_archive_stores(
    configured: HashMap<String, String>,
) -> Result<HashMap<String, Arc<dyn ArchiveStore>>> {
    let mut by_bucket = HashMap::<String, Arc<dyn ArchiveStore>>::new();
    let mut stores = HashMap::with_capacity(configured.len());
    for (region, bucket) in configured {
        let store = match by_bucket.get(&bucket) {
            Some(store) => store.clone(),
            None => {
                let store: Arc<dyn ArchiveStore> =
                    Arc::new(GcsArchiveStore::connect(bucket.clone()).await?);
                by_bucket.insert(bucket, store.clone());
                store
            }
        };
        stores.insert(region, store);
    }
    Ok(stores)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn parse(values: HashMap<&str, &str>) -> Result<ControlPlaneProcessConfig> {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    }

    #[test]
    fn parses_the_required_jwks_url() -> Result<()> {
        let config = parse(HashMap::from([
            ("DURABLE_OBJECT_STORAGE", "local"),
            (
                "DURABLE_OBJECT_JWKS_URL",
                "https://keys.example.com/actors/jwks.json",
            ),
        ]))?;

        assert_eq!(
            config.jwt_jwks_url,
            "https://keys.example.com/actors/jwks.json"
        );
        Ok(())
    }

    #[test]
    fn configures_control_plane_owned_actor_host_provisioning() -> Result<()> {
        let config = parse(HashMap::from([
            ("DURABLE_OBJECT_STORAGE", "local"),
            (
                "DURABLE_OBJECT_JWKS_URL",
                "https://keys.example.com/actors/jwks.json",
            ),
            (
                "DURABLE_OBJECT_SANDBOX_PROVIDER_URL",
                "https://api.example.com/internal/actor-host/activate",
            ),
            ("DURABLE_OBJECT_SANDBOX_PROVIDER_TOKEN", "activation-secret"),
        ]))?;

        let activation = config.sandbox_provider.expect("sandbox provider config");
        assert_eq!(
            activation.url,
            "https://api.example.com/internal/actor-host/activate"
        );
        assert_eq!(activation.secret, "activation-secret");
        Ok(())
    }

    #[test]
    fn rejects_partial_actor_host_provisioning_configuration() {
        let result = parse(HashMap::from([
            ("DURABLE_OBJECT_STORAGE", "local"),
            (
                "DURABLE_OBJECT_JWKS_URL",
                "https://keys.example.com/actors/jwks.json",
            ),
            (
                "DURABLE_OBJECT_SANDBOX_PROVIDER_URL",
                "https://api.example.com/internal/actor-host/activate",
            ),
        ]));

        assert!(result.is_err());
    }

    #[test]
    fn rejects_a_missing_jwks_url() {
        let result = parse(HashMap::from([("DURABLE_OBJECT_STORAGE", "local")]));

        let error = result.err().expect("missing JWKS URL must fail");
        assert!(
            error
                .to_string()
                .contains("DURABLE_OBJECT_JWKS_URL is required")
        );
    }

    #[test]
    fn parses_matching_rapid_and_standard_region_maps() -> Result<()> {
        let config = parse(HashMap::from([
            (
                "DURABLE_OBJECT_JWKS_URL",
                "https://keys.example.com/actors/jwks.json",
            ),
            ("DURABLE_OBJECT_STORAGE", "rapid"),
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
        ]))?;

        let ControlPlaneStorageConfig::Distributed {
            rapid_buckets,
            archive_buckets,
            ..
        } = config.storage
        else {
            panic!("expected Rapid storage config")
        };
        assert_eq!(rapid_buckets.len(), 2);
        assert_eq!(archive_buckets["eu-west"], "checkpoints-eu");
        Ok(())
    }

    #[test]
    fn rejects_removed_single_bucket_settings() {
        let error = parse(HashMap::from([
            (
                "DURABLE_OBJECT_JWKS_URL",
                "https://keys.example.com/actors/jwks.json",
            ),
            ("DURABLE_OBJECT_STORAGE", "rapid"),
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://database/durable_objects",
            ),
            ("TERSE_RAPID_BUCKET", "rapid"),
            ("TERSE_STANDARD_BUCKET", "standard"),
        ]))
        .err()
        .expect("single-bucket settings must no longer be accepted");

        assert_eq!(
            error.to_string(),
            "DURABLE_OBJECT_RAPID_BUCKETS is required"
        );
    }
}
