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
use tracing::info;

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
const DEFAULT_INVOCATION_JWT_AUDIENCE: &str = "durable-object-invoke";
const DEFAULT_ACTOR_JWT_MAX_TTL_SECONDS: u64 = 1_800;
const CONTROL_PLANE_HEALTH_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_ACTOR_IDLE_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_HOST_IDLE_TIMEOUT_MS: u64 = 300_000;
const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

pub struct ControlPlaneProcessConfig {
    pub bind: SocketAddr,
    pub jwt_signing_key: String,
    pub jwt_key_id: String,
    pub jwt_issuer: String,
    pub jwt_audience: String,
    pub invocation_jwt_audience: String,
    pub jwt_max_lifetime: std::time::Duration,
    pub admin_token: String,
    pub storage: ControlPlaneStorageConfig,
    pub sandbox_provider: Option<SandboxProviderConfig>,
}

pub struct SandboxProviderConfig {
    pub provider_name: String,
    pub command: String,
    pub environment: HashMap<String, String>,
    pub runtime: crate::sandbox::HostSandboxRuntimeConfig,
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
        let jwt_signing_key = required(&mut get, "DURABLE_OBJECT_JWT_SIGNING_KEY")?;
        let jwt_key_id = get("DURABLE_OBJECT_JWT_KEY_ID").unwrap_or_else(|| "primary".into());
        let jwt_issuer =
            get("DURABLE_OBJECT_JWT_ISSUER").unwrap_or_else(|| DEFAULT_ACTOR_JWT_ISSUER.into());
        let jwt_audience = get("DURABLE_OBJECT_AUTHORITY_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_ACTOR_JWT_AUDIENCE.into());
        let invocation_jwt_audience = get("DURABLE_OBJECT_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_INVOCATION_JWT_AUDIENCE.into());
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
        let admin_token = required(&mut get, "DURABLE_OBJECT_ADMIN_TOKEN")?;
        ensure!(
            admin_token.trim() == admin_token,
            "DURABLE_OBJECT_ADMIN_TOKEN must not contain surrounding whitespace"
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
        let sandbox_provider =
            sandbox_provider_config(&mut get, &jwt_issuer, &invocation_jwt_audience)?;
        Ok(Self {
            bind,
            jwt_signing_key,
            jwt_key_id,
            jwt_issuer,
            jwt_audience,
            invocation_jwt_audience,
            jwt_max_lifetime,
            admin_token,
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
    let issuer = super::ActorJwtIssuer::from_base64_pkcs8(
        &config.jwt_signing_key,
        config.jwt_key_id,
        config.jwt_issuer.clone(),
        config.jwt_audience.clone(),
        config.invocation_jwt_audience,
        config.jwt_max_lifetime,
    )?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        config.jwt_issuer,
        config.jwt_audience,
        super::ActorTokenPurpose::ControlPlane,
        config.jwt_max_lifetime,
    )?;
    let (durability, leases, registry) = connect_control_plane_stores(config.storage).await?;
    let mut service = ControlPlaneService::new(durability, leases, auth)
        .with_telemetry(telemetry.clone())
        .with_admin(config.admin_token, registry, issuer)?;
    if let Some(provider) = config.sandbox_provider {
        service = service
            .with_sandbox_provider(Arc::new(crate::sandbox::CommandSandboxProvider::new(
                provider.provider_name,
                provider.command,
                provider.environment,
            )?))
            .with_sandbox_runtime(provider.runtime);
    }
    let admin_service = service.clone().into_admin_service();
    let service = service.into_service();
    let mut server = Server::builder().timeout(CONTROL_PLANE_REQUEST_TIMEOUT);
    info!(bind = %config.bind, "actor control plane is ready");
    publish_control_plane_health(telemetry.as_ref(), process_started, true, 0);
    let (background_shutdown, background_shutdown_rx) = tokio::sync::watch::channel(false);
    let health_task =
        spawn_control_plane_health(telemetry.clone(), process_started, background_shutdown_rx);
    let serve_result = server
        .add_service(service)
        .add_service(admin_service)
        .serve_with_shutdown(config.bind, shutdown)
        .await
        .context("serve actor control plane");
    let _ = background_shutdown.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), health_task).await;
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
) -> Result<(
    Arc<dyn ActorDurabilityStore>,
    Arc<dyn HostLeaseStore>,
    Arc<dyn super::AdminRegistry>,
)> {
    match config {
        ControlPlaneStorageConfig::Local { root } => Ok((
            Arc::new(LocalActorStore::new(root.join("objects"))),
            Arc::new(LocalHostLeaseStore::new(root.join("nodes")).await?),
            Arc::new(super::LocalAdminRegistry::default()),
        )),
        ControlPlaneStorageConfig::Distributed {
            postgres_url,
            rapid_buckets,
            archive_buckets,
        } => {
            let database = PostgresDatabase::connect(&postgres_url).await?;
            let leases = Arc::new(PostgresHostLeaseStore::from_database(database.clone()));
            let registry = Arc::new(super::PostgresAdminRegistry::from_database(
                database.clone(),
            ));
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
            Ok((durability, leases, registry))
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

fn sandbox_provider_config(
    get: &mut impl FnMut(&str) -> Option<String>,
    jwt_issuer: &str,
    invocation_jwt_audience: &str,
) -> Result<Option<SandboxProviderConfig>> {
    let Some(provider_name) = get("DURABLE_OBJECT_SANDBOX_PROVIDER") else {
        ensure!(
            get("DURABLE_OBJECT_SANDBOX_COMMAND").is_none(),
            "DURABLE_OBJECT_SANDBOX_COMMAND requires DURABLE_OBJECT_SANDBOX_PROVIDER"
        );
        ensure!(
            get("MODAL_TOKEN_ID").is_none() && get("MODAL_TOKEN_SECRET").is_none(),
            "Modal credentials require DURABLE_OBJECT_SANDBOX_PROVIDER=modal"
        );
        return Ok(None);
    };
    ensure!(
        !provider_name.is_empty() && provider_name.trim() == provider_name,
        "DURABLE_OBJECT_SANDBOX_PROVIDER must be non-empty without surrounding whitespace"
    );

    let (default_command, environment) = match provider_name.as_str() {
        "modal" => {
            let modal_token_id = provider_credential(get, "MODAL_TOKEN_ID")?;
            let modal_token_secret = provider_credential(get, "MODAL_TOKEN_SECRET")?;
            (
                "terse-durable-objects-modal",
                HashMap::from([
                    ("MODAL_TOKEN_ID".into(), modal_token_id),
                    ("MODAL_TOKEN_SECRET".into(), modal_token_secret),
                ]),
            )
        }
        provider => anyhow::bail!("unsupported DURABLE_OBJECT_SANDBOX_PROVIDER {provider:?}"),
    };
    let control_plane_url = validated_http_url(
        &required(get, "DURABLE_OBJECT_CONTROL_PLANE_URL")?,
        "DURABLE_OBJECT_CONTROL_PLANE_URL",
    )?;

    Ok(Some(SandboxProviderConfig {
        provider_name,
        command: get("DURABLE_OBJECT_SANDBOX_COMMAND").unwrap_or_else(|| default_command.into()),
        environment,
        runtime: crate::sandbox::HostSandboxRuntimeConfig {
            control_plane_url,
            jwt_issuer: jwt_issuer.into(),
            invocation_jwt_audience: invocation_jwt_audience.into(),
            actor_idle_timeout_ms: idle_timeout_ms(
                get,
                "DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS",
                DEFAULT_ACTOR_IDLE_TIMEOUT_MS,
            )?,
            host_idle_timeout_ms: idle_timeout_ms(
                get,
                "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS",
                DEFAULT_HOST_IDLE_TIMEOUT_MS,
            )?,
        },
    }))
}

fn provider_credential(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = required(get, name)?;
    ensure!(
        value.trim() == value,
        "{name} must not contain surrounding whitespace"
    );
    Ok(value)
}

fn idle_timeout_ms(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<u64> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of milliseconds"))?
        .unwrap_or(default);
    ensure!(
        (1..=MAX_IDLE_TIMEOUT_MS).contains(&value),
        "{name} must be between 1 and {MAX_IDLE_TIMEOUT_MS}"
    );
    Ok(value)
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

    fn parse(mut values: HashMap<&str, &str>) -> Result<ControlPlaneProcessConfig> {
        values
            .entry("DURABLE_OBJECT_JWT_SIGNING_KEY")
            .or_insert("c2lnbmluZw==");
        values
            .entry("DURABLE_OBJECT_ADMIN_TOKEN")
            .or_insert("admin-token");
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    }

    #[test]
    fn parses_system_signing_and_admin_credentials() -> Result<()> {
        let config = parse(HashMap::from([("DURABLE_OBJECT_STORAGE", "local")]))?;

        assert_eq!(config.jwt_signing_key, "c2lnbmluZw==");
        assert_eq!(config.jwt_key_id, "primary");
        assert_eq!(config.admin_token, "admin-token");
        Ok(())
    }

    #[test]
    fn configures_control_plane_owned_actor_host_provisioning() -> Result<()> {
        let config = parse(HashMap::from([
            ("DURABLE_OBJECT_STORAGE", "local"),
            ("DURABLE_OBJECT_SANDBOX_PROVIDER", "modal"),
            (
                "DURABLE_OBJECT_CONTROL_PLANE_URL",
                "https://actors.example.com",
            ),
            ("MODAL_TOKEN_ID", "modal-token-id"),
            ("MODAL_TOKEN_SECRET", "modal-token-secret"),
            ("DURABLE_OBJECT_SANDBOX_COMMAND", "./modal-cli"),
            ("DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS", "60000"),
            ("DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS", "300000"),
        ]))?;

        let activation = config.sandbox_provider.expect("sandbox provider config");
        assert_eq!(activation.provider_name, "modal");
        assert_eq!(activation.command, "./modal-cli");
        assert_eq!(activation.environment["MODAL_TOKEN_ID"], "modal-token-id");
        assert_eq!(
            activation.environment["MODAL_TOKEN_SECRET"],
            "modal-token-secret"
        );
        assert_eq!(
            activation.runtime.control_plane_url,
            "https://actors.example.com/"
        );
        assert_eq!(activation.runtime.actor_idle_timeout_ms, 60_000);
        assert_eq!(activation.runtime.host_idle_timeout_ms, 300_000);
        Ok(())
    }

    #[test]
    fn rejects_partial_actor_host_provisioning_configuration() {
        let result = parse(HashMap::from([
            ("DURABLE_OBJECT_STORAGE", "local"),
            ("DURABLE_OBJECT_SANDBOX_PROVIDER", "modal"),
            ("MODAL_TOKEN_ID", "modal-token-id"),
        ]));

        assert!(result.is_err());
    }

    #[test]
    fn rejects_provider_credentials_without_a_global_provider_selection() {
        let result = parse(HashMap::from([
            ("DURABLE_OBJECT_STORAGE", "local"),
            ("MODAL_TOKEN_ID", "modal-token-id"),
            ("MODAL_TOKEN_SECRET", "modal-token-secret"),
        ]));

        assert_eq!(
            result
                .err()
                .expect("provider selection must be explicit")
                .to_string(),
            "Modal credentials require DURABLE_OBJECT_SANDBOX_PROVIDER=modal"
        );
    }

    #[test]
    fn rejects_an_unsupported_global_sandbox_provider() {
        let result = parse(HashMap::from([
            ("DURABLE_OBJECT_STORAGE", "local"),
            ("DURABLE_OBJECT_SANDBOX_PROVIDER", "future-provider"),
        ]));

        assert_eq!(
            result
                .err()
                .expect("unknown provider must fail")
                .to_string(),
            "unsupported DURABLE_OBJECT_SANDBOX_PROVIDER \"future-provider\""
        );
    }

    #[test]
    fn rejects_missing_system_credentials() {
        let values = HashMap::from([("DURABLE_OBJECT_STORAGE", "local")]);
        let result = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        });

        let error = result.err().expect("missing signing key must fail");
        assert!(
            error
                .to_string()
                .contains("DURABLE_OBJECT_JWT_SIGNING_KEY is required")
        );
    }

    #[test]
    fn parses_matching_rapid_and_standard_region_maps() -> Result<()> {
        let config = parse(HashMap::from([
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
