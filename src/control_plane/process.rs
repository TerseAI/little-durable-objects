use std::{collections::HashMap, env, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tonic::transport::Server;
use tracing::info;

use crate::{
    host_leases::PostgresHostLeaseStore,
    placement::PostgresObjectPlacementStore,
    postgres::PostgresDatabase,
    sandbox::{CommandSandboxProvider, HostSandboxRuntimeConfig},
    storage_urls::{GcsStorageUrlSigner, validate_buckets},
};

use super::{ActorJwtVerifier, ControlPlaneService};

const DEFAULT_JWT_ISSUER: &str = "durable-object-control-plane";
const DEFAULT_AUTHORITY_AUDIENCE: &str = "durable-object-authority";
const DEFAULT_INVOCATION_AUDIENCE: &str = "durable-object-invoke";
const DEFAULT_JWT_TTL_SECONDS: u64 = 1_800;
const DEFAULT_ACTOR_IDLE_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_HOST_IDLE_TIMEOUT_MS: u64 = 300_000;
const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

pub struct ControlPlaneProcessConfig {
    pub bind: SocketAddr,
    pub jwt_signing_key: String,
    pub jwt_key_id: String,
    pub jwt_issuer: String,
    pub authority_audience: String,
    pub invocation_audience: String,
    pub jwt_max_lifetime: Duration,
    pub admin_token: String,
    pub storage: ControlPlaneStorageConfig,
    pub sandbox_provider: Option<SandboxProviderConfig>,
}

pub struct ControlPlaneStorageConfig {
    pub postgres_url: String,
    pub standard_buckets: HashMap<String, String>,
}

pub struct SandboxProviderConfig {
    pub provider_name: String,
    pub command: String,
    pub environment: HashMap<String, String>,
    pub runtime: HostSandboxRuntimeConfig,
}

impl ControlPlaneProcessConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }
}

pub async fn serve_control_plane(
    config: ControlPlaneProcessConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let bind = config.bind;
    let routes = control_plane_routes(config).await?;
    info!(bind = %bind, "durable-object control plane is ready");
    Server::builder()
        .accept_http1(true)
        .add_routes(routes)
        .serve_with_shutdown(bind, shutdown)
        .await
        .context("serve durable-object control plane")
}

async fn control_plane_routes(config: ControlPlaneProcessConfig) -> Result<tonic::service::Routes> {
    let issuer = super::ActorJwtIssuer::from_base64_pkcs8(
        &config.jwt_signing_key,
        config.jwt_key_id,
        config.jwt_issuer.clone(),
        config.authority_audience.clone(),
        config.invocation_audience,
        config.jwt_max_lifetime,
    )?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        config.jwt_issuer,
        config.authority_audience,
        super::ActorTokenPurpose::ControlPlane,
        config.jwt_max_lifetime,
    )?;
    let database = PostgresDatabase::connect(&config.storage.postgres_url).await?;
    let leases = Arc::new(PostgresHostLeaseStore::from_database(database.clone()));
    let placements = Arc::new(PostgresObjectPlacementStore::from_database(
        database.clone(),
    ));
    let registry = Arc::new(super::PostgresAdminRegistry::from_database(database));
    let storage_urls = Arc::new(GcsStorageUrlSigner::from_adc(
        config.storage.standard_buckets,
    )?);
    let provisioner = sandbox_provisioner(config.sandbox_provider, &issuer, &leases)?;
    let service = ControlPlaneService::new(leases, placements, storage_urls, auth).with_routing(
        registry.clone(),
        issuer.clone(),
        provisioner,
    );
    let admin = super::admin::AdminService::new(config.admin_token, registry, issuer)?;
    let public_api = super::public_api::router(service.clone(), admin);
    let internal_api = service.into_internal_service();
    Ok(tonic::service::Routes::from(public_api).add_service(internal_api))
}

fn sandbox_provisioner(
    config: Option<SandboxProviderConfig>,
    issuer: &super::ActorJwtIssuer,
    leases: &Arc<PostgresHostLeaseStore>,
) -> Result<Option<Arc<dyn super::service::HostProvisioner>>> {
    config
        .map(
            |config| -> Result<Arc<dyn super::service::HostProvisioner>> {
                let provider = Arc::new(CommandSandboxProvider::new(
                    config.provider_name,
                    config.command,
                    config.environment,
                )?);
                Ok(Arc::new(super::service::SandboxHostProvisioner::new(
                    provider,
                    config.runtime,
                    issuer.clone(),
                    leases.clone(),
                )))
            },
        )
        .transpose()
}

impl ControlPlaneProcessConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind = get("DURABLE_OBJECT_CONTROL_PLANE_BIND")
            .unwrap_or_else(|| "127.0.0.1:7100".into())
            .parse()
            .context("DURABLE_OBJECT_CONTROL_PLANE_BIND must be a socket address")?;
        let jwt_signing_key = required(&mut get, "DURABLE_OBJECT_JWT_SIGNING_KEY")?;
        let jwt_key_id = get("DURABLE_OBJECT_JWT_KEY_ID").unwrap_or_else(|| "primary".into());
        let jwt_issuer =
            get("DURABLE_OBJECT_JWT_ISSUER").unwrap_or_else(|| DEFAULT_JWT_ISSUER.into());
        let authority_audience = get("DURABLE_OBJECT_AUTHORITY_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_AUTHORITY_AUDIENCE.into());
        let invocation_audience = get("DURABLE_OBJECT_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_INVOCATION_AUDIENCE.into());
        let jwt_max_lifetime = Duration::from_secs(
            get("DURABLE_OBJECT_JWT_MAX_TTL_SECONDS")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_OBJECT_JWT_MAX_TTL_SECONDS must be an integer")?
                .unwrap_or(DEFAULT_JWT_TTL_SECONDS),
        );
        ensure!(
            !jwt_max_lifetime.is_zero(),
            "DURABLE_OBJECT_JWT_MAX_TTL_SECONDS must be positive"
        );
        let admin_token = required(&mut get, "DURABLE_OBJECT_ADMIN_TOKEN")?;
        ensure!(
            admin_token.trim() == admin_token,
            "DURABLE_OBJECT_ADMIN_TOKEN has surrounding whitespace"
        );
        let standard_buckets: HashMap<String, String> =
            serde_json::from_str(&required(&mut get, "DURABLE_OBJECT_STANDARD_BUCKETS")?)
                .context("DURABLE_OBJECT_STANDARD_BUCKETS must be a JSON region-to-bucket map")?;
        validate_buckets(&standard_buckets)?;
        let storage = ControlPlaneStorageConfig {
            postgres_url: required(&mut get, "DURABLE_OBJECT_POSTGRES_URL")?,
            standard_buckets,
        };
        let sandbox_provider =
            sandbox_provider_config(&mut get, &jwt_issuer, &invocation_audience)?;
        Ok(Self {
            bind,
            jwt_signing_key,
            jwt_key_id,
            jwt_issuer,
            authority_audience,
            invocation_audience,
            jwt_max_lifetime,
            admin_token,
            storage,
            sandbox_provider,
        })
    }
}

fn sandbox_provider_config(
    get: &mut impl FnMut(&str) -> Option<String>,
    jwt_issuer: &str,
    invocation_audience: &str,
) -> Result<Option<SandboxProviderConfig>> {
    let Some(provider_name) = get("DURABLE_OBJECT_SANDBOX_PROVIDER") else {
        ensure!(
            get("DURABLE_OBJECT_SANDBOX_COMMAND").is_none(),
            "sandbox command requires a provider"
        );
        return Ok(None);
    };
    ensure!(
        provider_name == "modal",
        "unsupported sandbox provider {provider_name:?}"
    );
    let environment = HashMap::from([
        (
            "MODAL_TOKEN_ID".into(),
            provider_credential(get, "MODAL_TOKEN_ID")?,
        ),
        (
            "MODAL_TOKEN_SECRET".into(),
            provider_credential(get, "MODAL_TOKEN_SECRET")?,
        ),
    ]);
    let control_plane_url = validated_http_url(
        &required(get, "DURABLE_OBJECT_CONTROL_PLANE_URL")?,
        "DURABLE_OBJECT_CONTROL_PLANE_URL",
    )?;
    Ok(Some(SandboxProviderConfig {
        provider_name,
        command: get("DURABLE_OBJECT_SANDBOX_COMMAND")
            .unwrap_or_else(|| "little-durable-objects-modal".into()),
        environment,
        runtime: HostSandboxRuntimeConfig {
            control_plane_url,
            jwt_issuer: jwt_issuer.into(),
            invocation_jwt_audience: invocation_audience.into(),
            actor_idle_timeout_ms: idle_timeout(
                get,
                "DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS",
                DEFAULT_ACTOR_IDLE_TIMEOUT_MS,
            )?,
            host_idle_timeout_ms: idle_timeout(
                get,
                "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS",
                DEFAULT_HOST_IDLE_TIMEOUT_MS,
            )?,
        },
    }))
}

fn provider_credential(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = required(get, name)?;
    ensure!(value.trim() == value, "{name} has surrounding whitespace");
    Ok(value)
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn validated_http_url(value: &str, name: &str) -> Result<String> {
    let url = reqwest::Url::parse(value).with_context(|| format!("{name} must be a URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "{name} must be HTTP or HTTPS"
    );
    Ok(url.to_string())
}

fn idle_timeout(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<u64> {
    let value = get(name)
        .map(|value| value.parse())
        .transpose()
        .with_context(|| format!("{name} must be an integer"))?
        .unwrap_or(default);
    ensure!(
        (1..=MAX_IDLE_TIMEOUT_MS).contains(&value),
        "{name} is outside the supported range"
    );
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_minimal_storage_configuration() -> Result<()> {
        let values = HashMap::from([
            ("DURABLE_OBJECT_JWT_SIGNING_KEY", "c2lnbmluZw=="),
            ("DURABLE_OBJECT_ADMIN_TOKEN", "admin-token"),
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://localhost/actors",
            ),
            (
                "DURABLE_OBJECT_STANDARD_BUCKETS",
                "{\"us-east\":\"actor-state-test\"}",
            ),
        ]);
        let config = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        })?;
        assert_eq!(
            config.storage.standard_buckets["us-east"],
            "actor-state-test"
        );
        Ok(())
    }
}
