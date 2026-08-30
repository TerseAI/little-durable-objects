pub(crate) mod control_plane;
#[cfg(test)]
mod local;
mod posthog;

use std::{env, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing_subscriber::EnvFilter;

use crate::actor::ActorScope;

pub(crate) use self::control_plane::ControlPlaneActorTelemetry;
#[cfg(test)]
pub(crate) use self::local::LocalActorTelemetry;
use self::posthog::PostHogActorTelemetry;

const DEFAULT_LOG_FILTER: &str = "warn,durable_object_runtime=info";
const ACTOR_TELEMETRY_SCHEMA_VERSION: u32 = 1;
pub(crate) const ACTOR_TELEMETRY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorSystemRole {
    ControlPlane,
    Maintenance,
    Host,
}

impl ActorSystemRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlane => "control_plane",
            Self::Maintenance => "maintenance",
            Self::Host => "host",
        }
    }
}

impl std::str::FromStr for ActorSystemRole {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "control-plane" => Ok(Self::ControlPlane),
            "maintenance" => Ok(Self::Maintenance),
            "host" => Ok(Self::Host),
            value => Err(value.to_owned()),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorTelemetryScope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
}

impl ActorTelemetryScope {
    pub(crate) fn namespace(scope: &ActorScope) -> Self {
        Self {
            namespace_id: Some(scope.namespace_id.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorExecutionKind {
    ColdRead,
    ColdWrite,
    HotRead,
    HotWrite,
    ReceiptReplay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneOperation {
    RegisterLease,
    GetLeaseStatus,
    UnregisterLease,
    GetManifest,
    Claim,
    Publish,
    Recovery,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostStartupTelemetry {
    #[serde(flatten)]
    pub scope: ActorTelemetryScope,
    pub role: ActorSystemRole,
    pub total_ms: f64,
    pub token_exchange_ms: f64,
    #[serde(rename = "authorityConnectMs")]
    pub control_plane_connect_ms: f64,
    #[serde(rename = "peerBindMs")]
    pub host_bind_ms: f64,
    pub initial_lease_ms: f64,
    pub success: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorExecutionTelemetry {
    #[serde(flatten)]
    pub scope: ActorTelemetryScope,
    pub role: ActorSystemRole,
    pub total_ms: f64,
    pub queue_wait_ms: f64,
    #[serde(rename = "objectReadyMs")]
    pub actor_ready_ms: f64,
    pub executor_ms: f64,
    pub capture_ms: f64,
    pub publish_ms: f64,
    pub checkpoint_ms: f64,
    pub cold_start: bool,
    pub state_changed: bool,
    pub receipt_replay: bool,
    pub execution_kind: ActorExecutionKind,
    pub success: bool,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    pub actor_type: String,
    pub method: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneRequestTelemetry {
    #[serde(flatten)]
    pub scope: ActorTelemetryScope,
    pub role: ActorSystemRole,
    pub operation: ControlPlaneOperation,
    pub total_ms: f64,
    pub success: bool,
    pub timed_out: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_code: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DurabilityMaintenanceTelemetry {
    #[serde(flatten)]
    pub scope: ActorTelemetryScope,
    pub role: ActorSystemRole,
    pub total_ms: f64,
    pub success: bool,
    pub objects_attempted: usize,
    pub objects_succeeded: usize,
    pub objects_failed: usize,
    pub archived_logs: usize,
    pub checkpoints_installed: usize,
    pub rapid_logs_deleted: usize,
    pub batch_full: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorProcessHealthTelemetry {
    #[serde(flatten)]
    pub scope: ActorTelemetryScope,
    pub role: ActorSystemRole,
    pub uptime_ms: u64,
    pub ready: bool,
    pub consecutive_failures: u64,
    pub telemetry_dropped_events: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_age_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", content = "properties", rename_all = "snake_case")]
pub enum ActorTelemetryEvent {
    #[serde(rename = "actor_host_startup_finished")]
    ActorHostStartupFinished(ActorHostStartupTelemetry),
    ActorExecutionFinished(ActorExecutionTelemetry),
    #[serde(rename = "actor_authority_request_finished")]
    ControlPlaneRequestFinished(ControlPlaneRequestTelemetry),
    #[serde(rename = "actor_durability_pass_finished")]
    DurabilityMaintenanceFinished(DurabilityMaintenanceTelemetry),
    ActorProcessHealth(ActorProcessHealthTelemetry),
}

impl ActorTelemetryEvent {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::ActorHostStartupFinished(_) => "actor_host_startup_finished",
            Self::ActorExecutionFinished(_) => "actor_execution_finished",
            Self::ControlPlaneRequestFinished(_) => "actor_authority_request_finished",
            Self::DurabilityMaintenanceFinished(_) => "actor_durability_pass_finished",
            Self::ActorProcessHealth(_) => "actor_process_health",
        }
    }

    pub(crate) fn scope(&self) -> &ActorTelemetryScope {
        match self {
            Self::ActorHostStartupFinished(event) => &event.scope,
            Self::ActorExecutionFinished(event) => &event.scope,
            Self::ControlPlaneRequestFinished(event) => &event.scope,
            Self::DurabilityMaintenanceFinished(event) => &event.scope,
            Self::ActorProcessHealth(event) => &event.scope,
        }
    }

    pub(crate) fn set_scope(&mut self, scope: &ActorScope) {
        let scope = ActorTelemetryScope::namespace(scope);
        match self {
            Self::ActorHostStartupFinished(event) => event.scope = scope,
            Self::ActorExecutionFinished(event) => event.scope = scope,
            Self::ControlPlaneRequestFinished(event) => event.scope = scope,
            Self::DurabilityMaintenanceFinished(event) => event.scope = scope,
            Self::ActorProcessHealth(event) => event.scope = scope,
        }
    }

    pub(crate) fn is_host_event(&self) -> bool {
        match self {
            Self::ActorHostStartupFinished(event) => event.role == ActorSystemRole::Host,
            Self::ActorExecutionFinished(event) => event.role == ActorSystemRole::Host,
            Self::ControlPlaneRequestFinished(event) => event.role == ActorSystemRole::Host,
            Self::ActorProcessHealth(event) => event.role == ActorSystemRole::Host,
            Self::DurabilityMaintenanceFinished(_) => false,
        }
    }

    pub(crate) fn role(&self) -> ActorSystemRole {
        match self {
            Self::ActorHostStartupFinished(event) => event.role,
            Self::ActorExecutionFinished(event) => event.role,
            Self::ControlPlaneRequestFinished(event) => event.role,
            Self::DurabilityMaintenanceFinished(event) => event.role,
            Self::ActorProcessHealth(event) => event.role,
        }
    }

    pub(crate) fn posthog_properties(
        &self,
        environment: &str,
        region: &str,
    ) -> Result<Map<String, Value>> {
        let Value::Object(mut properties) = serde_json::to_value(self)?
            .get("properties")
            .cloned()
            .context("actor telemetry event omitted properties")?
        else {
            anyhow::bail!("actor telemetry properties are not an object")
        };
        properties.insert(
            "schemaVersion".into(),
            ACTOR_TELEMETRY_SCHEMA_VERSION.into(),
        );
        properties.insert("runtimeVersion".into(), env!("CARGO_PKG_VERSION").into());
        properties.insert("environment".into(), environment.into());
        properties.insert("region".into(), region.into());
        Ok(properties)
    }
}

#[async_trait]
pub trait ActorTelemetry: Send + Sync {
    /// Queue an event without making telemetry part of request success or latency.
    fn publish(&self, event: ActorTelemetryEvent);

    fn dropped_events(&self) -> u64 {
        0
    }

    async fn shutdown(&self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
}

struct NoopActorTelemetry;

#[async_trait]
impl ActorTelemetry for NoopActorTelemetry {
    fn publish(&self, _event: ActorTelemetryEvent) {}
}

pub(crate) fn noop_actor_telemetry() -> Arc<dyn ActorTelemetry> {
    Arc::new(NoopActorTelemetry)
}

pub(crate) fn actor_telemetry_from_env() -> Result<Arc<dyn ActorTelemetry>> {
    match env::var("DURABLE_OBJECT_TELEMETRY_EXPORTER") {
        Err(env::VarError::NotPresent) => {
            return Ok(noop_actor_telemetry());
        }
        Ok(exporter) if exporter.is_empty() || exporter == "none" => {
            return Ok(noop_actor_telemetry());
        }
        Ok(exporter) if exporter == "posthog" => {}
        Ok(exporter) => anyhow::bail!("unsupported telemetry exporter {exporter:?}"),
        Err(error) => return Err(error.into()),
    }
    let Some(api_key) = env::var("DURABLE_OBJECT_POSTHOG_API_KEY")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        anyhow::bail!("DURABLE_OBJECT_POSTHOG_API_KEY is required for the PostHog exporter");
    };
    let host = env::var("DURABLE_OBJECT_POSTHOG_HOST")
        .unwrap_or_else(|_| "https://us.i.posthog.com".into());
    let environment =
        env::var("DURABLE_OBJECT_ENVIRONMENT").unwrap_or_else(|_| "development".into());
    let region = env::var("DURABLE_OBJECT_REGION").unwrap_or_else(|_| "unknown".into());
    Ok(Arc::new(PostHogActorTelemetry::new(
        api_key,
        host,
        environment,
        region,
    )?))
}

pub(crate) fn elapsed_ms(started: std::time::Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

pub fn init_logging() -> Result<()> {
    let filter = log_filter(env::var("RUST_LOG").ok().as_deref())?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("install global logging subscriber: {error}"))
}

fn log_filter(configured: Option<&str>) -> Result<EnvFilter> {
    let configured = configured.unwrap_or(DEFAULT_LOG_FILTER);
    EnvFilter::try_new(configured)
        .with_context(|| format!("RUST_LOG contains an invalid filter: {configured}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_roles_use_current_topology_names() -> Result<()> {
        assert_eq!(
            serde_json::to_value(ActorSystemRole::ControlPlane)?,
            "control_plane"
        );
        assert_eq!(
            serde_json::to_value(ActorSystemRole::Maintenance)?,
            "maintenance"
        );
        assert_eq!(serde_json::to_value(ActorSystemRole::Host)?, "host");
        assert_eq!(
            "control-plane".parse::<ActorSystemRole>(),
            Ok(ActorSystemRole::ControlPlane)
        );
        assert_eq!(
            "maintenance".parse::<ActorSystemRole>(),
            Ok(ActorSystemRole::Maintenance)
        );
        assert_eq!("host".parse::<ActorSystemRole>(), Ok(ActorSystemRole::Host));
        assert!("data-plane".parse::<ActorSystemRole>().is_err());
        assert!("node".parse::<ActorSystemRole>().is_err());
        assert!(
            serde_json::from_value::<ActorSystemRole>(serde_json::json!("data_plane")).is_err()
        );
        assert!(serde_json::from_value::<ActorSystemRole>(serde_json::json!("node")).is_err());
        assert!("service".parse::<ActorSystemRole>().is_err());
        assert!("durability".parse::<ActorSystemRole>().is_err());
        Ok(())
    }

    #[test]
    fn defaults_to_info_for_actor_and_warnings_for_dependencies() -> Result<()> {
        let rendered = log_filter(None)?.to_string();

        assert!(rendered.split(',').any(|directive| directive == "warn"));
        assert!(
            rendered
                .split(',')
                .any(|directive| directive == "durable_object_runtime=info")
        );

        Ok(())
    }

    #[test]
    fn accepts_targeted_rust_log_directives() -> Result<()> {
        let rendered =
            log_filter(Some("warn,durable_object_runtime::host::actor_host=debug"))?.to_string();

        assert!(rendered.split(',').any(|directive| directive == "warn"));
        assert!(
            rendered
                .split(',')
                .any(|directive| directive == "durable_object_runtime::host::actor_host=debug")
        );

        Ok(())
    }

    #[test]
    fn rejects_an_invalid_rust_log_directive() {
        let error = log_filter(Some("actor==debug")).expect_err("filter must be rejected");

        assert!(error.to_string().contains("RUST_LOG"));
    }
}
