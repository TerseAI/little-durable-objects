//! Credentialless actor-host process lifecycle.

use std::{
    env,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tokio::{net::TcpListener, process::Command, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::{error, info};

use crate::{
    actor::{ActorExecutorListener, ActorScope},
    actor_state::ActorDatabaseStore,
    control_plane::{ActorJwtVerifier, ActorTokenPurpose, ControlPlaneClient},
    durability::{ActorDurabilityStore, LocalActorChangeCapture, LtxActorStateRestorer},
    grpc::ActorHostGrpcService,
    host_leases::{HostLeaseStore, MAX_HOST_LEASE_DURATION_MS},
    telemetry::{
        ACTOR_TELEMETRY_SHUTDOWN_TIMEOUT, ActorHostStartupTelemetry, ActorProcessHealthTelemetry,
        ActorSystemRole, ActorTelemetry, ActorTelemetryEvent, ActorTelemetryScope,
        ControlPlaneActorTelemetry, elapsed_ms,
    },
};

use super::{
    ActorDrainReason, ActorHostDependencies, ActorProcessRole, HOST_ACTOR_DRAIN_TIMEOUT,
    HostEndpoint, LeasedActorHost,
    credentials::{HostCredentialIssuer, HostCredentialRequest, IssuedHostCredentials},
};

const HOST_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const HOST_HEALTH_INTERVAL: Duration = Duration::from_secs(60);

/// Configuration injected into one sandbox. The bootstrap credential is exchanged
/// for short-lived JWTs and is never sent to the durable-object control plane.
pub struct ActorHostConfig {
    pub credentials_url: String,
    pub control_plane_url: String,
    pub credential: String,
    pub host_id: Option<super::HostId>,
    pub session_id: Option<String>,
    pub region: String,
    pub code_revision: Option<String>,
    pub local_root: PathBuf,
    pub executor_socket: PathBuf,
    pub host_bind: SocketAddr,
    pub host_route: Option<String>,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub jwt_max_lifetime: Duration,
    pub lease_duration: Duration,
    pub renew_every: Duration,
}

impl ActorHostConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let credentials_url = required(&mut get, "DURABLE_OBJECT_CREDENTIALS_URL")?;
        let control_plane_url = required(&mut get, "DURABLE_OBJECT_CONTROL_PLANE_URL")?;
        let credential = required(&mut get, "DURABLE_OBJECT_CREDENTIAL")?;
        let host_id = get("DURABLE_OBJECT_HOST_ID").map(super::HostId::new);
        let session_id = get("DURABLE_OBJECT_SESSION_ID");
        ensure!(
            host_id.is_some() == session_id.is_some(),
            "DURABLE_OBJECT_HOST_ID and DURABLE_OBJECT_SESSION_ID must be set together"
        );
        if let Some(session_id) = &session_id {
            uuid::Uuid::parse_str(session_id)
                .context("DURABLE_OBJECT_SESSION_ID must be a UUID")?;
        }
        let region = get("DURABLE_OBJECT_REGION").unwrap_or_else(|| "default".into());
        ensure!(valid_region(&region), "DURABLE_OBJECT_REGION is invalid");
        let code_revision = get("DURABLE_OBJECT_CODE_REVISION");
        let local_root: PathBuf = required(&mut get, "DURABLE_OBJECT_LOCAL_ROOT")?.into();
        let executor_socket = get("DURABLE_OBJECT_EXECUTOR_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| local_root.join("actor-session.sock"));
        let host_route = get("DURABLE_OBJECT_HOST_ROUTE");
        if let Some(route) = &host_route {
            tonic::transport::Endpoint::from_shared(route.clone())
                .context("DURABLE_OBJECT_HOST_ROUTE must be a valid HTTP or HTTPS URI")?;
        }
        let default_host_bind = if host_route.is_some() {
            "0.0.0.0:7101"
        } else {
            "127.0.0.1:0"
        };
        let host_bind = get("DURABLE_OBJECT_HOST_BIND")
            .unwrap_or_else(|| default_host_bind.into())
            .parse()
            .context("DURABLE_OBJECT_HOST_BIND must be a socket address")?;
        let jwt_issuer = get("DURABLE_OBJECT_JWT_ISSUER")
            .unwrap_or_else(|| "durable-object-control-plane".into());
        let invocation_jwt_audience = get("DURABLE_OBJECT_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| "durable-object-invoke".into());
        let jwt_max_lifetime =
            duration_seconds(&mut get, "DURABLE_OBJECT_JWT_MAX_TTL_SECONDS", 1_800)?;
        let lease_duration = duration_ms(&mut get, "DURABLE_OBJECT_LEASE_MS", 30_000)?;
        let renew_every = duration_ms(&mut get, "DURABLE_OBJECT_RENEW_MS", 10_000)?;
        ensure!(
            lease_duration.as_millis() <= u128::from(MAX_HOST_LEASE_DURATION_MS),
            "DURABLE_OBJECT_LEASE_MS must not exceed {MAX_HOST_LEASE_DURATION_MS}"
        );
        ensure!(
            renew_every < lease_duration,
            "DURABLE_OBJECT_RENEW_MS must be shorter than DURABLE_OBJECT_LEASE_MS"
        );
        Ok(Self {
            credentials_url,
            control_plane_url,
            credential,
            host_id,
            session_id,
            region,
            code_revision,
            local_root,
            executor_socket,
            host_bind,
            host_route,
            jwt_issuer,
            invocation_jwt_audience,
            jwt_max_lifetime,
            lease_duration,
            renew_every,
        })
    }
}

pub async fn serve_actor_host<F>(config: ActorHostConfig, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let process_started = Instant::now();
    let mut host_process = start_host_process(config, process_started).await?;
    let stop = supervise_host(&mut host_process, shutdown).await;
    stop_host(host_process, stop).await
}

type HostTask = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

struct AuthenticatedHost {
    token_issuer: HostCredentialIssuer,
    issued: IssuedHostCredentials,
    credential_request: HostCredentialRequest,
    control_plane: Arc<ControlPlaneClient>,
    actor_scope: ActorScope,
    invocation_auth: ActorJwtVerifier,
    telemetry: Arc<dyn ActorTelemetry>,
    token_exchange_ms: f64,
    control_plane_connect_ms: f64,
}

struct StartedActorHost {
    leased_host: LeasedActorHost,
    shutdown: watch::Sender<bool>,
    host_server: HostTask,
    executor_connection: HostTask,
    javascript_process: HostTask,
    host_route: String,
    host_bind_ms: f64,
    initial_lease_ms: f64,
}

struct ActorHostProcess {
    host_id: super::HostId,
    process_started: Instant,
    telemetry_scope: ActorTelemetryScope,
    telemetry: Arc<dyn ActorTelemetry>,
    leased_host: LeasedActorHost,
    shutdown: watch::Sender<bool>,
    host_server: HostTask,
    executor_connection: HostTask,
    javascript_process: HostTask,
    token_refresh: HostTask,
    lease_lost: watch::Receiver<bool>,
}

async fn authenticate_host(config: &ActorHostConfig) -> Result<AuthenticatedHost> {
    let phase_started = Instant::now();
    let token_issuer =
        HostCredentialIssuer::new(&config.credentials_url, config.credential.clone())?;
    let request = HostCredentialRequest {
        host_id: config.host_id.clone(),
        session_id: config.session_id.clone(),
        process_role: ActorProcessRole::Host,
        region: config.region.clone(),
        code_revision: config.code_revision.clone(),
    };
    let issued = token_issuer.issue(&request).await?;
    let token_exchange_ms = elapsed_ms(phase_started);
    let credential_request = issued.host_request();

    let phase_started = Instant::now();
    let control_plane = Arc::new(
        ControlPlaneClient::connect(&config.control_plane_url, &issued.control_plane_token).await?,
    );
    let control_plane_connect_ms = elapsed_ms(phase_started);
    let actor_scope = ActorScope {
        namespace_id: issued.namespace_id.clone(),
    };
    actor_scope.validate()?;
    let telemetry: Arc<dyn ActorTelemetry> = Arc::new(ControlPlaneActorTelemetry::new(
        control_plane.telemetry_transport(),
    ));
    control_plane.set_telemetry(
        ActorTelemetryScope::namespace(&actor_scope),
        telemetry.clone(),
    )?;
    ensure!(
        issued.process_role == ActorProcessRole::Host
            && issued.region == config.region
            && issued.code_revision == config.code_revision,
        "control-plane credential placement does not match host configuration"
    );
    let invocation_auth = ActorJwtVerifier::for_scope(
        issued.public_keys_json()?,
        config.jwt_issuer.clone(),
        config.invocation_jwt_audience.clone(),
        ActorTokenPurpose::Invocation,
        config.jwt_max_lifetime,
    )?;

    Ok(AuthenticatedHost {
        token_issuer,
        issued,
        credential_request,
        control_plane,
        actor_scope,
        invocation_auth,
        telemetry,
        token_exchange_ms,
        control_plane_connect_ms,
    })
}

async fn start_host_process(
    config: ActorHostConfig,
    process_started: Instant,
) -> Result<ActorHostProcess> {
    let authenticated = authenticate_host(&config).await?;
    let host = start_actor_host(&config, &authenticated).await?;
    let AuthenticatedHost {
        token_issuer,
        issued,
        credential_request,
        control_plane,
        actor_scope,
        invocation_auth,
        telemetry,
        token_exchange_ms,
        control_plane_connect_ms,
    } = authenticated;
    let StartedActorHost {
        leased_host,
        shutdown,
        host_server,
        executor_connection,
        javascript_process,
        host_route,
        host_bind_ms,
        initial_lease_ms,
    } = host;
    let host_id = issued.host_id.clone();
    let session_id = issued.session_id.clone();

    info!(
        host_id = %host_id,
        namespace_id = %actor_scope.namespace_id,
        token_expires_at_ms = issued.expires_at_ms,
        session_id,
        host_route,
        region = %config.region,
        code_revision = ?config.code_revision,
        actor_socket = %config.executor_socket.display(),
        "durable-object host is ready"
    );
    let telemetry_scope = ActorTelemetryScope::namespace(&actor_scope);
    telemetry.publish(ActorTelemetryEvent::ActorHostStartupFinished(
        ActorHostStartupTelemetry {
            scope: telemetry_scope.clone(),
            role: ActorSystemRole::Host,
            total_ms: elapsed_ms(process_started),
            token_exchange_ms,
            control_plane_connect_ms,
            host_bind_ms,
            initial_lease_ms,
            success: true,
        },
    ));
    publish_host_health(
        telemetry.as_ref(),
        &telemetry_scope,
        process_started,
        true,
        0,
    );
    let token_refresh: HostTask = Box::pin(token_issuer.refresh(
        control_plane,
        invocation_auth,
        credential_request,
        issued.expires_at_ms,
    ));
    let lease_lost = leased_host.lease_lost();

    Ok(ActorHostProcess {
        host_id,
        process_started,
        telemetry_scope,
        telemetry,
        leased_host,
        shutdown,
        host_server,
        executor_connection,
        javascript_process,
        token_refresh,
        lease_lost,
    })
}

async fn start_actor_host(
    config: &ActorHostConfig,
    authenticated: &AuthenticatedHost,
) -> Result<StartedActorHost> {
    let phase_started = Instant::now();
    let host_listener = TcpListener::bind(config.host_bind)
        .await
        .with_context(|| format!("bind actor host server at {}", config.host_bind))?;
    let host_bind_ms = elapsed_ms(phase_started);
    let local_host_address = host_listener.local_addr()?;
    let host_route = config
        .host_route
        .clone()
        .unwrap_or_else(|| format!("http://{local_host_address}"));
    let endpoint = HostEndpoint {
        id: authenticated.issued.host_id.clone(),
        route: host_route.clone(),
    };
    let executor_listener = ActorExecutorListener::bind(&config.executor_socket).await?;
    let javascript_process = spawn_javascript_process()?;
    let executor_connection = executor_listener
        .accept()
        .await
        .context("accept customer JavaScript actor executor connection")?;
    let executor = executor_connection.executor();

    let durability: Arc<dyn ActorDurabilityStore> = authenticated.control_plane.clone();
    let nodes: Arc<dyn HostLeaseStore> = authenticated.control_plane.clone();
    let databases = Arc::new(ActorDatabaseStore::new(&config.local_root));
    let restore = Arc::new(LtxActorStateRestorer::new(
        durability.clone(),
        databases.clone(),
    ));
    let dependencies = ActorHostDependencies::new(
        durability,
        nodes,
        databases,
        Arc::new(LocalActorChangeCapture::new(&config.local_root)),
        restore,
    )
    .with_actor_executor(authenticated.actor_scope.clone(), executor)
    .with_telemetry(authenticated.telemetry.clone());
    let phase_started = Instant::now();
    let leased_host = LeasedActorHost::start(
        endpoint,
        authenticated.issued.session_id.clone(),
        dependencies,
        config.lease_duration,
        config.renew_every,
    )
    .await?;
    let initial_lease_ms = elapsed_ms(phase_started);
    let host_service =
        ActorHostGrpcService::new(leased_host.host(), authenticated.invocation_auth.clone())
            .into_service();
    executor_connection
        .mark_ready()
        .await
        .context("mark customer JavaScript actor executor connection ready")?;

    let (shutdown, shutdown_rx) = watch::channel(false);
    let host_shutdown = shutdown_rx.clone();
    let host_server: HostTask = Box::pin(async move {
        Server::builder()
            .add_service(host_service)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(host_listener),
                wait_for_shutdown(host_shutdown),
            )
            .await
            .context("serve actor host gRPC")
    });
    let executor_connection: HostTask = Box::pin(executor_connection.run(shutdown_rx));
    let javascript_process: HostTask = Box::pin(wait_for_javascript_process(javascript_process));

    Ok(StartedActorHost {
        leased_host,
        shutdown,
        host_server,
        executor_connection,
        javascript_process,
        host_route,
        host_bind_ms,
        initial_lease_ms,
    })
}

async fn supervise_host<F>(host_process: &mut ActorHostProcess, shutdown: F) -> HostStop
where
    F: Future<Output = ()>,
{
    let mut health_ticks = tokio::time::interval_at(
        tokio::time::Instant::now() + HOST_HEALTH_INTERVAL,
        HOST_HEALTH_INTERVAL,
    );
    tokio::pin!(shutdown);
    loop {
        let stop = tokio::select! {
            result = &mut host_process.host_server => Some(HostStop::Grpc(result)),
            result = &mut host_process.executor_connection => Some(HostStop::ExecutorConnection(result)),
            result = &mut host_process.javascript_process => Some(HostStop::JavaScriptProcess(result)),
            result = &mut host_process.token_refresh => Some(HostStop::TokenRefresh(result)),
            result = wait_for_lease_loss(&mut host_process.lease_lost) => Some(HostStop::LeaseLost(result)),
            () = &mut shutdown => Some(HostStop::Requested),
            _ = health_ticks.tick() => {
                publish_host_health(
                    host_process.telemetry.as_ref(),
                    &host_process.telemetry_scope,
                    host_process.process_started,
                    true,
                    host_process.leased_host.consecutive_lease_failures(),
                );
                None
            },
        };
        if let Some(stop) = stop {
            break stop;
        }
    }
}

async fn stop_host(host_process: ActorHostProcess, stop: HostStop) -> Result<()> {
    let ActorHostProcess {
        host_id,
        process_started,
        telemetry_scope,
        telemetry,
        leased_host,
        shutdown,
        host_server,
        executor_connection,
        javascript_process,
        token_refresh,
        lease_lost: _,
    } = host_process;
    let drain_reason = if matches!(&stop, HostStop::LeaseLost(_)) {
        ActorDrainReason::LeaseLost
    } else {
        ActorDrainReason::Shutdown
    };
    let stopped_after_lease_loss = matches!(&stop, HostStop::LeaseLost(_));
    let drain_result = leased_host.drain(drain_reason).await;
    if let Err(error) = &drain_result {
        error!(
            host_id = %host_id,
            timeout_ms = HOST_ACTOR_DRAIN_TIMEOUT.as_millis(),
            error = %format!("{error:#}"),
            "actor invocations did not drain cleanly; closing host transports"
        );
    }
    let _ = shutdown.send(true);

    let serve_result = match stop {
        HostStop::Grpc(host_result) => host_result
            .and(finish_host_task("actor executor connection", executor_connection).await),
        HostStop::ExecutorConnection(actor_result) => {
            actor_result.and(finish_host_task("actor host gRPC server", host_server).await)
        }
        HostStop::JavaScriptProcess(executor_result) => {
            let component_result = finish_host_tasks(host_server, executor_connection).await;
            executor_result.and(component_result)
        }
        HostStop::TokenRefresh(token_result) => {
            let component_result = finish_host_tasks(host_server, executor_connection).await;
            token_result.and(component_result)
        }
        HostStop::LeaseLost(lease_result) => {
            let component_result = finish_host_tasks(host_server, executor_connection).await;
            let lease_result: Result<()> = lease_result.and_then(|()| {
                Err(anyhow::anyhow!(
                    "host lease expired; the process was permanently self-fenced"
                ))
            });
            lease_result.and(component_result)
        }
        HostStop::Requested => finish_host_tasks(host_server, executor_connection).await,
    };
    drop(javascript_process);
    drop(token_refresh);
    if let Err(error) = &serve_result {
        error!(
            host_id = %host_id,
            error = %format!("{error:#}"),
            "sandbox actor host stopped with an error"
        );
    }
    let shutdown_result = leased_host.shutdown().await;
    publish_host_health(
        telemetry.as_ref(),
        &telemetry_scope,
        process_started,
        false,
        u64::from(stopped_after_lease_loss),
    );
    let telemetry_shutdown = telemetry.shutdown(ACTOR_TELEMETRY_SHUTDOWN_TIMEOUT).await;
    info!(host_id = %host_id, "sandbox actor host stopped");
    serve_result?;
    drain_result?;
    shutdown_result?;
    telemetry_shutdown
}

fn publish_host_health(
    telemetry: &dyn ActorTelemetry,
    scope: &ActorTelemetryScope,
    process_started: Instant,
    ready: bool,
    consecutive_failures: u64,
) {
    telemetry.publish(ActorTelemetryEvent::ActorProcessHealth(
        ActorProcessHealthTelemetry {
            scope: scope.clone(),
            role: ActorSystemRole::Host,
            uptime_ms: u64::try_from(process_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            ready,
            consecutive_failures,
            telemetry_dropped_events: telemetry.dropped_events(),
            last_success_age_ms: None,
        },
    ));
}

enum HostStop {
    Grpc(Result<()>),
    ExecutorConnection(Result<()>),
    JavaScriptProcess(Result<()>),
    TokenRefresh(Result<()>),
    LeaseLost(Result<()>),
    Requested,
}

fn spawn_javascript_process() -> Result<tokio::process::Child> {
    let mut command = Command::new("node");
    command
        .args([
            "--input-type=module",
            "--eval",
            "import(\"@terse/durable-objects/host\").then(module => module.runDurableObjectHost())",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command.spawn().context("start JavaScript actor executor")
}

async fn wait_for_javascript_process(mut child: tokio::process::Child) -> Result<()> {
    let status = child
        .wait()
        .await
        .context("wait for JavaScript actor executor")?;
    anyhow::bail!("JavaScript actor executor exited unexpectedly with {status}")
}

async fn finish_host_task(name: &str, component: impl Future<Output = Result<()>>) -> Result<()> {
    finish_host_task_within(name, component, HOST_TASK_SHUTDOWN_TIMEOUT).await
}

async fn finish_host_task_within(
    name: &str,
    component: impl Future<Output = Result<()>>,
    timeout: Duration,
) -> Result<()> {
    match tokio::time::timeout(timeout, component).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!("{name} did not stop within {}ms", timeout.as_millis()),
    }
}

async fn finish_host_tasks(
    host_server: impl Future<Output = Result<()>>,
    executor_connection: impl Future<Output = Result<()>>,
) -> Result<()> {
    finish_host_task(
        "actor host gRPC server and actor executor connection",
        async {
            let (host_result, actor_result) = tokio::join!(host_server, executor_connection);
            host_result.and(actor_result)
        },
    )
    .await
}

async fn wait_for_lease_loss(lease_lost: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *lease_lost.borrow() {
            return Ok(());
        }
        lease_lost
            .changed()
            .await
            .context("host lease renewal monitor stopped unexpectedly")?;
    }
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn valid_region(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn duration_ms(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of milliseconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_millis(value))
}

fn duration_seconds(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of seconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_secs(value))
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() || shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn valid() -> HashMap<String, String> {
        HashMap::from([
            (
                "DURABLE_OBJECT_CREDENTIALS_URL".into(),
                "http://127.0.0.1:3001/sdk/actor-token".into(),
            ),
            (
                "DURABLE_OBJECT_CONTROL_PLANE_URL".into(),
                "http://127.0.0.1:7100".into(),
            ),
            ("DURABLE_OBJECT_CREDENTIAL".into(), "project-token".into()),
            (
                "DURABLE_OBJECT_LOCAL_ROOT".into(),
                "/data/durable-objects".into(),
            ),
        ])
    }

    fn parse(values: &HashMap<String, String>) -> Result<ActorHostConfig> {
        ActorHostConfig::from_lookup(|name| values.get(name).cloned())
    }

    #[test]
    fn configures_a_credentialless_actor_host() -> Result<()> {
        let config = parse(&valid())?;
        assert_eq!(
            config.credentials_url,
            "http://127.0.0.1:3001/sdk/actor-token"
        );
        assert_eq!(config.control_plane_url, "http://127.0.0.1:7100");
        assert_eq!(config.local_root, PathBuf::from("/data/durable-objects"));
        assert_eq!(config.host_bind, "127.0.0.1:0".parse()?);
        assert!(config.host_route.is_none());
        assert_eq!(config.invocation_jwt_audience, "durable-object-invoke");
        assert_eq!(config.jwt_max_lifetime, Duration::from_secs(1_800));
        assert_eq!(
            config.executor_socket,
            PathBuf::from("/data/durable-objects/actor-session.sock")
        );
        assert_eq!(config.lease_duration, Duration::from_secs(30));
        assert_eq!(config.renew_every, Duration::from_secs(10));
        assert_eq!(config.region, "default");
        Ok(())
    }

    #[test]
    fn rejects_missing_credentials_and_invalid_lease_timing() {
        let mut missing_token = valid();
        missing_token.remove("DURABLE_OBJECT_CREDENTIAL");
        assert!(parse(&missing_token).is_err());

        let mut invalid_lease = valid();
        invalid_lease.insert("DURABLE_OBJECT_LEASE_MS".into(), "1000".into());
        invalid_lease.insert("DURABLE_OBJECT_RENEW_MS".into(), "1000".into());
        assert!(parse(&invalid_lease).is_err());

        let mut oversized_lease = valid();
        oversized_lease.insert(
            "DURABLE_OBJECT_LEASE_MS".into(),
            (MAX_HOST_LEASE_DURATION_MS + 1).to_string(),
        );
        assert!(parse(&oversized_lease).is_err());
    }

    #[test]
    fn uses_the_fixed_tunnel_port_when_a_host_origin_is_provisioned() -> Result<()> {
        let mut values = valid();
        values.insert(
            "DURABLE_OBJECT_HOST_ROUTE".into(),
            "https://actor-host_process.example.com".into(),
        );

        let config = parse(&values)?;

        assert_eq!(
            config.host_route.as_deref(),
            Some("https://actor-host_process.example.com")
        );
        assert_eq!(config.host_bind, "0.0.0.0:7101".parse()?);
        Ok(())
    }

    #[tokio::test]
    async fn stuck_host_task_shutdown_returns_a_bounded_error() {
        let error = finish_host_task_within(
            "stuck component",
            std::future::pending(),
            Duration::from_millis(20),
        )
        .await
        .expect_err("a stuck component must time out");

        assert_eq!(
            error.to_string(),
            "stuck component did not stop within 20ms"
        );
    }
}
