use std::{
    env,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tokio::{
    net::{TcpListener, lookup_host},
    process::Command,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info};

use crate::{
    actor::{ActorExecutorConnection, ActorExecutorListener, ActorScope},
    clock::SystemClock,
    control_plane::{ActorJwtVerifier, ActorTokenPurpose, ControlPlaneClient},
    grpc::ActorHostGrpcService,
    host_leases::{HostLeaseRegistry, MAX_HOST_LEASE_DURATION_MS},
    state_transport::HttpStateTransport,
};

use super::{ActorHost, HostEndpoint, HostLeaseMaintainer, LeaseRenewalTask};

const HOST_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const HOST_ACTOR_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_HOST_IDLE_TIMEOUT_MS: u64 = 300_000;
const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

pub struct ActorHostConfig {
    pub control_plane_url: String,
    pub host_token: String,
    pub jwt_public_keys: String,
    pub namespace_id: String,
    pub host_id: super::HostId,
    pub session_id: String,
    pub executor_socket: PathBuf,
    pub host_bind: SocketAddr,
    pub host_route: Option<String>,
    pub private_hostname: Option<String>,
    pub route_file: Option<PathBuf>,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub jwt_max_lifetime: Duration,
    pub lease_duration: Duration,
    pub renew_every: Duration,
    pub host_idle_timeout: Duration,
    startup_started_at: Instant,
    configuration_loaded_at_ms: f64,
}

impl ActorHostConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }
}

pub async fn serve_actor_host<F>(config: ActorHostConfig, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut timings = HostStartupTimings::new(&config);
    let prepared = match prepare_actor_host(&config, &mut timings).await {
        Ok(prepared) => prepared,
        Err(error) => {
            log_startup(&config, &timings, "failed", Some(&error));
            return Err(error);
        }
    };
    let PreparedActorHost {
        invocation_auth,
        listener,
        route,
        executor_connection,
        mut javascript,
        host,
        lease,
        renewal,
    } = prepared;
    let mut lease_lost = renewal.lease_lost();
    let mut activity = host.activity();

    let service = ActorHostGrpcService::new(host.clone(), invocation_auth).into_service();
    if let Err(error) = executor_connection.mark_ready().await {
        log_startup(&config, &timings, "failed", Some(&error));
        return Err(error);
    }
    timings.executor_notified_at_ms = Some(timings.elapsed_ms());
    log_startup(&config, &timings, "ready", None);
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let mut grpc_server = Box::pin(
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                server_stop.cancelled().await
            }),
    );
    let mut server =
        Box::pin(async move { grpc_server.as_mut().await.context("serve actor host gRPC") });
    let mut executor_task = Box::pin(executor_connection.run(stop.clone()));
    tokio::pin!(shutdown);

    info!(host_id = %config.host_id, namespace_id = %config.namespace_id, route, "durable-object host is ready");
    let stop_result = wait_for_host_stop(
        server.as_mut(),
        executor_task.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease_lost,
        &mut activity,
        config.host_idle_timeout,
    )
    .await;
    stop_host_tasks(&host, &stop, server, executor_task).await;
    drop(javascript);
    let renewal_result = renewal.shutdown().await;
    let unregister_result = lease.unregister().await;
    info!(host_id = %config.host_id, "durable-object host stopped");
    stop_result?;
    renewal_result?;
    unregister_result
}

impl ActorHostConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let startup_started_at = Instant::now();
        let control_plane_url = required(&mut get, "DURABLE_OBJECT_CONTROL_PLANE_URL")?;
        let host_token = required(&mut get, "DURABLE_OBJECT_HOST_TOKEN")?;
        let jwt_public_keys = required(&mut get, "DURABLE_OBJECT_JWT_PUBLIC_KEYS")?;
        let namespace_id = required(&mut get, "DURABLE_OBJECT_NAMESPACE_ID")?;
        ActorScope {
            namespace_id: namespace_id.clone(),
        }
        .validate()?;
        let host_id = super::HostId::new(required(&mut get, "DURABLE_OBJECT_HOST_ID")?);
        ensure!(
            host_id
                .as_str()
                .starts_with(&format!("host.v1.{namespace_id}.")),
            "DURABLE_OBJECT_HOST_ID does not belong to DURABLE_OBJECT_NAMESPACE_ID"
        );
        let session_id = required(&mut get, "DURABLE_OBJECT_SESSION_ID")?;
        uuid::Uuid::parse_str(&session_id).context("DURABLE_OBJECT_SESSION_ID must be a UUID")?;
        let executor_socket = get("DURABLE_OBJECT_EXECUTOR_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/durable-object-executor.sock"));
        let host_route = get("DURABLE_OBJECT_HOST_ROUTE");
        if let Some(route) = &host_route {
            tonic::transport::Endpoint::from_shared(route.clone())
                .context("DURABLE_OBJECT_HOST_ROUTE must be a valid HTTP or HTTPS URI")?;
        }
        let private_hostname = get("DURABLE_OBJECT_HOST_PRIVATE_HOSTNAME");
        if let Some(hostname) = &private_hostname {
            ensure!(
                !hostname.is_empty() && hostname.trim() == hostname,
                "DURABLE_OBJECT_HOST_PRIVATE_HOSTNAME must be non-empty without surrounding whitespace"
            );
        }
        ensure!(
            host_route.is_none() || private_hostname.is_none(),
            "DURABLE_OBJECT_HOST_ROUTE and DURABLE_OBJECT_HOST_PRIVATE_HOSTNAME are mutually exclusive"
        );
        let route_file = get("DURABLE_OBJECT_HOST_ROUTE_FILE").map(PathBuf::from);
        let host_bind = get("DURABLE_OBJECT_HOST_BIND")
            .unwrap_or_else(|| {
                if private_hostname.is_some() {
                    "[::]:7101"
                } else if host_route.is_some() {
                    "0.0.0.0:7101"
                } else {
                    "127.0.0.1:0"
                }
                .into()
            })
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
        let host_idle_timeout = duration_ms(
            &mut get,
            "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS",
            DEFAULT_HOST_IDLE_TIMEOUT_MS,
        )?;
        ensure!(
            host_idle_timeout.as_millis() <= u128::from(MAX_IDLE_TIMEOUT_MS),
            "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS is too large"
        );
        ensure!(
            lease_duration.as_millis() <= u128::from(MAX_HOST_LEASE_DURATION_MS),
            "DURABLE_OBJECT_LEASE_MS is too large"
        );
        ensure!(
            renew_every < lease_duration,
            "DURABLE_OBJECT_RENEW_MS must be shorter than DURABLE_OBJECT_LEASE_MS"
        );
        Ok(Self {
            control_plane_url,
            host_token,
            jwt_public_keys,
            namespace_id,
            host_id,
            session_id,
            executor_socket,
            host_bind,
            host_route,
            private_hostname,
            route_file,
            jwt_issuer,
            invocation_jwt_audience,
            jwt_max_lifetime,
            lease_duration,
            renew_every,
            host_idle_timeout,
            configuration_loaded_at_ms: startup_started_at.elapsed().as_secs_f64() * 1_000.0,
            startup_started_at,
        })
    }
}

struct PreparedActorHost {
    invocation_auth: ActorJwtVerifier,
    listener: TcpListener,
    route: String,
    executor_connection: ActorExecutorConnection,
    javascript: tokio::process::Child,
    host: Arc<ActorHost>,
    lease: Arc<HostLeaseMaintainer>,
    renewal: LeaseRenewalTask,
}

async fn prepare_actor_host(
    config: &ActorHostConfig,
    timings: &mut HostStartupTimings,
) -> Result<PreparedActorHost> {
    let invocation_auth = invocation_auth(config)?;
    timings.authentication_ready_at_ms = Some(timings.elapsed_ms());
    let control_plane =
        Arc::new(ControlPlaneClient::connect(&config.control_plane_url, &config.host_token).await?);
    timings.control_plane_connected_at_ms = Some(timings.elapsed_ms());
    let (listener, route, endpoint) = bind_host(config, timings).await?;
    let (executor_connection, javascript) = connect_executor(&config.executor_socket).await?;
    timings.executor_attached_at_ms = Some(timings.elapsed_ms());
    let host = Arc::new(ActorHost::new(
        endpoint.clone(),
        config.namespace_id.clone(),
        executor_connection.executor(),
        control_plane.clone(),
        Arc::new(HttpStateTransport::new()),
    ));
    let lease = Arc::new(HostLeaseMaintainer::new(
        endpoint,
        config.session_id.clone(),
        control_plane as Arc<dyn HostLeaseRegistry>,
        Arc::new(SystemClock),
        config.lease_duration,
        config.renew_every,
    )?);
    let renewal = lease.clone().start().await?;
    timings.lease_registered_at_ms = Some(timings.elapsed_ms());
    Ok(PreparedActorHost {
        invocation_auth,
        listener,
        route,
        executor_connection,
        javascript,
        host,
        lease,
        renewal,
    })
}

fn invocation_auth(config: &ActorHostConfig) -> Result<ActorJwtVerifier> {
    ActorJwtVerifier::for_scope(
        &config.jwt_public_keys,
        config.jwt_issuer.clone(),
        config.invocation_jwt_audience.clone(),
        ActorTokenPurpose::Invocation,
        config.jwt_max_lifetime,
    )
}

async fn bind_host(
    config: &ActorHostConfig,
    timings: &mut HostStartupTimings,
) -> Result<(TcpListener, String, HostEndpoint)> {
    let listener = TcpListener::bind(config.host_bind)
        .await
        .with_context(|| format!("bind actor host at {}", config.host_bind))?;
    timings.listener_bound_at_ms = Some(timings.elapsed_ms());
    let route = advertised_route(config, listener.local_addr()?).await?;
    if let Some(path) = &config.route_file {
        tokio::fs::write(path, &route)
            .await
            .with_context(|| format!("write actor host route to {}", path.display()))?;
    }
    timings.private_route_resolved_at_ms = Some(timings.elapsed_ms());
    let endpoint = HostEndpoint {
        id: config.host_id.clone(),
        route: route.clone(),
    };
    Ok((listener, route, endpoint))
}

struct HostStartupTimings {
    started_at: Instant,
    configuration_loaded_at_ms: f64,
    authentication_ready_at_ms: Option<f64>,
    control_plane_connected_at_ms: Option<f64>,
    listener_bound_at_ms: Option<f64>,
    private_route_resolved_at_ms: Option<f64>,
    executor_attached_at_ms: Option<f64>,
    lease_registered_at_ms: Option<f64>,
    executor_notified_at_ms: Option<f64>,
}

impl HostStartupTimings {
    fn new(config: &ActorHostConfig) -> Self {
        Self {
            started_at: config.startup_started_at,
            configuration_loaded_at_ms: config.configuration_loaded_at_ms,
            authentication_ready_at_ms: None,
            control_plane_connected_at_ms: None,
            listener_bound_at_ms: None,
            private_route_resolved_at_ms: None,
            executor_attached_at_ms: None,
            lease_registered_at_ms: None,
            executor_notified_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64() * 1_000.0
    }
}

fn log_startup(
    config: &ActorHostConfig,
    timings: &HostStartupTimings,
    outcome: &str,
    error: Option<&anyhow::Error>,
) {
    info!(
        event = "actor_host_startup",
        namespace_id = %config.namespace_id,
        host_id = %config.host_id,
        started_at_ms = 0,
        configuration_loaded_at_ms = timings.configuration_loaded_at_ms,
        authentication_ready_at_ms = timings.authentication_ready_at_ms,
        control_plane_connected_at_ms = timings.control_plane_connected_at_ms,
        listener_bound_at_ms = timings.listener_bound_at_ms,
        private_route_resolved_at_ms = timings.private_route_resolved_at_ms,
        executor_attached_at_ms = timings.executor_attached_at_ms,
        lease_registered_at_ms = timings.lease_registered_at_ms,
        executor_notified_at_ms = timings.executor_notified_at_ms,
        completed_at_ms = timings.elapsed_ms(),
        outcome,
        error = error.map(|error| format!("{error:#}")),
        "actor host startup completed"
    );
}

async fn advertised_route(config: &ActorHostConfig, bound: SocketAddr) -> Result<String> {
    if let Some(route) = &config.host_route {
        return Ok(route.clone());
    }
    let Some(hostname) = &config.private_hostname else {
        return Ok(format!("http://{bound}"));
    };
    let address = lookup_host((hostname.as_str(), bound.port()))
        .await
        .with_context(|| format!("resolve actor host private hostname {hostname}"))?
        .find(SocketAddr::is_ipv6)
        .with_context(|| format!("actor host private hostname {hostname} has no IPv6 address"))?;
    Ok(format!("http://{address}"))
}

async fn connect_executor(
    socket: &std::path::Path,
) -> Result<(ActorExecutorConnection, tokio::process::Child)> {
    let listener = ActorExecutorListener::bind(socket).await?;
    let javascript = spawn_javascript_process()?;
    Ok((listener.accept().await?, javascript))
}

async fn wait_for_host_stop<ServerFuture, ExecutorFuture, ShutdownFuture>(
    mut server: std::pin::Pin<&mut ServerFuture>,
    mut executor: std::pin::Pin<&mut ExecutorFuture>,
    javascript: &mut tokio::process::Child,
    mut shutdown: std::pin::Pin<&mut ShutdownFuture>,
    lease_lost: &mut tokio::sync::watch::Receiver<bool>,
    activity: &mut tokio::sync::watch::Receiver<usize>,
    idle_timeout: Duration,
) -> Result<()>
where
    ServerFuture: Future<Output = Result<()>> + ?Sized,
    ExecutorFuture: Future<Output = Result<()>> + ?Sized,
    ShutdownFuture: Future<Output = ()> + ?Sized,
{
    let mut idle_deadline = tokio::time::Instant::now() + idle_timeout;
    loop {
        tokio::select! {
            result = server.as_mut() => break result.context("serve actor host network endpoints"),
            result = executor.as_mut() => break result.context("run JavaScript actor executor"),
            result = javascript.wait() => break Err(anyhow::anyhow!("JavaScript actor executor exited with {}", result?)),
            () = shutdown.as_mut() => break Ok(()),
            changed = lease_lost.changed() => {
                if changed.is_err() || *lease_lost.borrow() {
                    break Err(anyhow::anyhow!("host lease expired; host self-fenced"));
                }
            }
            changed = activity.changed() => {
                if changed.is_err() { break Err(anyhow::anyhow!("actor activity tracker stopped")); }
                if *activity.borrow() == 0 {
                    idle_deadline = tokio::time::Instant::now() + idle_timeout;
                }
            }
            () = tokio::time::sleep_until(idle_deadline), if *activity.borrow() == 0 => break Ok(()),
        }
    }
}

async fn stop_host_tasks(
    host: &ActorHost,
    stop: &CancellationToken,
    server: impl Future,
    executor: impl Future,
) {
    if let Err(error) = host.drain(HOST_ACTOR_DRAIN_TIMEOUT).await {
        error!(error = %format!("{error:#}"), "actor invocations did not drain cleanly");
    }
    stop.cancel();
    let _ = tokio::time::timeout(HOST_TASK_SHUTDOWN_TIMEOUT, async {
        let _ = tokio::join!(server, executor);
    })
    .await;
}

fn spawn_javascript_process() -> Result<tokio::process::Child> {
    Command::new("node")
        .args([
            "--eval",
            "import(\"little-durable-objects/host\").then(module => module.runDurableObjectHost())",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("start JavaScript actor executor")
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn host_needs_no_local_state_directory() -> Result<()> {
        let values = values();
        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
        assert_eq!(
            config.executor_socket,
            PathBuf::from("/tmp/durable-object-executor.sock")
        );
        assert_eq!(config.host_idle_timeout, Duration::from_secs(300));
        Ok(())
    }

    #[test]
    fn private_network_hosts_bind_ipv6_and_publish_their_route() -> Result<()> {
        let mut values = values();
        values.insert(
            "DURABLE_OBJECT_HOST_PRIVATE_HOSTNAME".into(),
            "i6pn.modal.local".into(),
        );
        values.insert(
            "DURABLE_OBJECT_HOST_ROUTE_FILE".into(),
            "/tmp/durable-object-route".into(),
        );

        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;

        assert_eq!(config.host_bind, "[::]:7101".parse()?);
        assert_eq!(config.private_hostname.as_deref(), Some("i6pn.modal.local"));
        assert_eq!(
            config.route_file,
            Some(PathBuf::from("/tmp/durable-object-route"))
        );
        Ok(())
    }

    #[test]
    fn startup_timings_begin_with_only_configuration_loaded() {
        let values = values();
        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
        let timings = HostStartupTimings::new(&config);

        assert!(timings.configuration_loaded_at_ms <= timings.elapsed_ms());
        assert!(timings.control_plane_connected_at_ms.is_none());
        assert!(timings.executor_notified_at_ms.is_none());
    }

    fn values() -> HashMap<String, String> {
        HashMap::from([
            (
                "DURABLE_OBJECT_CONTROL_PLANE_URL".into(),
                "http://127.0.0.1:7100".into(),
            ),
            ("DURABLE_OBJECT_HOST_TOKEN".into(), "host-jwt".into()),
            ("DURABLE_OBJECT_JWT_PUBLIC_KEYS".into(), "{}".into()),
            ("DURABLE_OBJECT_NAMESPACE_ID".into(), "project-1".into()),
            (
                "DURABLE_OBJECT_HOST_ID".into(),
                "host.v1.project-1.revision-1.host-1".into(),
            ),
            (
                "DURABLE_OBJECT_SESSION_ID".into(),
                "00000000-0000-4000-8000-000000000001".into(),
            ),
        ])
    }
}
