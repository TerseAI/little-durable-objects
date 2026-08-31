use std::{
    env, future::Future, net::SocketAddr, path::PathBuf, process::Stdio, sync::Arc, time::Duration,
};

use anyhow::{Context, Result, ensure};
use tokio::{net::TcpListener, process::Command, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::{error, info};

use crate::{
    actor::{ActorExecutorListener, ActorScope},
    clock::SystemClock,
    control_plane::{ActorJwtVerifier, ActorTokenPurpose, ControlPlaneClient},
    grpc::ActorHostGrpcService,
    host_leases::{HostLeaseRegistry, MAX_HOST_LEASE_DURATION_MS},
    state_transport::HttpStateTransport,
};

use super::{ActorHost, HostEndpoint, HostLeaseMaintainer};

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
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub jwt_max_lifetime: Duration,
    pub lease_duration: Duration,
    pub renew_every: Duration,
    pub host_idle_timeout: Duration,
}

impl ActorHostConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
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
        let host_bind = get("DURABLE_OBJECT_HOST_BIND")
            .unwrap_or_else(|| {
                if host_route.is_some() {
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
            jwt_issuer,
            invocation_jwt_audience,
            jwt_max_lifetime,
            lease_duration,
            renew_every,
            host_idle_timeout,
        })
    }
}

pub async fn serve_actor_host<F>(config: ActorHostConfig, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let control_plane =
        Arc::new(ControlPlaneClient::connect(&config.control_plane_url, &config.host_token).await?);
    let invocation_auth = ActorJwtVerifier::for_scope(
        &config.jwt_public_keys,
        config.jwt_issuer.clone(),
        config.invocation_jwt_audience.clone(),
        ActorTokenPurpose::Invocation,
        config.jwt_max_lifetime,
    )?;

    let listener = TcpListener::bind(config.host_bind)
        .await
        .with_context(|| format!("bind actor host at {}", config.host_bind))?;
    let route = config.host_route.clone().unwrap_or_else(|| {
        format!(
            "http://{}",
            listener.local_addr().expect("bound host address")
        )
    });
    let endpoint = HostEndpoint {
        id: config.host_id.clone(),
        route: route.clone(),
    };

    let executor_listener = ActorExecutorListener::bind(&config.executor_socket).await?;
    let mut javascript = spawn_javascript_process()?;
    let executor_connection = executor_listener.accept().await?;
    let executor = executor_connection.executor();
    let host = Arc::new(ActorHost::new(
        endpoint.clone(),
        config.namespace_id.clone(),
        executor,
        control_plane.clone(),
        Arc::new(HttpStateTransport::new()),
    ));

    let lease = Arc::new(HostLeaseMaintainer::new(
        endpoint,
        config.session_id.clone(),
        control_plane.clone() as Arc<dyn HostLeaseRegistry>,
        Arc::new(SystemClock),
        config.lease_duration,
        config.renew_every,
    )?);
    let renewal = lease.clone().start().await?;
    let mut lease_lost = renewal.lease_lost();
    let mut activity = host.activity();

    let service = ActorHostGrpcService::new(host.clone(), invocation_auth).into_service();
    executor_connection.mark_ready().await?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut server = Box::pin(
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                wait_for_shutdown(stop_rx.clone()),
            ),
    );
    let mut executor_task = Box::pin(executor_connection.run(stop_rx));
    let mut idle_deadline = tokio::time::Instant::now() + config.host_idle_timeout;
    tokio::pin!(shutdown);

    info!(host_id = %config.host_id, namespace_id = %config.namespace_id, route, "durable-object host is ready");
    let stop_result: Result<()> = loop {
        tokio::select! {
            result = &mut server => break result.context("serve actor host gRPC"),
            result = &mut executor_task => break result.context("run JavaScript actor executor"),
            result = javascript.wait() => break Err(anyhow::anyhow!("JavaScript actor executor exited with {}", result?)),
            () = &mut shutdown => break Ok(()),
            changed = lease_lost.changed() => {
                if changed.is_err() || *lease_lost.borrow() {
                    break Err(anyhow::anyhow!("host lease expired; host self-fenced"));
                }
            }
            changed = activity.changed() => {
                if changed.is_err() { break Err(anyhow::anyhow!("actor activity tracker stopped")); }
                if *activity.borrow() == 0 {
                    idle_deadline = tokio::time::Instant::now() + config.host_idle_timeout;
                }
            }
            () = tokio::time::sleep_until(idle_deadline), if *activity.borrow() == 0 => break Ok(()),
        }
    };

    if let Err(error) = host.drain(HOST_ACTOR_DRAIN_TIMEOUT).await {
        error!(error = %format!("{error:#}"), "actor invocations did not drain cleanly");
    }
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(HOST_TASK_SHUTDOWN_TIMEOUT, async {
        let _ = tokio::join!(server, executor_task);
    })
    .await;
    drop(javascript);
    let renewal_result = renewal.shutdown().await;
    let unregister_result = lease.unregister().await;
    info!(host_id = %config.host_id, "durable-object host stopped");
    stop_result?;
    renewal_result?;
    unregister_result
}

fn spawn_javascript_process() -> Result<tokio::process::Child> {
    Command::new("node")
        .args([
            "--input-type=module",
            "--eval",
            "import(\"@terse/durable-objects/host\").then(module => module.runDurableObjectHost())",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("start JavaScript actor executor")
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
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
}
