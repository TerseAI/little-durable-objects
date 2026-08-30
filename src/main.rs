use anyhow::Result;
use durable_object_runtime::{
    control_plane::{ControlPlaneProcessConfig, serve_control_plane},
    host::{ActorHostConfig, serve_actor_host},
    maintenance::{DurabilityMaintenanceConfig, serve_durability_maintenance},
    telemetry::{ActorSystemRole, init_logging},
};
use tokio::io::AsyncReadExt;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    if let Err(error) = init_logging() {
        eprintln!("failed to initialize logging: {error:#}");
        std::process::exit(1);
    }

    if let Err(error) = run().await {
        error!(error = %format!("{error:#}"), "actor process failed");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let shutdown = shutdown_signal();
    let role = match std::env::var("DURABLE_OBJECT_PROCESS_ROLE") {
        Ok(role) => role
            .parse::<ActorSystemRole>()
            .map_err(|role| anyhow::anyhow!("unsupported DURABLE_OBJECT_PROCESS_ROLE {role:?}"))?,
        Err(std::env::VarError::NotPresent) => ActorSystemRole::Host,
        Err(error) => return Err(error.into()),
    };
    match role {
        ActorSystemRole::ControlPlane => {
            serve_control_plane(ControlPlaneProcessConfig::from_env()?, shutdown).await
        }
        ActorSystemRole::Maintenance => {
            serve_durability_maintenance(DurabilityMaintenanceConfig::from_env()?, shutdown).await
        }
        ActorSystemRole::Host => serve_actor_host(ActorHostConfig::from_env()?, shutdown).await,
    }
}

async fn shutdown_signal() {
    if std::env::var_os("DURABLE_OBJECT_PARENT_LIFETIME_STDIN").is_none() {
        wait_for_ctrl_c().await;
        return;
    }

    tokio::select! {
        () = wait_for_ctrl_c() => {},
        () = wait_for_parent_stdin_close() => {
            info!("parent process exited; shutting down actor host");
        }
    }
}

async fn wait_for_ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(error = %error, "failed to listen for shutdown signal");
    } else {
        info!("shutdown signal received");
    }
}

async fn wait_for_parent_stdin_close() {
    let mut stdin = tokio::io::stdin();
    let mut buffer = [0_u8; 1];
    loop {
        match stdin.read(&mut buffer).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) => {
                error!(error = %error, "failed to monitor parent process lifetime");
                return;
            }
        }
    }
}
