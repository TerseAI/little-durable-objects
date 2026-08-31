mod admin;
mod auth;
mod client;
mod issuer;
mod process;
mod protocol;
mod service;

use std::time::Duration;

/// Actor-executor messages are capped at 16 MiB. Keep enough control-plane envelope
/// headroom for that payload plus protobuf metadata and LTX/SQLite framing.
pub(crate) const SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CONTROL_PLANE_MESSAGE_BYTES: usize = 2 * SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES;
pub(crate) const CONTROL_PLANE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub use self::process::{
    ControlPlaneProcessConfig, ControlPlaneStorageConfig, serve_control_plane,
};
pub(crate) use self::{
    admin::{AdminRegistry, LocalAdminRegistry, PostgresAdminRegistry},
    auth::{ActorJwtVerifier, ActorTokenPurpose},
    client::ControlPlaneClient,
    issuer::ActorJwtIssuer,
    service::ControlPlaneService,
};
