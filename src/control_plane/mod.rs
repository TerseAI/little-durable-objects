mod admin;
mod auth;
mod client;
mod issuer;
mod process;
mod protocol;
mod service;

use std::time::Duration;

pub(crate) const SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CONTROL_PLANE_MESSAGE_BYTES: usize = SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES;
pub(crate) const CONTROL_PLANE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub use self::process::{
    ControlPlaneProcessConfig, ControlPlaneStorageConfig, serve_control_plane,
};
pub(crate) use self::{
    admin::PostgresAdminRegistry,
    auth::{ActorJwtVerifier, ActorTokenPurpose},
    client::ControlPlaneClient,
    issuer::ActorJwtIssuer,
    service::ControlPlaneService,
};
