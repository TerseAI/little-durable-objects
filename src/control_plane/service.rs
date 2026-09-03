use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::{
    actor::ActorKey,
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest,
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
    },
    host::{ActorProcessRole, HostId},
    host_leases::{HostLease, HostLeaseStore},
    placement::{
        ObjectPlacement, ObjectPlacementStore, PlacementClaim, StateCommit, StateCommitRequest,
    },
    sandbox::{
        EnsureHostRequest, HostSandboxRuntimeConfig, HostTermination, ImageWarmup, SandboxProvider,
        TerminateHostsRequest, WarmImageRequest,
    },
    storage_urls::{StateWriteTicket, StorageUrlSigner, validate_snapshot_object_name},
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminRegistry, AdminService, HostLaunchSpec},
    auth::{ActorJwtVerifier, ActorPrincipal},
    issuer::ActorJwtIssuer,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_command, encode_reply},
};

const HOST_TOKEN_RENEWAL_WINDOW_SECONDS: i64 = 10 * 60;

#[derive(Clone)]
pub struct ControlPlaneService {
    leases: Arc<dyn HostLeaseStore>,
    placements: Arc<dyn ObjectPlacementStore>,
    storage_urls: Arc<dyn StorageUrlSigner>,
    auth: ActorJwtVerifier,
    host_token_issuer: Option<ActorJwtIssuer>,
    routing: Option<RoutingDependencies>,
}

#[derive(Clone)]
struct RoutingDependencies {
    registry: Arc<dyn AdminRegistry>,
    provisioner: Option<Arc<dyn HostProvisioner>>,
}

impl ControlPlaneService {
    pub fn new(
        leases: Arc<dyn HostLeaseStore>,
        placements: Arc<dyn ObjectPlacementStore>,
        storage_urls: Arc<dyn StorageUrlSigner>,
        auth: ActorJwtVerifier,
    ) -> Self {
        Self {
            leases,
            placements,
            storage_urls,
            auth,
            host_token_issuer: None,
            routing: None,
        }
    }

    pub(crate) fn with_routing(
        mut self,
        registry: Arc<dyn AdminRegistry>,
        issuer: ActorJwtIssuer,
        provisioner: Option<Arc<dyn HostProvisioner>>,
    ) -> Self {
        self.host_token_issuer = Some(issuer);
        self.routing = Some(RoutingDependencies {
            registry,
            provisioner,
        });
        self
    }

    pub fn into_internal_service(self) -> ActorControlPlaneServiceServer<Self> {
        ActorControlPlaneServiceServer::new(self)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
    }

    pub(super) fn authenticate_workflow(&self, authorization: &str) -> Result<ActorPrincipal> {
        self.auth.authenticate_authorization(authorization)
    }

    pub(super) async fn register_deployment(
        &self,
        admin: &AdminService,
        spec: &HostLaunchSpec,
    ) -> Result<bool> {
        let previous = admin.current_deployment(&spec.namespace_id).await?;
        let changed = admin.ensure_namespace_and_register_deployment(spec).await?;
        if changed && let Some(previous) = previous {
            self.terminate_deployment_hosts(&previous).await;
        }
        Ok(changed)
    }

    pub(super) fn warm_deployment_image(&self, spec: HostLaunchSpec, region: String) {
        let Some(provisioner) = self
            .routing
            .as_ref()
            .and_then(|routing| routing.provisioner.clone())
        else {
            return;
        };
        if !self.storage_urls.regions().contains(&region) {
            warn!(
                event = "actor_image_warmup",
                namespace_id = %spec.namespace_id,
                code_revision = %spec.code_revision,
                region,
                outcome = "invalid_region",
                "actor image warmup skipped"
            );
            return;
        }
        tokio::spawn(async move {
            let started_at = Instant::now();
            match provisioner.warm_image(&spec, &region).await {
                Ok(warmup) => info!(
                    event = "actor_image_warmup",
                    namespace_id = %spec.namespace_id,
                    code_revision = %spec.code_revision,
                    region,
                    provider = %warmup.provider,
                    provider_resource_id = %warmup.resource_id,
                    provider_total_ms = warmup.total_ms,
                    total_ms = elapsed_ms(started_at),
                    outcome = "warmed",
                    "actor image warmup completed"
                ),
                Err(error) => warn!(
                    event = "actor_image_warmup",
                    namespace_id = %spec.namespace_id,
                    code_revision = %spec.code_revision,
                    region,
                    total_ms = elapsed_ms(started_at),
                    outcome = "failed",
                    error = %format!("{error:#}"),
                    "actor image warmup failed"
                ),
            }
        });
    }

    async fn terminate_deployment_hosts(&self, spec: &HostLaunchSpec) {
        let Some(provisioner) = self
            .routing
            .as_ref()
            .and_then(|routing| routing.provisioner.as_ref())
        else {
            return;
        };
        let started_at = Instant::now();
        match provisioner
            .terminate_hosts(spec, &self.storage_urls.regions())
            .await
        {
            Ok(termination) => info!(
                event = "actor_hosts_terminated",
                namespace_id = %spec.namespace_id,
                code_revision = %spec.code_revision,
                provider = %termination.provider,
                resource_count = termination.resource_ids.len(),
                total_ms = elapsed_ms(started_at),
                outcome = "terminated",
                "replaced deployment hosts terminated"
            ),
            Err(error) => warn!(
                event = "actor_hosts_terminated",
                namespace_id = %spec.namespace_id,
                code_revision = %spec.code_revision,
                total_ms = elapsed_ms(started_at),
                outcome = "failed",
                error = %format!("{error:#}"),
                "replaced deployment hosts could not be terminated"
            ),
        }
    }

    pub(super) async fn resolve_workflow_target(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
    ) -> Result<WorkflowActorTarget> {
        actor.validate()?;
        ensure!(
            principal.process_role == ActorProcessRole::Workflow && principal.scope.contains(actor),
            "workflow token cannot resolve this actor"
        );
        let target = self.route_actor(actor, &principal.region).await?;
        let state_read_url = match &target.placement.state_object {
            Some(object_name) => {
                self.storage_urls
                    .read_url(&target.placement.home_region, object_name)
                    .await?
            }
            None => String::new(),
        };
        let issued = self
            .host_token_issuer
            .as_ref()
            .context("JWT issuer is not configured")?
            .issue_invocation_target(
                actor,
                &target.lease.id,
                &target.lease.session_id,
                &target.spec.code_revision,
                &target.placement.home_region,
                target.placement.owner_epoch,
                target.placement.state_version,
                &state_read_url,
                principal.expires_at,
            )?;
        Ok(WorkflowActorTarget {
            route: target.lease.route,
            token: issued.token,
            owner_epoch: target.placement.owner_epoch,
            state_version: target.placement.state_version,
            state_read_url,
            expires_at_ms: issued.expires_at_ms,
        })
    }
}

pub(super) struct WorkflowActorTarget {
    pub route: String,
    pub token: String,
    pub owner_epoch: u64,
    pub state_version: u64,
    pub state_read_url: String,
    pub expires_at_ms: i64,
}

#[tonic::async_trait]
impl ActorControlPlaneService for ControlPlaneService {
    async fn execute(
        &self,
        request: Request<ControlPlaneRequest>,
    ) -> std::result::Result<Response<ControlPlaneReply>, Status> {
        let principal = self.auth.authenticate(&request).await?;
        let command = decode_command(request.into_inner())
            .map_err(|error| Status::invalid_argument(format!("invalid command: {error:#}")))?;
        let reply = self
            .execute_command(&principal, command)
            .await
            .map_err(failed_precondition)?;
        Ok(Response::new(encode_reply(reply).map_err(internal)?))
    }
}

impl ControlPlaneService {
    async fn execute_command(
        &self,
        principal: &ActorPrincipal,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandReply> {
        ensure!(
            principal.process_role == ActorProcessRole::Host,
            "only hosts may use the internal control-plane API"
        );
        match command {
            ControlPlaneCommand::RegisterLease { request } => {
                self.register_lease(principal, request).await
            }
            ControlPlaneCommand::UnregisterLease { host_id } => {
                self.unregister_lease(principal, host_id).await
            }
            ControlPlaneCommand::PrepareStateWrite {
                actor,
                host_id,
                owner_epoch,
                expected_version,
            } => {
                self.prepare_state_write(principal, actor, host_id, owner_epoch, expected_version)
                    .await
            }
            ControlPlaneCommand::CommitState {
                actor,
                host_id,
                owner_epoch,
                expected_version,
                state_object,
                request_id,
            } => {
                self.commit_state(
                    principal,
                    actor,
                    host_id,
                    owner_epoch,
                    expected_version,
                    state_object,
                    request_id,
                )
                .await
            }
        }
    }

    async fn register_lease(
        &self,
        principal: &ActorPrincipal,
        request: crate::host_leases::HostLeaseRequest,
    ) -> Result<ControlPlaneCommandReply> {
        principal.validate_host_id(request.id.as_str())?;
        ensure!(
            request.session_id == principal.session_id,
            "lease session does not match host token"
        );
        validate_host_route(&request.route)?;
        let lease = self.leases.register(&request).await?;
        Ok(ControlPlaneCommandReply::Lease {
            lease,
            replacement_token: self.replacement_host_token(principal)?,
        })
    }

    async fn unregister_lease(
        &self,
        principal: &ActorPrincipal,
        host_id: HostId,
    ) -> Result<ControlPlaneCommandReply> {
        principal.validate_host_id(host_id.as_str())?;
        self.leases
            .unregister(&host_id, &principal.session_id)
            .await?;
        Ok(ControlPlaneCommandReply::Unit)
    }

    async fn prepare_state_write(
        &self,
        principal: &ActorPrincipal,
        actor: ActorKey,
        host_id: HostId,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<ControlPlaneCommandReply> {
        actor.validate()?;
        ensure!(
            principal.scope.contains(&actor),
            "actor crossed the host namespace"
        );
        principal.validate_host_id(host_id.as_str())?;
        self.require_active_host(principal).await?;
        let placement = self.current_placement(&actor).await?;
        validate_state_owner(principal, &host_id, owner_epoch, &placement)?;
        ensure!(
            placement.state_version == expected_version,
            "actor state version changed before write preparation"
        );
        Ok(ControlPlaneCommandReply::StateWriteTicket {
            ticket: self
                .state_write_ticket(&placement.home_region, &actor, expected_version)
                .await?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_state(
        &self,
        principal: &ActorPrincipal,
        actor: ActorKey,
        host_id: HostId,
        owner_epoch: u64,
        expected_version: u64,
        state_object: String,
        request_id: String,
    ) -> Result<ControlPlaneCommandReply> {
        actor.validate()?;
        ensure!(
            principal.scope.contains(&actor),
            "actor crossed the host namespace"
        );
        principal.validate_host_id(host_id.as_str())?;
        self.require_active_host(principal).await?;
        validate_snapshot_object_name(
            &actor,
            expected_version
                .checked_add(1)
                .context("actor state version overflow")?,
            &state_object,
        )?;
        let committed = self
            .placements
            .commit_state(&StateCommitRequest {
                object: actor.storage_key(),
                owner: host_id,
                session_id: principal.session_id.clone(),
                owner_epoch,
                expected_version,
                state_object,
                request_id,
            })
            .await?;
        let placement = match committed {
            StateCommit::Committed(placement) => placement,
            StateCommit::Current(_) => {
                anyhow::bail!("actor ownership or state version changed before commit")
            }
        };
        let next_write = self
            .state_write_ticket(&placement.home_region, &actor, placement.state_version)
            .await
            .map_err(|error| {
                warn!(
                    event = "actor_state_write_ticket",
                    actor = %actor.storage_key(),
                    state_version = placement.state_version,
                    error = %format!("{error:#}"),
                    "next actor state write ticket could not be prepared"
                );
                error
            })
            .ok();
        Ok(ControlPlaneCommandReply::StateCommitted {
            state_version: placement.state_version,
            next_write,
        })
    }

    async fn state_write_ticket(
        &self,
        region: &str,
        actor: &ActorKey,
        expected_version: u64,
    ) -> Result<StateWriteTicket> {
        self.storage_urls
            .write_ticket(
                region,
                actor,
                expected_version
                    .checked_add(1)
                    .context("actor state version overflow")?,
            )
            .await
    }

    async fn route_actor(&self, actor: &ActorKey, storage_region: &str) -> Result<RoutedActor> {
        let routing = self
            .routing
            .as_ref()
            .context("actor routing is not configured")?;
        let spec = routing
            .registry
            .launch_spec(&actor.namespace_id)
            .await?
            .context("project has no registered actor code")?;
        loop {
            let current = self.placements.get(&actor.storage_key()).await?;
            if let Some(target) = self.active_target(&current, &spec).await? {
                return Ok(target);
            }
            let region = self.target_region(current.as_ref(), storage_region)?;
            let provisioner = routing
                .provisioner
                .as_ref()
                .context("sandbox provider is not configured")?;
            let lease = provisioner.ensure_host(&spec, &region).await?;
            match self
                .placements
                .claim(&actor.storage_key(), current.as_ref(), &lease.id, &region)
                .await?
            {
                PlacementClaim::Acquired(placement) | PlacementClaim::Current(placement)
                    if placement.owner == lease.id =>
                {
                    return Ok(RoutedActor {
                        placement,
                        lease,
                        spec,
                    });
                }
                PlacementClaim::Acquired(_) | PlacementClaim::Current(_) => continue,
            }
        }
    }

    fn replacement_host_token(&self, principal: &ActorPrincipal) -> Result<Option<String>> {
        if principal.expires_at.saturating_sub(unix_seconds()?) > HOST_TOKEN_RENEWAL_WINDOW_SECONDS
        {
            return Ok(None);
        }
        let issuer = self
            .host_token_issuer
            .as_ref()
            .context("JWT issuer is not configured")?;
        Ok(Some(
            issuer
                .issue_host(
                    &principal.scope.namespace_id,
                    &principal.host_id,
                    &principal.session_id,
                    principal
                        .code_revision
                        .as_deref()
                        .context("host token has no code revision")?,
                    &principal.region,
                )?
                .token,
        ))
    }

    async fn require_active_host(&self, principal: &ActorPrincipal) -> Result<HostLease> {
        let status = self.leases.lease_status(&principal.host_id).await?;
        ensure!(status.is_active(), "host lease is not active");
        let lease = status.lease.context("active host lease is missing")?;
        ensure!(
            lease.session_id == principal.session_id,
            "host lease belongs to another session"
        );
        Ok(lease)
    }

    async fn current_placement(&self, actor: &ActorKey) -> Result<ObjectPlacement> {
        self.placements
            .get(&actor.storage_key())
            .await?
            .context("actor has no current placement")
    }

    async fn active_target(
        &self,
        current: &Option<ObjectPlacement>,
        spec: &HostLaunchSpec,
    ) -> Result<Option<RoutedActor>> {
        let Some(placement) = current else {
            return Ok(None);
        };
        if !host_matches_revision(&placement.owner, &spec.namespace_id, &spec.code_revision) {
            return Ok(None);
        }
        let status = self.leases.lease_status(&placement.owner).await?;
        if !status.is_active() {
            return Ok(None);
        }
        Ok(Some(RoutedActor {
            placement: placement.clone(),
            lease: status.lease.context("active lease is missing")?,
            spec: spec.clone(),
        }))
    }

    fn target_region(
        &self,
        current: Option<&ObjectPlacement>,
        storage_region: &str,
    ) -> Result<String> {
        select_target_region(current, storage_region, &self.storage_urls.regions())
    }
}

fn select_target_region(
    current: Option<&ObjectPlacement>,
    storage_region: &str,
    configured_regions: &[String],
) -> Result<String> {
    if let Some(placement) = current {
        return Ok(placement.home_region.clone());
    }
    ensure!(
        configured_regions
            .iter()
            .any(|region| region == storage_region),
        "workflow storage region has no Standard bucket"
    );
    Ok(storage_region.to_owned())
}

#[async_trait]
pub(crate) trait HostProvisioner: Send + Sync {
    async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease>;
    async fn warm_image(&self, spec: &HostLaunchSpec, region: &str) -> Result<ImageWarmup>;
    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination>;
}

pub(crate) struct SandboxHostProvisioner {
    provider: Arc<dyn SandboxProvider>,
    runtime: HostSandboxRuntimeConfig,
    issuer: ActorJwtIssuer,
    leases: Arc<dyn HostLeaseStore>,
}

impl SandboxHostProvisioner {
    pub(crate) fn new(
        provider: Arc<dyn SandboxProvider>,
        runtime: HostSandboxRuntimeConfig,
        issuer: ActorJwtIssuer,
        leases: Arc<dyn HostLeaseStore>,
    ) -> Self {
        Self {
            provider,
            runtime,
            issuer,
            leases,
        }
    }
}

#[async_trait]
impl HostProvisioner for SandboxHostProvisioner {
    async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease> {
        let started_at = Instant::now();
        let request = self.request(spec, region)?;
        let provider_started_at = Instant::now();
        let handle = match self.provider.ensure_host(&request).await {
            Ok(handle) => handle,
            Err(error) => {
                warn!(
                    event = "actor_host_provisioning",
                    namespace_id = %spec.namespace_id,
                    code_revision = %spec.code_revision,
                    region,
                    host_id = %request.host_id,
                    provider_command_ms = elapsed_ms(provider_started_at),
                    total_ms = elapsed_ms(started_at),
                    outcome = "provider_failed",
                    error = %format!("{error:#}"),
                    "actor host provisioning failed"
                );
                return Err(error);
            }
        };
        let provider_command_ms = elapsed_ms(provider_started_at);
        let provisioning = handle.provisioning.clone();
        let lease_started_at = Instant::now();
        let lease = self.active_lease(handle, region).await?;
        let lease_validation_ms = elapsed_ms(lease_started_at);
        info!(
            event = "actor_host_provisioning",
            namespace_id = %spec.namespace_id,
            code_revision = %spec.code_revision,
            region,
            host_id = %lease.id,
            provider = provisioning.as_ref().map(|value| value.provider.as_str()).unwrap_or("unknown"),
            provider_resource_id = provisioning.as_ref().map(|value| value.resource_id.as_str()).unwrap_or(""),
            provider_reused = provisioning.as_ref().is_some_and(|value| value.reused),
            provider_resource_lookup_ms = provisioning.as_ref().map_or(0, |value| value.resource_lookup_ms),
            provider_existing_lookup_ms = provisioning.as_ref().map_or(0, |value| value.existing_lookup_ms),
            provider_create_ms = provisioning.as_ref().map_or(0, |value| value.create_ms),
            provider_placement_ms = provisioning.as_ref().map_or(0, |value| value.placement_ms),
            provider_tunnel_ms = provisioning.as_ref().map_or(0, |value| value.tunnel_ms),
            provider_ready_ms = provisioning.as_ref().map_or(0, |value| value.ready_ms),
            provider_metadata_ms = provisioning.as_ref().map_or(0, |value| value.metadata_ms),
            provider_total_ms = provisioning.as_ref().map_or(0, |value| value.total_ms),
            provider_command_ms,
            lease_validation_ms,
            total_ms = elapsed_ms(started_at),
            outcome = "ready",
            "actor host provisioning completed"
        );
        Ok(lease)
    }

    async fn warm_image(&self, spec: &HostLaunchSpec, region: &str) -> Result<ImageWarmup> {
        self.provider
            .warm_image(&WarmImageRequest {
                namespace_id: spec.namespace_id.clone(),
                code_revision: spec.code_revision.clone(),
                canonical_region: region.to_owned(),
                image_ref: spec.image_ref.clone(),
            })
            .await
    }

    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination> {
        self.provider
            .terminate_hosts(&TerminateHostsRequest {
                namespace_id: spec.namespace_id.clone(),
                code_revision: spec.code_revision.clone(),
                canonical_regions: regions.to_vec(),
            })
            .await
    }
}

impl SandboxHostProvisioner {
    fn request(&self, spec: &HostLaunchSpec, region: &str) -> Result<EnsureHostRequest> {
        let host_id = HostId::new(format!(
            "host.v1.{}.{}.{}",
            spec.namespace_id,
            spec.code_revision,
            uuid::Uuid::new_v4()
        ));
        let session_id = uuid::Uuid::new_v4().to_string();
        let host_token = self
            .issuer
            .issue_host(
                &spec.namespace_id,
                &host_id,
                &session_id,
                &spec.code_revision,
                region,
            )?
            .token;
        Ok(EnsureHostRequest {
            namespace_id: spec.namespace_id.clone(),
            code_revision: spec.code_revision.clone(),
            canonical_region: region.to_owned(),
            host_id,
            session_id,
            host_token,
            jwt_public_keys: self.issuer.verifier_keys_json()?,
            control_plane_url: self.runtime.control_plane_url.clone(),
            jwt_issuer: self.runtime.jwt_issuer.clone(),
            invocation_jwt_audience: self.runtime.invocation_jwt_audience.clone(),
            image_ref: spec.image_ref.clone(),
            working_directory: spec.working_directory.clone(),
            actor_entrypoint: spec.actor_entrypoint.clone(),
            actor_idle_timeout_ms: self.runtime.actor_idle_timeout_ms,
            host_idle_timeout_ms: self.runtime.host_idle_timeout_ms,
        })
    }

    async fn active_lease(
        &self,
        handle: crate::sandbox::ActorHostHandle,
        region: &str,
    ) -> Result<HostLease> {
        ensure!(
            handle.canonical_region == region,
            "sandbox provider returned the wrong region"
        );
        validate_host_route(&handle.route)?;
        let status = self.leases.lease_status(&handle.host_id).await?;
        ensure!(status.is_active(), "sandbox host lease is not active");
        let lease = status.lease.context("active sandbox lease is missing")?;
        ensure!(
            lease.route == handle.route,
            "sandbox route does not match its lease"
        );
        Ok(lease)
    }
}

struct RoutedActor {
    placement: ObjectPlacement,
    lease: HostLease,
    spec: HostLaunchSpec,
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn validate_state_owner(
    principal: &ActorPrincipal,
    host_id: &HostId,
    owner_epoch: u64,
    placement: &ObjectPlacement,
) -> Result<()> {
    ensure!(placement.owner == *host_id, "host does not own this actor");
    ensure!(
        placement.owner_epoch == owner_epoch,
        "actor owner epoch is stale"
    );
    ensure!(
        placement.home_region == principal.region,
        "host is outside the actor home region"
    );
    Ok(())
}

fn host_matches_revision(host: &HostId, namespace: &str, revision: &str) -> bool {
    host.as_str()
        .starts_with(&format!("host.v1.{namespace}.{revision}."))
}

fn validate_host_route(route: &str) -> Result<()> {
    let route = reqwest::Url::parse(route)?;
    ensure!(
        matches!(route.scheme(), "http" | "https") && route.host_str().is_some(),
        "host route must be an HTTP origin"
    );
    ensure!(
        route.username().is_empty() && route.password().is_none(),
        "host route must not contain credentials"
    );
    ensure!(
        route.path() == "/" && route.query().is_none() && route.fragment().is_none(),
        "host route must not contain a path, query, or fragment"
    );
    Ok(())
}

fn unix_seconds() -> Result<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?)
}

fn failed_precondition(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex, time::Duration};

    use super::super::{ActorTokenPurpose, admin::LocalAdminRegistry};
    use super::*;
    use crate::{
        actor::ActorScope,
        actor_state::ActorStorageKey,
        host_leases::{HostLeaseRegistry, HostLeaseRequest, HostLeaseStatus},
        placement::LocalObjectPlacementStore,
    };
    use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
    use base64::{Engine, engine::general_purpose::STANDARD};

    struct FakeLeaseStore {
        leases: Mutex<HashMap<HostId, HostLease>>,
    }

    #[async_trait]
    impl HostLeaseRegistry for FakeLeaseStore {
        async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
            let lease = HostLease {
                id: request.id.clone(),
                session_id: request.session_id.clone(),
                route: request.route.clone(),
                expires_at_ms: 10_000,
            };
            self.leases
                .lock()
                .unwrap()
                .insert(lease.id.clone(), lease.clone());
            Ok(lease)
        }

        async fn unregister(&self, id: &HostId, _session_id: &str) -> Result<()> {
            self.leases.lock().unwrap().remove(id);
            Ok(())
        }
    }

    #[async_trait]
    impl HostLeaseStore for FakeLeaseStore {
        async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus> {
            Ok(HostLeaseStatus {
                lease: self.leases.lock().unwrap().get(id).cloned(),
                store_now_ms: 0,
            })
        }
    }

    struct FakeStorageUrls;

    #[async_trait]
    impl StorageUrlSigner for FakeStorageUrls {
        async fn read_url(&self, _region: &str, _object_name: &str) -> Result<String> {
            Ok("https://storage.example.com/state".into())
        }

        async fn write_ticket(
            &self,
            _region: &str,
            _actor: &ActorKey,
            state_version: u64,
        ) -> Result<StateWriteTicket> {
            Ok(StateWriteTicket {
                state_version,
                object_name: format!(
                    "snapshots/00/00000000000000000000000000000000/project-1/Counter/counter-1/{state_version}.json"
                ),
                url: "https://storage.example.com/write".into(),
                expires_at_ms: i64::MAX,
            })
        }

        fn regions(&self) -> Vec<String> {
            vec!["us-east".into()]
        }
    }

    struct FakeWarmProvisioner {
        warmed: tokio::sync::mpsc::UnboundedSender<(HostLaunchSpec, String)>,
    }

    #[async_trait]
    impl HostProvisioner for FakeWarmProvisioner {
        async fn ensure_host(&self, _spec: &HostLaunchSpec, _region: &str) -> Result<HostLease> {
            anyhow::bail!("host creation is outside this test")
        }

        async fn warm_image(&self, spec: &HostLaunchSpec, region: &str) -> Result<ImageWarmup> {
            self.warmed.send((spec.clone(), region.to_owned()))?;
            Ok(ImageWarmup {
                provider: "test".into(),
                resource_id: "sandbox-1".into(),
                total_ms: 1,
            })
        }

        async fn terminate_hosts(
            &self,
            _spec: &HostLaunchSpec,
            _regions: &[String],
        ) -> Result<HostTermination> {
            anyhow::bail!("host termination is outside this test")
        }
    }

    struct FakeRetiringProvisioner {
        retired: tokio::sync::mpsc::UnboundedSender<(HostLaunchSpec, Vec<String>)>,
    }

    #[async_trait]
    impl HostProvisioner for FakeRetiringProvisioner {
        async fn ensure_host(&self, _spec: &HostLaunchSpec, _region: &str) -> Result<HostLease> {
            anyhow::bail!("host creation is outside this test")
        }

        async fn warm_image(&self, _spec: &HostLaunchSpec, _region: &str) -> Result<ImageWarmup> {
            anyhow::bail!("image warmup is outside this test")
        }

        async fn terminate_hosts(
            &self,
            spec: &HostLaunchSpec,
            regions: &[String],
        ) -> Result<HostTermination> {
            self.retired.send((spec.clone(), regions.to_vec()))?;
            Ok(HostTermination {
                provider: "test".into(),
                resource_ids: vec!["sandbox-1".into()],
            })
        }
    }

    #[tokio::test]
    async fn replacing_a_deployment_terminates_the_previous_revision_hosts() -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = super::super::admin::AdminService::new(
            "admin-token".into(),
            registry.clone(),
            issuer.clone(),
        )?;
        let (retired_tx, mut retired_rx) = tokio::sync::mpsc::unbounded_channel();
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            Arc::new(FakeStorageUrls),
            auth,
        )
        .with_routing(
            registry,
            issuer,
            Some(Arc::new(FakeRetiringProvisioner {
                retired: retired_tx,
            })),
        );
        let first = HostLaunchSpec {
            namespace_id: "project-1".into(),
            code_revision: "revision-1".into(),
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
        };
        let mut replacement = first.clone();
        replacement.code_revision = "revision-2".into();
        replacement.image_ref = "image-2".into();

        assert!(service.register_deployment(&admin, &first).await?);
        assert!(retired_rx.try_recv().is_err());
        assert!(service.register_deployment(&admin, &replacement).await?);

        assert_eq!(
            retired_rx.recv().await,
            Some((first, vec!["us-east".into()]))
        );
        Ok(())
    }

    #[tokio::test]
    async fn deployment_image_warmup_runs_in_the_background_without_creating_an_actor() -> Result<()>
    {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let leases = Arc::new(FakeLeaseStore {
            leases: Mutex::new(HashMap::new()),
        });
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let registry = Arc::new(LocalAdminRegistry::default());
        let (warmed_tx, mut warmed_rx) = tokio::sync::mpsc::unbounded_channel();
        let service = ControlPlaneService::new(leases, placements, Arc::new(FakeStorageUrls), auth)
            .with_routing(
                registry,
                issuer,
                Some(Arc::new(FakeWarmProvisioner { warmed: warmed_tx })),
            );
        let spec = HostLaunchSpec {
            namespace_id: "project-1".into(),
            code_revision: "revision-1".into(),
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
        };

        service.warm_deployment_image(spec.clone(), "us-east".into());

        let warmed = tokio::time::timeout(Duration::from_secs(1), warmed_rx.recv())
            .await?
            .context("warmup task stopped")?;
        assert_eq!(warmed, (spec, "us-east".into()));
        Ok(())
    }

    #[tokio::test]
    async fn workflow_target_resolves_a_direct_host_capability() -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let host_id = HostId::new("host.v1.project-1.revision-1.host-1");
        let leases = Arc::new(FakeLeaseStore {
            leases: Mutex::new(HashMap::from([(
                host_id.clone(),
                HostLease {
                    id: host_id.clone(),
                    session_id: "00000000-0000-4000-8000-000000000001".into(),
                    route: "https://actor.example.com/".into(),
                    expires_at_ms: 10_000,
                },
            )])),
        });
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let registry = Arc::new(LocalAdminRegistry::default());
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        };
        registry
            .ensure_namespace_and_register_deployment(&HostLaunchSpec {
                namespace_id: "project-1".into(),
                code_revision: "revision-1".into(),
                image_ref: "image-1".into(),
                working_directory: "/workspace".into(),
                actor_entrypoint: None,
            })
            .await?;
        placements
            .claim(&actor.storage_key(), None, &host_id, "us-east")
            .await?;
        let service = ControlPlaneService::new(leases, placements, Arc::new(FakeStorageUrls), auth)
            .with_routing(registry, issuer.clone(), None);

        let target = service
            .resolve_workflow_target(
                &ActorPrincipal {
                    scope: ActorScope {
                        namespace_id: "project-1".into(),
                    },
                    host_id: HostId::new("workflow.v1.project-1.execution-1"),
                    session_id: uuid::Uuid::new_v4().to_string(),
                    process_role: ActorProcessRole::Workflow,
                    region: "us-east".into(),
                    code_revision: None,
                    expires_at: unix_seconds()? + 30,
                    invocation: None,
                },
                &actor,
            )
            .await?;

        assert_eq!(target.route, "https://actor.example.com/");
        assert_eq!(target.owner_epoch, 1);
        assert_eq!(target.state_version, 0);
        assert!(target.state_read_url.is_empty());
        let verifier = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let principal = verifier.authenticate_authorization(&format!("Bearer {}", target.token))?;
        assert_eq!(
            principal
                .invocation
                .expect("direct invocation capability")
                .actor,
            actor
        );
        Ok(())
    }

    #[test]
    fn new_actors_use_the_workflow_region_and_existing_actors_stay_pinned() -> Result<()> {
        let regions = vec![
            "north-america-east".into(),
            "north-america-central".into(),
            "north-america-west".into(),
        ];
        let actor = ActorStorageKey::new("object.v1.project.Counter.one");
        let current = ObjectPlacement {
            object: actor,
            owner: HostId::new("host.v1.project.revision.host"),
            owner_epoch: 1,
            home_region: "north-america-east".into(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        };

        assert_eq!(
            select_target_region(None, "north-america-central", &regions)?,
            "north-america-central"
        );
        assert_eq!(
            select_target_region(Some(&current), "north-america-west", &regions)?,
            "north-america-east"
        );
        assert!(select_target_region(None, "europe-west", &regions).is_err());
        Ok(())
    }

    fn test_issuer() -> Result<ActorJwtIssuer> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "test-key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )
    }
}
