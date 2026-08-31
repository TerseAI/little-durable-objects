use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use subtle::ConstantTimeEq;
use tonic::{Request, Response, Status, metadata::MetadataValue, transport::Endpoint};

use crate::{
    actor::{ActorExecutionResult, ActorInvocation, ActorInvocationFailure, ActorKey},
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest, EnsureNamespaceReply, EnsureNamespaceRequest,
        GetJwksReply, GetJwksRequest, HostInvokeActorRequest, InvokeActorReply, InvokeActorRequest,
        IssueWorkflowTokenReply, IssueWorkflowTokenRequest, RegisterLaunchSpecReply,
        RegisterLaunchSpecRequest,
        actor_admin_service_server::{ActorAdminService, ActorAdminServiceServer},
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
        actor_host_service_client::ActorHostServiceClient,
    },
    host::{ActorProcessRole, HostId},
    host_leases::{HostLease, HostLeaseStore},
    placement::{ObjectPlacement, ObjectPlacementStore, PlacementClaim},
    sandbox::{EnsureHostRequest, HostSandboxRuntimeConfig, SandboxProvider},
    storage_urls::StorageUrlSigner,
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminRegistry, HostLaunchSpec},
    auth::{ActorJwtVerifier, ActorPrincipal},
    issuer::ActorJwtIssuer,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_command, encode_reply},
};

const HOST_TOKEN_RENEWAL_WINDOW_SECONDS: i64 = 10 * 60;
const MAX_ROUTE_ATTEMPTS: usize = 3;

#[derive(Clone)]
pub struct ControlPlaneService {
    leases: Arc<dyn HostLeaseStore>,
    placements: Arc<dyn ObjectPlacementStore>,
    storage_urls: Arc<dyn StorageUrlSigner>,
    auth: ActorJwtVerifier,
    sandbox_provider: Option<Arc<dyn SandboxProvider>>,
    sandbox_runtime: Option<HostSandboxRuntimeConfig>,
    admin: Option<AdminDependencies>,
}

#[derive(Clone)]
struct AdminDependencies {
    token: String,
    registry: Arc<dyn AdminRegistry>,
    issuer: ActorJwtIssuer,
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
            sandbox_provider: None,
            sandbox_runtime: None,
            admin: None,
        }
    }

    pub fn with_sandbox_provider(mut self, provider: Arc<dyn SandboxProvider>) -> Self {
        self.sandbox_provider = Some(provider);
        self
    }

    pub fn with_sandbox_runtime(mut self, runtime: HostSandboxRuntimeConfig) -> Self {
        self.sandbox_runtime = Some(runtime);
        self
    }

    pub fn with_admin(
        mut self,
        token: String,
        registry: Arc<dyn AdminRegistry>,
        issuer: ActorJwtIssuer,
    ) -> Result<Self> {
        ensure!(
            !token.is_empty() && token.trim() == token,
            "admin token is invalid"
        );
        self.admin = Some(AdminDependencies {
            token,
            registry,
            issuer,
        });
        Ok(self)
    }

    pub fn into_service(self) -> ActorControlPlaneServiceServer<Self> {
        ActorControlPlaneServiceServer::new(self)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
    }

    pub fn into_admin_service(self) -> ActorAdminServiceServer<Self> {
        ActorAdminServiceServer::new(self)
    }

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
                principal.validate_host_id(request.id.as_str())?;
                ensure!(
                    request.session_id == principal.session_id,
                    "lease session does not match host token"
                );
                validate_host_route(&request.route)?;
                let lease = self.leases.register(&request).await?;
                let replacement_token = if principal.expires_at.saturating_sub(unix_seconds()?)
                    <= HOST_TOKEN_RENEWAL_WINDOW_SECONDS
                {
                    let admin = self
                        .admin
                        .as_ref()
                        .context("JWT issuer is not configured")?;
                    Some(
                        admin
                            .issuer
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
                    )
                } else {
                    None
                };
                Ok(ControlPlaneCommandReply::Lease {
                    lease,
                    replacement_token,
                })
            }
            ControlPlaneCommand::UnregisterLease { host_id } => {
                principal.validate_host_id(host_id.as_str())?;
                self.leases
                    .unregister(&host_id, &principal.session_id)
                    .await?;
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::AuthorizeStateWrite {
                actor,
                host_id,
                owner_epoch,
                expected_generation,
            } => {
                actor.validate()?;
                ensure!(
                    principal.scope.contains(&actor),
                    "actor crossed the host namespace"
                );
                principal.validate_host_id(host_id.as_str())?;
                self.require_active_host(principal).await?;
                let placement = self
                    .placements
                    .get(&actor.storage_key())
                    .await?
                    .context("actor has no current placement")?;
                ensure!(placement.owner == host_id, "host does not own this actor");
                ensure!(
                    placement.owner_epoch == owner_epoch,
                    "actor owner epoch is stale"
                );
                ensure!(
                    placement.home_region == principal.region,
                    "host is outside the actor home region"
                );
                Ok(ControlPlaneCommandReply::StateWriteUrl {
                    url: self
                        .storage_urls
                        .write_url(&placement.home_region, &actor, &expected_generation)
                        .await?,
                })
            }
        }
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

    async fn invoke_workflow(
        &self,
        principal: ActorPrincipal,
        request: InvokeActorRequest,
    ) -> ActorExecutionResult {
        let invocation: ActorInvocation = match request.clone().try_into() {
            Ok(invocation) => invocation,
            Err(error) => {
                return failed(
                    "state_error",
                    format!("invalid actor invocation: {error:#}"),
                );
            }
        };
        if principal.process_role != ActorProcessRole::Workflow
            || !principal.scope.contains(&invocation.actor)
        {
            return failed("unauthenticated", "workflow token cannot invoke this actor");
        }

        for _ in 0..MAX_ROUTE_ATTEMPTS {
            let target = match self.route_actor(&invocation.actor).await {
                Ok(target) => target,
                Err(error) => {
                    return failed(
                        "unavailable",
                        format!("actor host is unavailable: {error:#}"),
                    );
                }
            };
            let reply = match self
                .invoke_host(request.clone(), &invocation.actor, &target)
                .await
            {
                Ok(reply) => reply,
                Err(HostCallError::Unavailable(message)) => return failed("unavailable", message),
                Err(HostCallError::OutcomeUnknown(message)) => {
                    return failed("outcome_unknown", message);
                }
            };
            match ActorExecutionResult::try_from(reply) {
                Ok(ActorExecutionResult::Reroute) => continue,
                Ok(result) => return result,
                Err(error) => {
                    return failed(
                        "outcome_unknown",
                        format!("actor host returned an invalid reply: {error:#}"),
                    );
                }
            }
        }
        failed(
            "unavailable",
            "actor ownership changed repeatedly before execution",
        )
    }

    async fn route_actor(&self, actor: &ActorKey) -> Result<RoutedActor> {
        let admin = self
            .admin
            .as_ref()
            .context("admin registry is not configured")?;
        let spec = admin
            .registry
            .launch_spec(&actor.namespace_id)
            .await?
            .context("project has no registered actor code")?;
        loop {
            let current = self.placements.get(&actor.storage_key()).await?;
            if let Some(placement) = &current {
                if host_matches_revision(&placement.owner, &actor.namespace_id, &spec.code_revision)
                {
                    let status = self.leases.lease_status(&placement.owner).await?;
                    if status.is_active() {
                        return Ok(RoutedActor {
                            placement: placement.clone(),
                            lease: status.lease.context("active lease is missing")?,
                            spec,
                        });
                    }
                }
            }

            let region = current
                .as_ref()
                .map(|placement| placement.home_region.clone())
                .or_else(|| self.storage_urls.regions().into_iter().next())
                .context("no sandbox region has a Standard bucket")?;
            let lease = self.ensure_host(&spec, &region).await?;
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

    async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease> {
        let provider = self
            .sandbox_provider
            .as_ref()
            .context("sandbox provider is not configured")?;
        let runtime = self
            .sandbox_runtime
            .as_ref()
            .context("sandbox runtime is not configured")?;
        let admin = self
            .admin
            .as_ref()
            .context("JWT issuer is not configured")?;
        let host_id = HostId::new(format!(
            "host.v1.{}.{}.{}",
            spec.namespace_id,
            spec.code_revision,
            uuid::Uuid::new_v4()
        ));
        let session_id = uuid::Uuid::new_v4().to_string();
        let host_token = admin
            .issuer
            .issue_host(
                &spec.namespace_id,
                &host_id,
                &session_id,
                &spec.code_revision,
                region,
            )?
            .token;
        let handle = provider
            .ensure_host(&EnsureHostRequest {
                namespace_id: spec.namespace_id.clone(),
                code_revision: spec.code_revision.clone(),
                canonical_region: region.to_owned(),
                host_id,
                session_id,
                host_token,
                jwt_public_keys: admin.issuer.verifier_keys_json()?,
                control_plane_url: runtime.control_plane_url.clone(),
                jwt_issuer: runtime.jwt_issuer.clone(),
                invocation_jwt_audience: runtime.invocation_jwt_audience.clone(),
                image_ref: spec.image_ref.clone(),
                working_directory: spec.working_directory.clone(),
                actor_entrypoint: spec.actor_entrypoint.clone(),
                actor_idle_timeout_ms: runtime.actor_idle_timeout_ms,
                host_idle_timeout_ms: runtime.host_idle_timeout_ms,
            })
            .await?;
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

    async fn invoke_host(
        &self,
        request: InvokeActorRequest,
        actor: &ActorKey,
        target: &RoutedActor,
    ) -> std::result::Result<InvokeActorReply, HostCallError> {
        let admin = self
            .admin
            .as_ref()
            .expect("routed actors require admin dependencies");
        let read_url = self
            .storage_urls
            .read_url(&target.placement.home_region, actor)
            .await
            .map_err(|error| {
                HostCallError::Unavailable(format!("could not authorize state read: {error:#}"))
            })?;
        let token = admin
            .issuer
            .issue_host(
                &actor.namespace_id,
                &target.lease.id,
                &target.lease.session_id,
                &target.spec.code_revision,
                &target.placement.home_region,
            )
            .map_err(|error| {
                HostCallError::Unavailable(format!("could not issue host credential: {error:#}"))
            })?
            .token;
        let channel = Endpoint::from_shared(target.lease.route.clone())
            .map_err(|error| HostCallError::Unavailable(format!("host route is invalid: {error}")))?
            .connect()
            .await
            .map_err(|error| {
                HostCallError::Unavailable(format!("could not connect to actor host: {error}"))
            })?;
        let mut rpc = Request::new(HostInvokeActorRequest {
            invocation: Some(request),
            owner_epoch: target.placement.owner_epoch,
            state_read_url: read_url,
        });
        rpc.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse::<MetadataValue<_>>()
                .map_err(|error| {
                    HostCallError::Unavailable(format!(
                        "host credential is invalid metadata: {error}"
                    ))
                })?,
        );
        ActorHostServiceClient::new(channel)
            .invoke(rpc)
            .await
            .map(|reply| reply.into_inner())
            .map_err(|error| {
                HostCallError::OutcomeUnknown(format!(
                    "actor host RPC failed after dispatch: {error}"
                ))
            })
    }

    fn authenticate_admin<T>(
        &self,
        request: &Request<T>,
    ) -> std::result::Result<&AdminDependencies, Status> {
        let admin = self
            .admin
            .as_ref()
            .ok_or_else(|| Status::unavailable("admin service is not configured"))?;
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("admin credential is required"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("admin credential is invalid"))?;
        let token = authorization.strip_prefix("Bearer ").ok_or_else(|| {
            Status::unauthenticated("admin credential must use Bearer authentication")
        })?;
        if !bool::from(token.as_bytes().ct_eq(admin.token.as_bytes())) {
            return Err(Status::unauthenticated("admin credential is invalid"));
        }
        Ok(admin)
    }
}

#[tonic::async_trait]
impl ActorControlPlaneService for ControlPlaneService {
    async fn invoke(
        &self,
        request: Request<InvokeActorRequest>,
    ) -> std::result::Result<Response<InvokeActorReply>, Status> {
        let principal = self.auth.authenticate(&request).await?;
        let timeout_ms = request.get_ref().timeout_ms;
        if timeout_ms == 0 {
            return Err(Status::invalid_argument("actor timeout must be positive"));
        }
        let service = self.clone();
        let invocation = request.into_inner();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = tx.send(service.invoke_workflow(principal, invocation).await);
        });
        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(result)) => Ok(Response::new(result.into())),
            Ok(Err(_)) => Ok(Response::new(
                failed(
                    "outcome_unknown",
                    "actor invocation task stopped without a reply",
                )
                .into(),
            )),
            Err(_) => Ok(Response::new(
                failed(
                    "deadline_exceeded",
                    "caller deadline elapsed; actor execution may still complete",
                )
                .into(),
            )),
        }
    }

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

#[tonic::async_trait]
impl ActorAdminService for ControlPlaneService {
    async fn ensure_namespace(
        &self,
        request: Request<EnsureNamespaceRequest>,
    ) -> std::result::Result<Response<EnsureNamespaceReply>, Status> {
        let admin = self.authenticate_admin(&request)?;
        let created = admin
            .registry
            .ensure_namespace(&request.get_ref().namespace_id)
            .await
            .map_err(failed_precondition)?;
        Ok(Response::new(EnsureNamespaceReply { created }))
    }

    async fn register_launch_spec(
        &self,
        request: Request<RegisterLaunchSpecRequest>,
    ) -> std::result::Result<Response<RegisterLaunchSpecReply>, Status> {
        let admin = self.authenticate_admin(&request)?;
        let request = request.into_inner();
        let created = admin
            .registry
            .register_launch_spec(&HostLaunchSpec {
                namespace_id: request.namespace_id,
                code_revision: request.code_revision,
                image_ref: request.image_ref,
                working_directory: request.working_directory,
                actor_entrypoint: request.actor_entrypoint,
            })
            .await
            .map_err(failed_precondition)?;
        Ok(Response::new(RegisterLaunchSpecReply { created }))
    }

    async fn issue_workflow_token(
        &self,
        request: Request<IssueWorkflowTokenRequest>,
    ) -> std::result::Result<Response<IssueWorkflowTokenReply>, Status> {
        let admin = self.authenticate_admin(&request)?;
        let request = request.into_inner();
        ensure_workflow_request(&request).map_err(failed_precondition)?;
        if admin
            .registry
            .launch_spec(&request.namespace_id)
            .await
            .map_err(failed_precondition)?
            .is_none()
        {
            return Err(Status::failed_precondition(
                "project has no registered actor code",
            ));
        }
        let issued = admin
            .issuer
            .issue_workflow(
                &request.namespace_id,
                &request.execution_id,
                request.deadline_unix_ms,
            )
            .map_err(failed_precondition)?;
        Ok(Response::new(IssueWorkflowTokenReply {
            token: issued.token,
            expires_at_ms: issued.expires_at_ms,
        }))
    }

    async fn get_jwks(
        &self,
        _request: Request<GetJwksRequest>,
    ) -> std::result::Result<Response<GetJwksReply>, Status> {
        let admin = self
            .admin
            .as_ref()
            .ok_or_else(|| Status::unavailable("admin service is not configured"))?;
        Ok(Response::new(GetJwksReply {
            jwks_json: admin.issuer.jwks_json().map_err(internal)?,
        }))
    }
}

struct RoutedActor {
    placement: ObjectPlacement,
    lease: HostLease,
    spec: HostLaunchSpec,
}

enum HostCallError {
    Unavailable(String),
    OutcomeUnknown(String),
}

fn host_matches_revision(host: &HostId, namespace: &str, revision: &str) -> bool {
    host.as_str()
        .starts_with(&format!("host.v1.{namespace}.{revision}."))
}

fn failed(code: impl Into<String>, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}

fn ensure_workflow_request(request: &IssueWorkflowTokenRequest) -> Result<()> {
    ensure!(
        !request.execution_id.is_empty() && request.execution_id.len() <= 255,
        "workflow execution ID is invalid"
    );
    ensure!(
        request.deadline_unix_ms > 0,
        "workflow deadline is required"
    );
    Ok(())
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
