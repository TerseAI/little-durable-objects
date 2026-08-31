use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use tonic::{Request, Response, Status};

use crate::{
    actor_state::ActorStorageKey,
    durability::{ActorDurabilityStore, VersionedActorManifest},
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest, EnsureNamespaceReply, EnsureNamespaceRequest,
        GetJwksReply, GetJwksRequest, IssueWorkflowTokenReply, IssueWorkflowTokenRequest,
        RegisterLaunchSpecReply, RegisterLaunchSpecRequest, ResolveActorHostRequest,
        ResolvedActorHost,
        actor_admin_service_server::{ActorAdminService, ActorAdminServiceServer},
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
    },
    host::{ActorProcessRole, HostId},
    host_leases::HostLeaseStore,
    sandbox::{EnsureHostRequest, HostSandboxRuntimeConfig, SandboxProvider},
    telemetry::{ActorTelemetry, noop_actor_telemetry},
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminRegistry, HostLaunchSpec},
    auth::{ActorJwtVerifier, ActorPrincipal},
    issuer::ActorJwtIssuer,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_command, encode_reply},
};

const HOST_TOKEN_RENEWAL_WINDOW_SECONDS: i64 = 10 * 60;

/// Stateless ownership and durability gateway. Any replica can serve any request;
/// host reachability lives in the lease store and actor bytes live in the configured
/// durability stores, never in process-local connections.
#[derive(Clone)]
pub struct ControlPlaneService {
    durability: Arc<dyn ActorDurabilityStore>,
    leases: Arc<dyn HostLeaseStore>,
    auth: ActorJwtVerifier,
    telemetry: Arc<dyn ActorTelemetry>,
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
        durability: Arc<dyn ActorDurabilityStore>,
        leases: Arc<dyn HostLeaseStore>,
        auth: ActorJwtVerifier,
    ) -> Self {
        Self {
            durability,
            leases,
            auth,
            telemetry: noop_actor_telemetry(),
            sandbox_provider: None,
            sandbox_runtime: None,
            admin: None,
        }
    }

    pub fn with_telemetry(mut self, telemetry: Arc<dyn ActorTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_sandbox_provider(mut self, sandbox_provider: Arc<dyn SandboxProvider>) -> Self {
        self.sandbox_provider = Some(sandbox_provider);
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
            "DURABLE_OBJECT_ADMIN_TOKEN must be non-empty without surrounding whitespace"
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
            "only actor host leases may execute control-plane commands"
        );
        match command {
            ControlPlaneCommand::RegisterLease { request } => {
                principal.validate_host_id(request.id.as_str())?;
                ensure!(
                    request.session_id == principal.session_id,
                    "lease session does not match the authenticated host session"
                );
                validate_host_route(&request.route)?;
                let lease = self.leases.register(&request).await?;
                let replacement_token = if principal.expires_at.saturating_sub(unix_seconds()?)
                    <= HOST_TOKEN_RENEWAL_WINDOW_SECONDS
                {
                    let admin = self
                        .admin
                        .as_ref()
                        .context("system JWT issuer is not configured")?;
                    let code_revision = principal
                        .code_revision
                        .as_deref()
                        .context("host token is missing its code revision")?;
                    Some(
                        admin
                            .issuer
                            .issue_host(
                                &principal.scope.namespace_id,
                                &principal.host_id,
                                &principal.session_id,
                                code_revision,
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
            ControlPlaneCommand::GetLeaseStatus { host_id } => {
                principal.validate_namespace_host_id(host_id.as_str())?;
                Ok(ControlPlaneCommandReply::LeaseStatus {
                    status: self.leases.lease_status(&host_id).await?,
                })
            }
            ControlPlaneCommand::UnregisterLease { host_id } => {
                principal.validate_host_id(host_id.as_str())?;
                self.leases
                    .unregister(&host_id, &principal.session_id)
                    .await?;
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::GetManifest {
                storage_key: object,
            } => {
                principal.validate_actor_storage_key(object.as_str())?;
                Ok(ControlPlaneCommandReply::Manifest {
                    manifest: self.durability.manifest(&object).await?,
                })
            }
            ControlPlaneCommand::Claim {
                storage_key: object,
                expected,
                host_id,
            } => self.claim_actor(principal, object, expected, host_id).await,
            ControlPlaneCommand::Publish {
                storage_key: object,
                current,
                captured,
            } => {
                principal.validate_actor_storage_key(object.as_str())?;
                principal.validate_host_id(current.owner().host.as_str())?;
                self.require_active_host_lease(principal).await?;
                Ok(ControlPlaneCommandReply::Published {
                    manifest: self
                        .durability
                        .publish(&object, &current, &captured)
                        .await?,
                })
            }
            ControlPlaneCommand::Recovery {
                storage_key: object,
                manifest,
            } => {
                principal.validate_actor_storage_key(object.as_str())?;
                principal.validate_namespace_host_id(manifest.owner.host.as_str())?;
                Ok(ControlPlaneCommandReply::Recovery {
                    recovery: self.durability.recovery(&object, &manifest).await?,
                })
            }
            ControlPlaneCommand::TelemetryBatch { mut events } => {
                ensure!(
                    events.len() <= crate::telemetry::control_plane::MAX_FORWARDED_BATCH_EVENTS,
                    "actor telemetry batch contains too many events"
                );
                for event in &mut events {
                    ensure!(
                        event.is_host_event(),
                        "sandbox hosts may only forward host telemetry"
                    );
                    event.set_scope(&principal.scope);
                }
                for event in events {
                    self.telemetry.publish(event);
                }
                Ok(ControlPlaneCommandReply::Unit)
            }
        }
    }

    async fn claim_actor(
        &self,
        principal: &ActorPrincipal,
        storage_key: ActorStorageKey,
        expected: Option<VersionedActorManifest>,
        host_id: HostId,
    ) -> Result<ControlPlaneCommandReply> {
        principal.validate_actor_storage_key(storage_key.as_str())?;
        principal.validate_host_id(host_id.as_str())?;
        self.require_active_host_lease(principal).await?;
        if let Some(expected) = &expected {
            principal.validate_namespace_host_id(expected.owner().host.as_str())?;
            if expected.owner().host != principal.host_id {
                let previous = self.leases.lease_status(&expected.owner().host).await?;
                ensure!(
                    !previous.is_active(),
                    "cannot claim an actor from an active owner"
                );
            }
        }
        Ok(ControlPlaneCommandReply::Claim {
            result: self
                .durability
                .claim_in_home_region(&storage_key, expected.as_ref(), &host_id, &principal.region)
                .await?,
        })
    }

    async fn require_active_host_lease(&self, principal: &ActorPrincipal) -> Result<()> {
        let status = self.leases.lease_status(&principal.host_id).await?;
        ensure!(status.is_active(), "authenticated host lease is not active");
        ensure!(
            status
                .lease
                .as_ref()
                .is_some_and(|lease| lease.session_id == principal.session_id),
            "active lease belongs to a different host session"
        );
        Ok(())
    }

    async fn resolve_actor_host(
        &self,
        principal: &ActorPrincipal,
        request: ResolveActorHostRequest,
    ) -> Result<ResolvedActorHost> {
        ensure!(
            principal.process_role == ActorProcessRole::Workflow,
            "only workflow callers may resolve actor hosts"
        );
        let actor: crate::actor::ActorKey = request
            .actor
            .ok_or_else(|| anyhow::anyhow!("actor key is required"))?
            .into();
        actor.validate()?;
        ensure!(
            principal.scope.contains(&actor),
            "actor host resolution crossed namespace scope"
        );
        let object = actor.storage_key();
        let manifest = self.durability.manifest(&object).await?;
        if let Some(manifest) = &manifest {
            let owner = manifest.owner();
            let status = self.leases.lease_status(&owner.host).await?;
            if status.is_active() {
                let lease = status
                    .lease
                    .context("active actor owner lease is missing")?;
                return Ok(ResolvedActorHost { route: lease.route });
            }
        }

        let target_region = manifest
            .as_ref()
            .map(|manifest| manifest.manifest.home_region.as_str())
            .unwrap_or(&principal.region);
        let provider = self
            .sandbox_provider
            .as_ref()
            .context("a sandbox provider is not configured")?;
        let code_revision = principal
            .code_revision
            .as_ref()
            .context("workflow credential is missing its code revision")?;
        let admin = self
            .admin
            .as_ref()
            .context("admin registry is not configured")?;
        let spec = admin
            .registry
            .launch_spec(&principal.scope.namespace_id, code_revision)
            .await?
            .context("no launch spec is registered for this namespace and revision")?;
        let runtime = self
            .sandbox_runtime
            .as_ref()
            .context("sandbox runtime configuration is missing")?;
        let host_id = HostId::new(format!(
            "host.v1.{}.{}",
            principal.scope.namespace_id,
            uuid::Uuid::new_v4()
        ));
        let session_id = uuid::Uuid::new_v4().to_string();
        let host_token = admin
            .issuer
            .issue_host(
                &principal.scope.namespace_id,
                &host_id,
                &session_id,
                code_revision,
                target_region,
            )?
            .token;
        let provisioned = provider
            .ensure_host(&EnsureHostRequest {
                namespace_id: principal.scope.namespace_id.clone(),
                code_revision: code_revision.clone(),
                canonical_region: target_region.to_owned(),
                host_id: host_id.clone(),
                session_id,
                host_token,
                jwt_public_keys: admin.issuer.verifier_keys_json()?,
                control_plane_url: runtime.control_plane_url.clone(),
                jwt_issuer: runtime.jwt_issuer.clone(),
                invocation_jwt_audience: runtime.invocation_jwt_audience.clone(),
                modal_image_id: spec.modal_image_id,
                working_directory: spec.working_directory,
                actor_entrypoint: spec.actor_entrypoint,
                actor_idle_timeout_ms: runtime.actor_idle_timeout_ms,
                host_idle_timeout_ms: runtime.host_idle_timeout_ms,
            })
            .await?;
        principal.validate_namespace_host_id(provisioned.host_id.as_str())?;
        validate_host_route(&provisioned.route)?;
        ensure!(
            provisioned.canonical_region == target_region,
            "provisioned actor host is in the wrong storage region"
        );
        let status = self.leases.lease_status(&provisioned.host_id).await?;
        ensure!(
            status.is_active(),
            "provisioned actor host lease is not active"
        );
        let lease = status
            .lease
            .context("provisioned actor host lease is missing")?;
        ensure!(
            lease.id == provisioned.host_id && lease.route == provisioned.route,
            "provisioned actor host does not match its authoritative lease"
        );
        Ok(ResolvedActorHost { route: lease.route })
    }
}

#[tonic::async_trait]
impl ActorAdminService for ControlPlaneService {
    async fn ensure_namespace(
        &self,
        request: Request<EnsureNamespaceRequest>,
    ) -> Result<Response<EnsureNamespaceReply>, Status> {
        let admin = self.authenticate_admin(&request)?;
        let created = admin
            .registry
            .ensure_namespace(&request.get_ref().namespace_id)
            .await
            .map_err(admin_error)?;
        Ok(Response::new(EnsureNamespaceReply { created }))
    }

    async fn register_launch_spec(
        &self,
        request: Request<RegisterLaunchSpecRequest>,
    ) -> Result<Response<RegisterLaunchSpecReply>, Status> {
        let admin = self.authenticate_admin(&request)?;
        let request = request.into_inner();
        let created = admin
            .registry
            .register_launch_spec(&HostLaunchSpec {
                namespace_id: request.namespace_id,
                code_revision: request.code_revision,
                modal_image_id: request.modal_image_id,
                working_directory: request.working_directory,
                actor_entrypoint: request.actor_entrypoint,
            })
            .await
            .map_err(admin_error)?;
        Ok(Response::new(RegisterLaunchSpecReply { created }))
    }

    async fn issue_workflow_token(
        &self,
        request: Request<IssueWorkflowTokenRequest>,
    ) -> Result<Response<IssueWorkflowTokenReply>, Status> {
        let admin = self.authenticate_admin(&request)?;
        let request = request.into_inner();
        if admin
            .registry
            .launch_spec(&request.namespace_id, &request.code_revision)
            .await
            .map_err(admin_error)?
            .is_none()
        {
            return Err(Status::failed_precondition(
                "no launch spec is registered for this namespace and revision",
            ));
        }
        validate_workflow_token_request(&request).map_err(admin_error)?;
        let issued = admin
            .issuer
            .issue_workflow(
                &request.namespace_id,
                &request.execution_id,
                &request.code_revision,
                &request.region,
                request.deadline_unix_ms,
            )
            .map_err(admin_error)?;
        Ok(Response::new(IssueWorkflowTokenReply {
            token: issued.token,
            expires_at_ms: issued.expires_at_ms,
        }))
    }

    async fn get_jwks(
        &self,
        _request: Request<GetJwksRequest>,
    ) -> Result<Response<GetJwksReply>, Status> {
        let admin = self
            .admin
            .as_ref()
            .ok_or_else(|| Status::unavailable("admin service is not configured"))?;
        Ok(Response::new(GetJwksReply {
            jwks_json: admin.issuer.jwks_json().map_err(internal)?,
        }))
    }
}

impl ControlPlaneService {
    fn authenticate_admin<T>(&self, request: &Request<T>) -> Result<&AdminDependencies, Status> {
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
        if !bool::from(subtle::ConstantTimeEq::ct_eq(
            token.as_bytes(),
            admin.token.as_bytes(),
        )) {
            return Err(Status::unauthenticated("admin credential is invalid"));
        }
        Ok(admin)
    }
}

fn validate_workflow_token_request(request: &IssueWorkflowTokenRequest) -> Result<()> {
    ensure!(
        !request.execution_id.is_empty() && request.execution_id.len() <= 255,
        "workflow execution ID must contain between 1 and 255 bytes"
    );
    ensure!(
        !request.region.is_empty()
            && request.region.len() <= 64
            && request.region.bytes().all(|byte| byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'-')),
        "workflow region is invalid"
    );
    Ok(())
}

fn admin_error(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn unix_seconds() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_secs()).context("system clock exceeds supported JWT range")
}

#[tonic::async_trait]
impl ActorControlPlaneService for ControlPlaneService {
    async fn execute(
        &self,
        request: Request<ControlPlaneRequest>,
    ) -> Result<Response<ControlPlaneReply>, Status> {
        let principal = self.auth.authenticate(&request).await?;
        let command = decode_command(request.into_inner()).map_err(|error| {
            Status::invalid_argument(format!("invalid control-plane command: {error:#}"))
        })?;
        let reply = self
            .execute_command(&principal, command)
            .await
            .map_err(control_plane_error)?;
        Ok(Response::new(encode_reply(reply).map_err(internal)?))
    }

    async fn resolve_actor_host(
        &self,
        request: Request<ResolveActorHostRequest>,
    ) -> Result<Response<ResolvedActorHost>, Status> {
        let principal = self.auth.authenticate(&request).await?;
        self.resolve_actor_host(&principal, request.into_inner())
            .await
            .map(Response::new)
            .map_err(control_plane_error)
    }
}

fn validate_host_route(route: &str) -> Result<()> {
    let route = reqwest::Url::parse(route)?;
    ensure!(
        matches!(route.scheme(), "http" | "https"),
        "actor host route must use HTTP or HTTPS"
    );
    ensure!(
        route.host_str().is_some(),
        "actor host route must have a host"
    );
    ensure!(
        route.username().is_empty() && route.password().is_none(),
        "actor host route must not contain credentials"
    );
    ensure!(
        route.path() == "/" && route.query().is_none() && route.fragment().is_none(),
        "actor host route must not contain a path, query, or fragment"
    );
    Ok(())
}

fn control_plane_error(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex, time::Duration};

    use async_trait::async_trait;
    use aws_lc_rs::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };
    use base64::{
        Engine,
        engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    };
    use tempfile::TempDir;

    use crate::control_plane::{ActorTokenPurpose, LocalAdminRegistry};
    use crate::{
        actor::ActorScope,
        actor_state::ActorStorageKey,
        durability::{LocalActorStore, OwnershipClaimResult},
        host::HostId,
        host_leases::{HostLeaseRequest, LocalHostLeaseStore, MAX_HOST_LEASE_DURATION_MS},
        sandbox::{ActorHostHandle, CacheSource},
        telemetry::{
            ActorProcessHealthTelemetry, ActorSystemRole, ActorTelemetryEvent, ActorTelemetryScope,
            LocalActorTelemetry,
        },
    };

    use super::*;

    struct RecordingProvisioner {
        route: String,
        leases: Arc<LocalHostLeaseStore>,
        regions: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SandboxProvider for RecordingProvisioner {
        async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
            self.regions
                .lock()
                .map_err(|_| anyhow::anyhow!("activation recorder poisoned"))?
                .push(request.canonical_region.clone());
            self.leases
                .register(&HostLeaseRequest {
                    id: request.host_id.clone(),
                    session_id: request.session_id.clone(),
                    route: self.route.clone(),
                    duration_ms: 60_000,
                })
                .await?;
            Ok(ActorHostHandle {
                host_id: request.host_id.clone(),
                route: self.route.clone(),
                canonical_region: request.canonical_region.clone(),
                cache_source: CacheSource::DurableStorage,
            })
        }
    }

    #[test]
    fn accepts_only_origin_host_routes() {
        assert!(validate_host_route("https://node.example.com").is_ok());
        assert!(validate_host_route("http://127.0.0.1:7101").is_ok());
        assert!(validate_host_route("file:///tmp/socket").is_err());
        assert!(validate_host_route("https://node.example.com/admin").is_err());
        assert!(validate_host_route("https://user:password@node.example.com").is_err());
    }

    fn test_verifier() -> Result<ActorJwtVerifier> {
        let key_pair = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?.as_ref(),
        )?;
        let keys = serde_json::to_string(&HashMap::from([(
            "test-key",
            URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref()),
        )]))?;
        ActorJwtVerifier::new(
            keys,
            "durable-object-control-plane",
            "durable-object-authority",
            Duration::from_secs(60),
        )
    }

    fn test_issuer() -> Result<ActorJwtIssuer> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "test-key",
            "durable-object-control-plane",
            "durable-object-authority",
            "durable-object-invoke",
            Duration::from_secs(1_800),
        )
    }

    async fn with_test_provisioning(
        base: ControlPlaneService,
        provider: Arc<dyn SandboxProvider>,
    ) -> Result<ControlPlaneService> {
        let registry = Arc::new(LocalAdminRegistry::default());
        registry.ensure_namespace("namespace-1").await?;
        registry
            .register_launch_spec(&HostLaunchSpec {
                namespace_id: "namespace-1".into(),
                code_revision: "revision-1".into(),
                modal_image_id: "im-test".into(),
                working_directory: "/workspace".into(),
                actor_entrypoint: Some("src/durable-objects.ts".into()),
            })
            .await?;
        base.with_sandbox_provider(provider)
            .with_sandbox_runtime(HostSandboxRuntimeConfig {
                control_plane_url: "https://control-plane.example.com/".into(),
                jwt_issuer: "durable-object-control-plane".into(),
                invocation_jwt_audience: "durable-object-invoke".into(),
                actor_idle_timeout_ms: 60_000,
                host_idle_timeout_ms: 300_000,
            })
            .with_admin("admin-token".into(), registry, test_issuer()?)
    }

    fn principal(node: &str, session: &str) -> ActorPrincipal {
        ActorPrincipal {
            scope: ActorScope {
                namespace_id: "namespace-1".into(),
            },
            host_id: HostId::new(node),
            session_id: session.into(),
            process_role: ActorProcessRole::Host,
            region: "default".into(),
            code_revision: None,
            expires_at: i64::MAX,
        }
    }

    fn caller(region: &str) -> ActorPrincipal {
        ActorPrincipal {
            process_role: ActorProcessRole::Workflow,
            code_revision: Some("revision-1".into()),
            region: region.into(),
            ..principal(
                "host.v1.namespace-1.00000000-0000-4000-8000-000000000099",
                "00000000-0000-4000-8000-000000000099",
            )
        }
    }

    fn resolve_request() -> ResolveActorHostRequest {
        ResolveActorHostRequest {
            actor: Some(crate::grpc::proto::ActorKey {
                namespace_id: "namespace-1".into(),
                actor_type: "Counter".into(),
                actor_id: "counter-1".into(),
            }),
        }
    }

    async fn register(
        service: &ControlPlaneService,
        principal: &ActorPrincipal,
        duration_ms: u64,
    ) -> Result<()> {
        service
            .execute_command(
                principal,
                ControlPlaneCommand::RegisterLease {
                    request: HostLeaseRequest {
                        id: principal.host_id.clone(),
                        session_id: principal.session_id.clone(),
                        route: "https://actor-host.example.com".into(),
                        duration_ms,
                    },
                },
            )
            .await?;
        Ok(())
    }

    fn admin_request<T>(value: T) -> Result<Request<T>> {
        let mut request = Request::new(value);
        request.metadata_mut().insert(
            "authorization",
            "Bearer admin-token".parse().context("admin metadata")?,
        );
        Ok(request)
    }

    #[tokio::test]
    async fn admin_credential_registers_a_project_and_issues_its_single_workflow_jwt() -> Result<()>
    {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let registry = Arc::new(LocalAdminRegistry::default());
        let issuer = test_issuer()?;
        let service = ControlPlaneService::new(store, leases, test_verifier()?).with_admin(
            "admin-token".into(),
            registry,
            issuer.clone(),
        )?;

        let unauthenticated = service
            .ensure_namespace(Request::new(EnsureNamespaceRequest {
                namespace_id: "project-1".into(),
            }))
            .await
            .expect_err("admin RPCs must require the backend credential");
        assert_eq!(unauthenticated.code(), tonic::Code::Unauthenticated);

        let ensured = service
            .ensure_namespace(admin_request(EnsureNamespaceRequest {
                namespace_id: "project-1".into(),
            })?)
            .await?
            .into_inner();
        assert!(ensured.created);
        let registered = service
            .register_launch_spec(admin_request(RegisterLaunchSpecRequest {
                namespace_id: "project-1".into(),
                code_revision: "revision-1".into(),
                modal_image_id: "im-actor".into(),
                working_directory: "/workspace".into(),
                actor_entrypoint: Some("src/durable-objects.ts".into()),
            })?)
            .await?
            .into_inner();
        assert!(registered.created);
        let issued = service
            .issue_workflow_token(admin_request(IssueWorkflowTokenRequest {
                namespace_id: "project-1".into(),
                execution_id: "execution-1".into(),
                code_revision: "revision-1".into(),
                region: "north-america-east".into(),
                deadline_unix_ms: unix_seconds()?.saturating_mul(1_000) + 60_000,
            })?)
            .await?
            .into_inner();

        let keys = issuer.verifier_keys_json()?;
        for (audience, purpose) in [
            ("durable-object-authority", ActorTokenPurpose::ControlPlane),
            ("durable-object-invoke", ActorTokenPurpose::Invocation),
        ] {
            let verifier = ActorJwtVerifier::for_scope(
                &keys,
                "durable-object-control-plane",
                audience,
                purpose,
                Duration::from_secs(1_800),
            )?;
            let mut request = Request::new(());
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", issued.token)
                    .parse()
                    .context("workflow metadata")?,
            );
            let principal = verifier
                .authenticate(&request)
                .await
                .map_err(|status| anyhow::anyhow!(status.to_string()))?;
            assert_eq!(principal.scope.namespace_id, "project-1");
            assert_eq!(principal.process_role, ActorProcessRole::Workflow);
        }
        Ok(())
    }

    #[tokio::test]
    async fn forwarded_telemetry_uses_the_authenticated_namespace_scope() -> Result<()> {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let telemetry = Arc::new(LocalActorTelemetry::default());
        let service = ControlPlaneService::new(store, leases, test_verifier()?)
            .with_telemetry(telemetry.clone());
        let principal = principal(
            "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
            "00000000-0000-4000-8000-000000000011",
        );
        let forged = ActorTelemetryEvent::ActorProcessHealth(ActorProcessHealthTelemetry {
            scope: ActorTelemetryScope {
                namespace_id: Some("forged-namespace".into()),
            },
            role: ActorSystemRole::Host,
            uptime_ms: 1,
            ready: true,
            consecutive_failures: 0,
            telemetry_dropped_events: 0,
            last_success_age_ms: None,
        });

        let reply = service
            .execute_command(
                &principal,
                ControlPlaneCommand::TelemetryBatch {
                    events: vec![forged],
                },
            )
            .await?;

        assert!(matches!(reply, ControlPlaneCommandReply::Unit));
        let events = telemetry.events()?;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].scope(),
            &ActorTelemetryScope {
                namespace_id: Some("namespace-1".into()),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn control_plane_fences_identity_session_and_live_owner() -> Result<()> {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let service = ControlPlaneService::new(store, leases, test_verifier()?);
        let node_a = principal(
            "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
            "00000000-0000-4000-8000-000000000011",
        );
        let node_b = principal(
            "host.v1.namespace-1.00000000-0000-4000-8000-000000000002",
            "00000000-0000-4000-8000-000000000012",
        );
        register(&service, &node_a, 60_000).await?;
        register(&service, &node_b, 60_000).await?;
        let object = ActorStorageKey::new("object.v1.namespace-1.Counter.counter-1");
        let ControlPlaneCommandReply::Claim {
            result: OwnershipClaimResult::Acquired(current),
        } = service
            .execute_command(
                &node_a,
                ControlPlaneCommand::Claim {
                    storage_key: object.clone(),
                    expected: None,
                    host_id: node_a.host_id.clone(),
                },
            )
            .await?
        else {
            anyhow::bail!("initial claim did not acquire the actor")
        };

        assert!(
            service
                .execute_command(
                    &node_b,
                    ControlPlaneCommand::Claim {
                        storage_key: object.clone(),
                        expected: Some(current.clone()),
                        host_id: node_b.host_id.clone(),
                    },
                )
                .await
                .is_err(),
            "a live owner must not be displaced"
        );
        assert!(
            service
                .execute_command(
                    &node_b,
                    ControlPlaneCommand::Publish {
                        storage_key: object,
                        current,
                        captured: crate::durability::CapturedActorChanges::new(Vec::new()),
                    },
                )
                .await
                .is_err(),
            "a different authenticated host must not publish for the owner"
        );

        let mismatched_session = ActorPrincipal {
            session_id: "00000000-0000-4000-8000-000000000099".into(),
            ..node_a.clone()
        };
        assert!(
            service
                .execute_command(
                    &mismatched_session,
                    ControlPlaneCommand::RegisterLease {
                        request: HostLeaseRequest {
                            id: node_a.host_id,
                            session_id: node_a.session_id,
                            route: "https://actor-host.example.com".into(),
                            duration_ms: 60_000,
                        },
                    },
                )
                .await
                .is_err(),
            "the credential session must match lease registration"
        );
        Ok(())
    }

    #[tokio::test]
    async fn control_plane_rejects_out_of_range_lease_durations() -> Result<()> {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let service = ControlPlaneService::new(store, leases, test_verifier()?);
        let node = principal(
            "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
            "00000000-0000-4000-8000-000000000011",
        );

        assert!(register(&service, &node, 0).await.is_err());
        assert!(
            register(&service, &node, MAX_HOST_LEASE_DURATION_MS + 1)
                .await
                .is_err()
        );
        register(&service, &node, MAX_HOST_LEASE_DURATION_MS).await?;
        Ok(())
    }

    #[tokio::test]
    async fn workflow_principal_cannot_claim_actor_ownership() -> Result<()> {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let service = ControlPlaneService::new(store, leases, test_verifier()?);
        let caller = ActorPrincipal {
            process_role: ActorProcessRole::Workflow,
            code_revision: Some("revision-1".into()),
            region: "us-east".into(),
            ..principal(
                "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "00000000-0000-4000-8000-000000000011",
            )
        };
        let error = service
            .execute_command(
                &caller,
                ControlPlaneCommand::Claim {
                    storage_key: ActorStorageKey::new("object.v1.namespace-1.Counter.counter-1"),
                    expected: None,
                    host_id: caller.host_id.clone(),
                },
            )
            .await
            .expect_err("a workflow caller must never become the actor owner");
        assert!(
            error
                .to_string()
                .contains("only actor host leases may execute")
        );
        Ok(())
    }

    #[tokio::test]
    async fn workflow_resolution_activates_a_host_in_the_workflows_region() -> Result<()> {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let base = ControlPlaneService::new(store, leases.clone(), test_verifier()?);
        let provisioner = Arc::new(RecordingProvisioner {
            route: "https://actor-host.example.com".into(),
            leases,
            regions: Mutex::new(Vec::new()),
        });
        let service = with_test_provisioning(base, provisioner.clone()).await?;

        let resolved = service
            .resolve_actor_host(&caller("us-east"), resolve_request())
            .await?;

        assert_eq!(resolved.route, "https://actor-host.example.com");
        assert_eq!(*provisioner.regions.lock().unwrap(), vec!["us-east"]);
        Ok(())
    }

    #[tokio::test]
    async fn workflow_resolution_preserves_an_existing_actors_home_region() -> Result<()> {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let old_host = ActorPrincipal {
            region: "eu-west".into(),
            ..principal(
                "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "00000000-0000-4000-8000-000000000011",
            )
        };
        let base = ControlPlaneService::new(store, leases.clone(), test_verifier()?);
        register(&base, &old_host, 50).await?;
        let ControlPlaneCommandReply::Claim {
            result: OwnershipClaimResult::Acquired(_),
        } = base
            .execute_command(
                &old_host,
                ControlPlaneCommand::Claim {
                    storage_key: ActorStorageKey::new("object.v1.namespace-1.Counter.counter-1"),
                    expected: None,
                    host_id: old_host.host_id.clone(),
                },
            )
            .await?
        else {
            anyhow::bail!("initial claim did not acquire the actor")
        };
        tokio::time::sleep(Duration::from_millis(60)).await;
        let provisioner = Arc::new(RecordingProvisioner {
            route: "https://actor-host.example.com".into(),
            leases,
            regions: Mutex::new(Vec::new()),
        });
        let service = with_test_provisioning(base, provisioner.clone()).await?;

        let resolved = service
            .resolve_actor_host(&caller("us-east"), resolve_request())
            .await?;

        assert_eq!(resolved.route, "https://actor-host.example.com");
        assert_eq!(*provisioner.regions.lock().unwrap(), vec!["eu-west"]);
        Ok(())
    }

    #[tokio::test]
    async fn stale_unregister_cannot_remove_a_successor_session() -> Result<()> {
        let root = TempDir::new()?;
        let store = Arc::new(LocalActorStore::new(root.path().join("objects")));
        let leases = Arc::new(LocalHostLeaseStore::new(root.path().join("leases")).await?);
        let service = ControlPlaneService::new(store, leases.clone(), test_verifier()?);
        let stale = principal(
            "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
            "00000000-0000-4000-8000-000000000011",
        );
        let successor = ActorPrincipal {
            session_id: "00000000-0000-4000-8000-000000000012".into(),
            ..stale.clone()
        };

        register(&service, &stale, 1).await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
        register(&service, &successor, 30_000).await?;

        service
            .execute_command(
                &stale,
                ControlPlaneCommand::UnregisterLease {
                    host_id: stale.host_id.clone(),
                },
            )
            .await?;
        assert_eq!(
            leases
                .get(&successor.host_id)
                .await?
                .expect("successor lease")
                .session_id,
            successor.session_id
        );
        Ok(())
    }
}
