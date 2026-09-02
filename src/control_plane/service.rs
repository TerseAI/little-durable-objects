use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tonic::{Request, Response, Status, metadata::MetadataValue, transport::Endpoint};

use crate::{
    actor::{ActorExecutionResult, ActorInvocation, ActorInvocationFailure, ActorKey},
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest, HostInvokeActorRequest, InvokeActorReply,
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
    host_token_issuer: Option<ActorJwtIssuer>,
    routing: Option<RoutingDependencies>,
}

#[derive(Clone)]
struct RoutingDependencies {
    registry: Arc<dyn AdminRegistry>,
    provisioner: Option<Arc<dyn HostProvisioner>>,
    invoker: Arc<dyn HostInvoker>,
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
        let invoker = Arc::new(GrpcHostInvoker::new(
            self.storage_urls.clone(),
            issuer.clone(),
        ));
        self.host_token_issuer = Some(issuer);
        self.routing = Some(RoutingDependencies {
            registry,
            provisioner,
            invoker,
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

    pub(super) async fn invoke_workflow(
        &self,
        principal: ActorPrincipal,
        invocation: ActorInvocation,
    ) -> ActorExecutionResult {
        if let Err(failure) = validate_workflow_invocation(&principal, &invocation) {
            return failure;
        }
        for _ in 0..MAX_ROUTE_ATTEMPTS {
            match self.invoke_once(&invocation, &principal.region).await {
                Ok(ActorExecutionResult::Reroute) => continue,
                Ok(result) => return result,
                Err(failure) => return failure,
            }
        }
        failed(
            "unavailable",
            "actor ownership changed repeatedly before execution",
        )
    }
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

#[cfg(test)]
impl ControlPlaneService {
    fn with_host_invoker(mut self, invoker: Arc<dyn HostInvoker>) -> Self {
        self.routing
            .as_mut()
            .expect("routing must be configured before its host invoker")
            .invoker = invoker;
        self
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
            ControlPlaneCommand::AuthorizeStateWrite {
                actor,
                host_id,
                owner_epoch,
                expected_generation,
            } => {
                self.authorize_state_write(
                    principal,
                    actor,
                    host_id,
                    owner_epoch,
                    expected_generation,
                )
                .await
            }
        }
    }

    async fn invoke_once(
        &self,
        invocation: &ActorInvocation,
        storage_region: &str,
    ) -> std::result::Result<ActorExecutionResult, ActorExecutionResult> {
        let target = self
            .route_actor(&invocation.actor, storage_region)
            .await
            .map_err(|error| {
                failed(
                    "unavailable",
                    format!("actor host is unavailable: {error:#}"),
                )
            })?;
        let routing = self
            .routing
            .as_ref()
            .expect("route_actor requires routing dependencies");
        let reply = routing
            .invoker
            .invoke(invocation.clone(), &invocation.actor, &target)
            .await
            .map_err(host_call_failure)?;
        ActorExecutionResult::try_from(reply).map_err(|error| {
            failed(
                "outcome_unknown",
                format!("actor host returned an invalid reply: {error:#}"),
            )
        })
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

    async fn authorize_state_write(
        &self,
        principal: &ActorPrincipal,
        actor: ActorKey,
        host_id: HostId,
        owner_epoch: u64,
        expected_generation: String,
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
        Ok(ControlPlaneCommandReply::StateWriteUrl {
            url: self
                .storage_urls
                .write_url(&placement.home_region, &actor, &expected_generation)
                .await?,
        })
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
        let handle = self
            .provider
            .ensure_host(&self.request(spec, region)?)
            .await?;
        self.active_lease(handle, region).await
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

#[async_trait]
trait HostInvoker: Send + Sync {
    async fn invoke(
        &self,
        invocation: ActorInvocation,
        actor: &ActorKey,
        target: &RoutedActor,
    ) -> std::result::Result<InvokeActorReply, HostCallError>;
}

struct GrpcHostInvoker {
    storage_urls: Arc<dyn StorageUrlSigner>,
    issuer: ActorJwtIssuer,
}

impl GrpcHostInvoker {
    fn new(storage_urls: Arc<dyn StorageUrlSigner>, issuer: ActorJwtIssuer) -> Self {
        Self {
            storage_urls,
            issuer,
        }
    }
}

#[async_trait]
impl HostInvoker for GrpcHostInvoker {
    async fn invoke(
        &self,
        invocation: ActorInvocation,
        actor: &ActorKey,
        target: &RoutedActor,
    ) -> std::result::Result<InvokeActorReply, HostCallError> {
        let request = Self::request(
            invocation,
            target,
            self.read_url(actor, target).await?,
            self.token(actor, target)?,
        )?;
        ActorHostServiceClient::new(self.connect(target).await?)
            .invoke(request)
            .await
            .map(|reply| reply.into_inner())
            .map_err(|error| {
                HostCallError::OutcomeUnknown(format!(
                    "actor host RPC failed after dispatch: {error}"
                ))
            })
    }
}

impl GrpcHostInvoker {
    async fn read_url(
        &self,
        actor: &ActorKey,
        target: &RoutedActor,
    ) -> std::result::Result<String, HostCallError> {
        self.storage_urls
            .read_url(&target.placement.home_region, actor)
            .await
            .map_err(|error| {
                HostCallError::Unavailable(format!("could not authorize state read: {error:#}"))
            })
    }

    fn token(
        &self,
        actor: &ActorKey,
        target: &RoutedActor,
    ) -> std::result::Result<String, HostCallError> {
        self.issuer
            .issue_host(
                &actor.namespace_id,
                &target.lease.id,
                &target.lease.session_id,
                &target.spec.code_revision,
                &target.placement.home_region,
            )
            .map(|issued| issued.token)
            .map_err(|error| {
                HostCallError::Unavailable(format!("could not issue host credential: {error:#}"))
            })
    }

    async fn connect(
        &self,
        target: &RoutedActor,
    ) -> std::result::Result<tonic::transport::Channel, HostCallError> {
        Endpoint::new(target.lease.route.clone())
            .map_err(|error| HostCallError::Unavailable(format!("host route is invalid: {error}")))?
            .connect()
            .await
            .map_err(|error| {
                HostCallError::Unavailable(format!("could not connect to actor host: {error}"))
            })
    }

    fn request(
        invocation: ActorInvocation,
        target: &RoutedActor,
        read_url: String,
        token: String,
    ) -> std::result::Result<Request<HostInvokeActorRequest>, HostCallError> {
        let mut request = Request::new(HostInvokeActorRequest {
            invocation: Some(invocation.into()),
            owner_epoch: target.placement.owner_epoch,
            state_read_url: read_url,
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse::<MetadataValue<_>>()
                .map_err(|error| {
                    HostCallError::Unavailable(format!(
                        "host credential is invalid metadata: {error}"
                    ))
                })?,
        );
        Ok(request)
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

fn validate_workflow_invocation(
    principal: &ActorPrincipal,
    invocation: &ActorInvocation,
) -> std::result::Result<(), ActorExecutionResult> {
    invocation.validate().map_err(|error| {
        failed(
            "state_error",
            format!("invalid actor invocation: {error:#}"),
        )
    })?;
    if principal.process_role != ActorProcessRole::Workflow
        || !principal.scope.contains(&invocation.actor)
    {
        return Err(failed(
            "unauthenticated",
            "workflow token cannot invoke this actor",
        ));
    }
    Ok(())
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

fn host_call_failure(error: HostCallError) -> ActorExecutionResult {
    match error {
        HostCallError::Unavailable(message) => failed("unavailable", message),
        HostCallError::OutcomeUnknown(message) => failed("outcome_unknown", message),
    }
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

    use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde_json::json;

    use super::super::{ActorTokenPurpose, admin::LocalAdminRegistry};
    use super::*;
    use crate::{
        actor::ActorScope,
        actor_state::ActorStorageKey,
        host_leases::{HostLeaseRegistry, HostLeaseRequest, HostLeaseStatus},
        placement::LocalObjectPlacementStore,
    };

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
        async fn read_url(&self, _region: &str, _actor: &ActorKey) -> Result<String> {
            anyhow::bail!("the injected host invoker should own read authorization")
        }

        async fn write_url(
            &self,
            _region: &str,
            _actor: &ActorKey,
            _expected_generation: &str,
        ) -> Result<String> {
            anyhow::bail!("state writes are outside this test")
        }

        fn regions(&self) -> Vec<String> {
            vec!["us-east".into()]
        }
    }

    struct FakeHostInvoker;

    #[async_trait]
    impl HostInvoker for FakeHostInvoker {
        async fn invoke(
            &self,
            _invocation: ActorInvocation,
            _actor: &ActorKey,
            _target: &RoutedActor,
        ) -> std::result::Result<InvokeActorReply, HostCallError> {
            Ok(ActorExecutionResult::Completed {
                result: json!({ "count": 1 }),
            }
            .into())
        }
    }

    #[tokio::test]
    async fn workflow_routing_uses_the_injected_host_invoker() -> Result<()> {
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
                    session_id: "session-1".into(),
                    route: "http://host.invalid/".into(),
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
            .with_routing(registry, issuer, None)
            .with_host_invoker(Arc::new(FakeHostInvoker));

        let result = service
            .invoke_workflow(
                ActorPrincipal {
                    scope: ActorScope {
                        namespace_id: "project-1".into(),
                    },
                    host_id: HostId::new("workflow.v1.project-1.execution-1"),
                    session_id: uuid::Uuid::new_v4().to_string(),
                    process_role: ActorProcessRole::Workflow,
                    region: "auto".into(),
                    code_revision: None,
                    expires_at: i64::MAX,
                },
                ActorInvocation {
                    request_id: "request-1".into(),
                    actor,
                    method: "increment".into(),
                    args: Vec::new(),
                },
            )
            .await;

        assert_eq!(
            result,
            ActorExecutionResult::Completed {
                result: json!({ "count": 1 })
            }
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
