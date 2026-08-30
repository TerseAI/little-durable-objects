use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use tonic::{Request, Response, Status};

use crate::{
    actor_state::ActorStorageKey,
    durability::{ActorDurabilityStore, VersionedActorManifest},
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest, ResolveActorHostRequest, ResolvedActorHost,
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
    },
    host::{ActorProcessRole, HostId},
    host_leases::HostLeaseStore,
    sandbox::{EnsureHostRequest, SandboxProvider},
    telemetry::{ActorTelemetry, noop_actor_telemetry},
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    auth::{ActorJwtVerifier, ActorPrincipal},
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_command, encode_reply},
};

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

    pub fn into_service(self) -> ActorControlPlaneServiceServer<Self> {
        ActorControlPlaneServiceServer::new(self)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
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
                Ok(ControlPlaneCommandReply::Lease {
                    lease: self.leases.register(&request).await?,
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
        let provisioned = provider
            .ensure_host(&EnsureHostRequest {
                namespace_id: principal.scope.namespace_id.clone(),
                principal_id: principal.host_id.as_str().to_owned(),
                credential_id: principal.token_id.clone(),
                code_revision: code_revision.clone(),
                canonical_region: target_region.to_owned(),
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
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use tempfile::TempDir;

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
        provisioned: ActorHostHandle,
        regions: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SandboxProvider for RecordingProvisioner {
        async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
            self.regions
                .lock()
                .map_err(|_| anyhow::anyhow!("activation recorder poisoned"))?
                .push(request.canonical_region.clone());
            Ok(self.provisioned.clone())
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

    fn principal(node: &str, session: &str) -> ActorPrincipal {
        ActorPrincipal {
            scope: ActorScope {
                namespace_id: "namespace-1".into(),
            },
            token_id: "project-token-1".into(),
            host_id: HostId::new(node),
            session_id: session.into(),
            process_role: ActorProcessRole::Host,
            region: "default".into(),
            code_revision: None,
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
        let host = ActorPrincipal {
            region: "us-east".into(),
            ..principal(
                "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "00000000-0000-4000-8000-000000000011",
            )
        };
        let base = ControlPlaneService::new(store, leases, test_verifier()?);
        register(&base, &host, 60_000).await?;
        let provisioner = Arc::new(RecordingProvisioner {
            provisioned: ActorHostHandle {
                host_id: host.host_id.clone(),
                route: "https://actor-host.example.com".into(),
                canonical_region: "us-east".into(),
                cache_source: CacheSource::DurableStorage,
            },
            regions: Mutex::new(Vec::new()),
        });
        let service = base.with_sandbox_provider(provisioner.clone());

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
        let next_host = ActorPrincipal {
            region: "eu-west".into(),
            ..principal(
                "host.v1.namespace-1.00000000-0000-4000-8000-000000000002",
                "00000000-0000-4000-8000-000000000012",
            )
        };
        let base = ControlPlaneService::new(store, leases, test_verifier()?);
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
        register(&base, &next_host, 60_000).await?;
        let provisioner = Arc::new(RecordingProvisioner {
            provisioned: ActorHostHandle {
                host_id: next_host.host_id.clone(),
                route: "https://actor-host.example.com".into(),
                canonical_region: "eu-west".into(),
                cache_source: CacheSource::DurableStorage,
            },
            regions: Mutex::new(Vec::new()),
        });
        let service = base.with_sandbox_provider(provisioner.clone());

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
