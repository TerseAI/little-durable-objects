use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::{actor::ActorKey, host::ActorProcessRole};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminService, HostLaunchSpec},
    service::{ControlPlaneService, TargetResolutionTimings},
};

#[derive(Clone)]
struct PublicApiState {
    invocations: ControlPlaneService,
    admin: AdminService,
}

pub(super) fn router(invocations: ControlPlaneService, admin: AdminService) -> Router {
    let sockets = super::websocket::router(
        invocations.clone(),
        super::websocket::SocketRegistry::default(),
    );
    Router::new()
        .route("/.well-known/jwks.json", get(jwks))
        .route(
            "/v1/namespaces/{namespace_id}/deployment",
            put(register_deployment),
        )
        .route(
            "/v1/namespaces/{namespace_id}/workflow-tokens",
            post(issue_workflow_token),
        )
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/target",
            post(resolve_actor_target),
        )
        .layer(DefaultBodyLimit::max(MAX_CONTROL_PLANE_MESSAGE_BYTES))
        .with_state(PublicApiState { invocations, admin })
        .merge(sockets)
}

async fn register_deployment(
    State(state): State<PublicApiState>,
    Path(namespace_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RegisterDeploymentRequest>,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let spec = HostLaunchSpec {
        namespace_id,
        code_revision: request.code_revision,
        image_ref: request.image_ref,
        working_directory: request.working_directory,
        actor_entrypoint: request.actor_entrypoint,
    };
    let changed = state
        .invocations
        .register_deployment(&state.admin, &spec)
        .await
        .map_err(ApiError::bad_request)?;
    if let Some(region) = request.warm_region {
        state.invocations.warm_deployment_image(spec, region);
    }
    Ok(Json(DeploymentReply { changed }))
}

async fn issue_workflow_token(
    State(state): State<PublicApiState>,
    Path(namespace_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<IssueWorkflowTokenRequest>,
) -> Result<Json<IssueWorkflowTokenReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    if request.execution_id.is_empty() || request.execution_id.len() > 255 {
        return Err(ApiError::bad_request("workflow execution ID is invalid"));
    }
    if request.deadline_unix_ms <= 0 {
        return Err(ApiError::bad_request("workflow deadline is required"));
    }
    if !state
        .admin
        .deployment_exists(&namespace_id)
        .await
        .map_err(ApiError::internal)?
    {
        return Err(ApiError::conflict("project has no registered actor code"));
    }
    let issued = state
        .admin
        .issue_workflow_token(
            &namespace_id,
            &request.execution_id,
            &request.storage_region,
            request.deadline_unix_ms,
            request.private_routing,
        )
        .map_err(ApiError::bad_request)?;
    Ok(Json(IssueWorkflowTokenReply {
        token: issued.token,
        expires_at_ms: issued.expires_at_ms,
    }))
}

async fn jwks(State(state): State<PublicApiState>) -> Result<Json<Value>, ApiError> {
    let document = serde_json::from_slice(&state.admin.jwks_json().map_err(ApiError::internal)?)
        .map_err(ApiError::internal)?;
    Ok(Json(document))
}

async fn resolve_actor_target(
    State(state): State<PublicApiState>,
    Path((namespace_id, actor_type, actor_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<ActorTargetReply>, ApiError> {
    let mut timings = TargetResolutionTimings::new();
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let actor = ActorKey {
        namespace_id,
        actor_type,
        actor_id,
    };
    let result: Result<Json<ActorTargetReply>, ApiError> = async {
        actor.validate().map_err(ApiError::bad_request)?;
        timings.request_validated_at_ms = Some(timings.elapsed_ms());
        let principal = authorized_workflow(&state.invocations, &headers, &actor)?;
        timings.workflow_authenticated_at_ms = Some(timings.elapsed_ms());
        let target = state
            .invocations
            .resolve_workflow_target_timed(&principal, &actor, &mut timings)
            .await
            .map_err(|error| {
                ApiError::unavailable(format!("actor host is unavailable: {error:#}"))
            })?;
        Ok(Json(ActorTargetReply {
            route: target.route,
            token: target.token,
            owner_epoch: target.owner_epoch,
            state_version: target.state_version,
            state_read_url: target.state_read_url,
            expires_at_ms: target.expires_at_ms,
        }))
    }
    .await;
    let completed_at_ms = timings.elapsed_ms();
    match &result {
        Ok(_) => info!(
            event = "actor_target_resolution",
            request_id,
            namespace_id = %actor.namespace_id,
            actor_type = %actor.actor_type,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            workflow_authenticated_at_ms = timings.workflow_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,
            state_url_signed_at_ms = timings.state_url_signed_at_ms,
            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "resolved",
            "actor target resolution completed"
        ),
        Err(error) => warn!(
            event = "actor_target_resolution",
            request_id,
            namespace_id = %actor.namespace_id,
            actor_type = %actor.actor_type,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            workflow_authenticated_at_ms = timings.workflow_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,
            state_url_signed_at_ms = timings.state_url_signed_at_ms,
            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "failed",
            error_code = %error.code,
            error = %error.message,
            "actor target resolution failed"
        ),
    }
    result
}

fn authorized_workflow(
    service: &ControlPlaneService,
    headers: &HeaderMap,
    actor: &ActorKey,
) -> Result<super::auth::ActorPrincipal, ApiError> {
    let principal = service
        .authenticate_workflow(authorization(headers)?)
        .map_err(|_| ApiError::unauthorized("workflow token was rejected"))?;
    if principal.process_role != ActorProcessRole::Workflow {
        return Err(ApiError::forbidden("credential is not a workflow token"));
    }
    if !principal.scope.contains(actor) {
        return Err(ApiError::forbidden(
            "workflow token cannot cross namespace scope",
        ));
    }
    Ok(principal)
}

fn authorized_admin(admin: &AdminService, headers: &HeaderMap) -> Result<(), ApiError> {
    let authorization = authorization(headers)?;
    admin
        .authenticate(authorization)
        .map_err(|_| ApiError::unauthorized("admin credential was rejected"))?;
    Ok(())
}

fn authorization(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| ApiError::unauthorized("bearer credential is required"))?
        .to_str()
        .map_err(|_| ApiError::unauthorized("bearer credential is invalid"))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterDeploymentRequest {
    code_revision: String,
    image_ref: String,
    working_directory: String,
    #[serde(default)]
    actor_entrypoint: Option<String>,
    #[serde(default)]
    warm_region: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssueWorkflowTokenRequest {
    execution_id: String,
    deadline_unix_ms: i64,
    storage_region: String,
    #[serde(default)]
    private_routing: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentReply {
    changed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IssueWorkflowTokenReply {
    token: String,
    expires_at_ms: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorTargetReply {
    route: String,
    token: String,
    owner_epoch: u64,
    state_version: u64,
    state_read_url: String,
    expires_at_ms: i64,
}

struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
}

impl ApiError {
    fn bad_request(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            error.to_string(),
        )
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            error.to_string(),
        )
    }

    fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorDocument {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorDocument {
    error: ErrorBody,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_token_request_accepts_a_storage_region() {
        let request: IssueWorkflowTokenRequest = serde_json::from_value(serde_json::json!({
            "executionId": "execution-1",
            "deadlineUnixMs": 1_800_000_000_000_i64,
            "storageRegion": "north-america-west",
            "privateRouting": true
        }))
        .unwrap();

        assert_eq!(request.storage_region, "north-america-west");
        assert!(request.private_routing);
    }

    #[test]
    fn workflow_token_request_defaults_to_public_routing() {
        let request: IssueWorkflowTokenRequest = serde_json::from_value(serde_json::json!({
            "executionId": "execution-1",
            "deadlineUnixMs": 1_800_000_000_000_i64,
            "storageRegion": "north-america-west"
        }))
        .unwrap();

        assert!(!request.private_routing);
    }

    #[test]
    fn deployment_registration_accepts_a_background_warm_region() {
        let request: RegisterDeploymentRequest = serde_json::from_value(serde_json::json!({
            "codeRevision": "revision-1",
            "imageRef": "im-actor",
            "workingDirectory": "/workspace",
            "warmRegion": "north-america-west"
        }))
        .unwrap();

        assert_eq!(request.warm_region.as_deref(), Some("north-america-west"));
    }
}
