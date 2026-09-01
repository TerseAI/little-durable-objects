use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    actor::{ActorExecutionResult, ActorInvocation, ActorInvocationFailure, ActorKey},
    host::ActorProcessRole,
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::HostLaunchSpec,
    service::{AdminDependencies, ControlPlaneService},
};

pub(super) fn router(service: ControlPlaneService) -> Router {
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
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/invocations",
            post(invoke_actor),
        )
        .layer(DefaultBodyLimit::max(MAX_CONTROL_PLANE_MESSAGE_BYTES))
        .with_state(service)
}

async fn register_deployment(
    State(service): State<ControlPlaneService>,
    Path(namespace_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RegisterDeploymentRequest>,
) -> Result<Json<DeploymentReply>, ApiError> {
    let admin = authorized_admin(&service, &headers)?;
    let changed = admin
        .registry
        .register_deployment(&HostLaunchSpec {
            namespace_id,
            code_revision: request.code_revision,
            image_ref: request.image_ref,
            working_directory: request.working_directory,
            actor_entrypoint: request.actor_entrypoint,
        })
        .await
        .map_err(ApiError::bad_request)?;
    Ok(Json(DeploymentReply { changed }))
}

async fn issue_workflow_token(
    State(service): State<ControlPlaneService>,
    Path(namespace_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<IssueWorkflowTokenRequest>,
) -> Result<Json<IssueWorkflowTokenReply>, ApiError> {
    let admin = authorized_admin(&service, &headers)?;
    if request.execution_id.is_empty() || request.execution_id.len() > 255 {
        return Err(ApiError::bad_request("workflow execution ID is invalid"));
    }
    if request.deadline_unix_ms <= 0 {
        return Err(ApiError::bad_request("workflow deadline is required"));
    }
    if admin
        .registry
        .launch_spec(&namespace_id)
        .await
        .map_err(ApiError::internal)?
        .is_none()
    {
        return Err(ApiError::conflict("project has no registered actor code"));
    }
    let issued = admin
        .issuer
        .issue_workflow(
            &namespace_id,
            &request.execution_id,
            request.deadline_unix_ms,
        )
        .map_err(ApiError::bad_request)?;
    Ok(Json(IssueWorkflowTokenReply {
        token: issued.token,
        expires_at_ms: issued.expires_at_ms,
    }))
}

async fn jwks(State(service): State<ControlPlaneService>) -> Result<Json<Value>, ApiError> {
    let admin = service
        .admin()
        .ok_or_else(|| ApiError::unavailable("JWT issuer is not configured"))?;
    let document = serde_json::from_slice(&admin.issuer.jwks_json().map_err(ApiError::internal)?)
        .map_err(ApiError::internal)?;
    Ok(Json(document))
}

async fn invoke_actor(
    State(service): State<ControlPlaneService>,
    Path((namespace_id, actor_type, actor_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(request): Json<InvokeActorRequest>,
) -> Response {
    let request_id = request.request_id.clone();
    match invoke_actor_inner(
        service,
        headers,
        ActorInvocation {
            request_id: request.request_id,
            actor: ActorKey {
                namespace_id,
                actor_type,
                actor_id,
            },
            method: request.method,
            args: request.args,
        },
    )
    .await
    {
        Ok(result) => invocation_response(&request_id, result),
        Err(error) => error.with_request_id(request_id).into_response(),
    }
}

async fn invoke_actor_inner(
    service: ControlPlaneService,
    headers: HeaderMap,
    invocation: ActorInvocation,
) -> Result<ActorExecutionResult, ApiError> {
    invocation.validate().map_err(ApiError::bad_request)?;
    let authorization = authorization(&headers)?;
    let principal = service
        .authenticate_workflow(authorization)
        .map_err(|_| ApiError::unauthorized("workflow token was rejected"))?;
    if principal.process_role != ActorProcessRole::Workflow {
        return Err(ApiError::forbidden("credential is not a workflow token"));
    }
    if !principal.scope.contains(&invocation.actor) {
        return Err(ApiError::forbidden(
            "workflow token cannot cross namespace scope",
        ));
    }

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = reply_tx.send(service.invoke_workflow(principal, invocation).await);
    });
    match reply_rx.await {
        Ok(result) => Ok(result),
        Err(_) => Ok(failed(
            "outcome_unknown",
            "actor invocation task stopped without a reply",
        )),
    }
}

fn authorized_admin<'a>(
    service: &'a ControlPlaneService,
    headers: &HeaderMap,
) -> Result<&'a AdminDependencies, ApiError> {
    let admin = service
        .admin()
        .ok_or_else(|| ApiError::unavailable("admin API is not configured"))?;
    let authorization = authorization(headers)?;
    service
        .authenticate_admin(authorization)
        .map_err(|_| ApiError::unauthorized("admin credential was rejected"))?;
    Ok(admin)
}

fn authorization(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| ApiError::unauthorized("bearer credential is required"))?
        .to_str()
        .map_err(|_| ApiError::unauthorized("bearer credential is invalid"))
}

fn invocation_response(request_id: &str, result: ActorExecutionResult) -> Response {
    match result {
        ActorExecutionResult::Completed { result } => {
            (StatusCode::OK, Json(InvocationReply { result })).into_response()
        }
        ActorExecutionResult::Failed { failure } => {
            actor_failure(request_id, failure).into_response()
        }
        ActorExecutionResult::HostUnavailable => ApiError::unavailable("actor host is draining")
            .with_request_id(request_id)
            .into_response(),
        ActorExecutionResult::Reroute => ApiError::internal("control plane exhausted routing")
            .with_request_id(request_id)
            .into_response(),
    }
}

fn actor_failure(request_id: &str, failure: ActorInvocationFailure) -> ApiError {
    let status = match failure.code.as_str() {
        "unauthenticated" => StatusCode::FORBIDDEN,
        "resource_exhausted" => StatusCode::TOO_MANY_REQUESTS,
        "unavailable" => StatusCode::SERVICE_UNAVAILABLE,
        "outcome_unknown" => StatusCode::BAD_GATEWAY,
        "actor_error" => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiError::new(status, failure.code, failure.message).with_request_id(request_id)
}

fn failed(code: impl Into<String>, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterDeploymentRequest {
    code_revision: String,
    image_ref: String,
    working_directory: String,
    #[serde(default)]
    actor_entrypoint: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssueWorkflowTokenRequest {
    execution_id: String,
    deadline_unix_ms: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InvokeActorRequest {
    request_id: String,
    method: String,
    args: Vec<Value>,
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
struct InvocationReply {
    result: Value,
}

struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
    request_id: Option<String>,
}

impl ApiError {
    fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            request_id: None,
        }
    }

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

    fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
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
                    request_id: self.request_id,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_distributed_outcomes_to_http_statuses() {
        assert_eq!(
            actor_failure(
                "request-1",
                ActorInvocationFailure {
                    code: "outcome_unknown".into(),
                    message: "unknown".into(),
                },
            )
            .status,
            StatusCode::BAD_GATEWAY
        );
    }
}
