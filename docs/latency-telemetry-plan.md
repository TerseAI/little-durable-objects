# Latency telemetry refactor plan

## Goal

Make cold activation, warm reads, and warm writes explainable from the existing structured logs. Keep the design development-oriented: add timing fields only, with no tracing backend, span hierarchy, sampling, latency thresholds, classification flags, counters, payload metadata, or protocol changes.

Each operation emits one completion event with flat `*_at_ms` milestones measured from its own monotonic start. Failed operations use the same timing schema and omit milestones they never reached. Existing outcome and error fields remain unchanged.

The milestone fields replace the current ambiguous duration fields. No compatibility aliases are retained while the runtime is still under development.

## Event contracts

All events include `started_at_ms` and `completed_at_ms`. They retain the event names, identifiers, outcomes, and errors already available at that boundary. No new non-timing metadata is introduced.

### `actor_client_invocation`

Emitted by the workflow client for every invocation.

- `invocation_built_at_ms`
- `target_cache_checked_at_ms`
- `target_resolved_at_ms`
- `host_rpc_completed_at_ms`
- `socket_effects_completed_at_ms`
- `completed_at_ms`

This event measures the latency visible to the workflow and separates control-plane resolution, the host RPC, and socket-effect delivery.

### `actor_target_resolution`

Emitted by the control-plane target endpoint.

- `request_validated_at_ms`
- `workflow_authenticated_at_ms`
- `deployment_loaded_at_ms`
- `placement_loaded_at_ms`
- `lease_checked_at_ms`
- `host_ensured_at_ms`
- `placement_claimed_at_ms`
- `state_url_signed_at_ms`
- `invocation_token_issued_at_ms`
- `route_selected_at_ms`
- `completed_at_ms`

This event accounts for request time that currently sits outside `actor_host_provisioning`. Milestones that do not apply to the active-host fast path are omitted.

### `actor_host_provisioning`

Emitted by the control plane after the provider command finishes.

Outer command milestones:

- `provider_process_spawned_at_ms`
- `provider_request_written_at_ms`
- `provider_process_completed_at_ms`
- `provider_response_decoded_at_ms`

Modal-provider milestones returned with the host handle use their own `modal_` prefix and process-local origin:

- `modal_provider_started_at_ms`
- `modal_input_parsed_at_ms`
- `modal_sdk_loaded_at_ms`
- `modal_resources_resolved_at_ms`
- `modal_existing_host_checked_at_ms`
- `modal_sandbox_scheduled_at_ms`
- `modal_host_ready_observed_at_ms`
- `modal_route_read_at_ms`
- `modal_metadata_written_at_ms`
- `modal_provider_completed_at_ms`

The event retains its existing Modal sandbox ID and region identifiers. The Modal dashboard remains authoritative for its scheduled, started, and ready lifecycle events. Client debug logs are not parsed.

### `actor_host_startup`

Emitted once by a new Rust actor-host process.

- `configuration_loaded_at_ms`
- `authentication_ready_at_ms`
- `control_plane_connected_at_ms`
- `listener_bound_at_ms`
- `private_route_resolved_at_ms`
- `executor_attached_at_ms`
- `lease_registered_at_ms`
- `executor_notified_at_ms`
- `completed_at_ms`

Node initialization remains represented by the Rust-measured executor-attachment interval; no worker timings are added to the internal protocol.

### `actor_host_invocation`

Emitted by the Rust host for every successful or failed method and socket invocation.

- `queue_admitted_at_ms`
- `state_cache_checked_at_ms`
- `state_downloaded_at_ms`
- `state_decoded_at_ms`
- `pending_commit_resolved_at_ms`
- `actor_execution_completed_at_ms`
- `state_publication_completed_at_ms`
- `completed_at_ms`

Actor execution remains one Unix-socket round trip. Cold and warm behavior is inferred from reached milestones: a state download indicates a cache miss, while its absence indicates the cached path.

### `actor_state_write`

Emitted for every attempted state mutation.

- `write_ticket_ready_at_ms`
- `snapshot_created_at_ms`
- `snapshot_encoded_at_ms`
- `snapshot_uploaded_at_ms`
- `commit_rpc_completed_at_ms`
- `completed_at_ms`

This separates GCS upload latency from the control-plane commit RPC. The host cannot split the PostgreSQL update from next-ticket creation because both arrive in one existing response, and this plan does not add another protocol.

## Implementation sequence

1. Add small Rust and TypeScript process-local timeline helpers. They own monotonic offsets, flat field naming, outcomes, and error stages; they do not implement tracing.
2. Refactor the command sandbox provider and Modal CLI to preserve outer process milestones and return inner Modal milestones in a typed success/failure envelope. Rename the handle-return boundary to `modal_sandbox_scheduled_at_ms`, matching Modal V2 semantics.
3. Instrument Rust actor-host startup and attach supported Modal environment identifiers.
4. Refactor state loading so network completion and snapshot decoding are separately observable, then enrich host invocation and state-write completion events.
5. Instrument the workflow client and propagate its existing `request_id` to target resolution. Add the control-plane target-resolution completion event and routing-attempt state.
6. Update `docs/system-architecture.md` with the six event families and their correlation boundaries. Add Cloud Logging query examples for one cold activation, warm read, warm write, and failure.

## Verification

Follow TDD for each production change:

- TypeScript provider tests use the existing fake Modal client and clock to assert cold, reused, and failure milestone sequences.
- Command-provider tests use an injected process runner or controlled child command to verify spawn, I/O, exit, decode, and failure milestones.
- Workflow-client tests use injected fetch, host transport, clock, and logger dependencies to cover target resolution, host RPC, socket effects, reroute, and failures.
- Rust control-plane tests capture structured events for active-host resolution, cold provisioning, placement contention, authentication failure, and provider failure.
- Rust host tests cover cached state, first load, unchanged state, successful mutation, pending-commit recovery, and every state-write failure boundary.
- Startup tests exercise successful initialization and failures before executor attachment and lease registration.
- Existing Rust and npm suites, formatting, linting, and architecture checks run before release.

After release, validate with three repeated operations against one actor: the first invocation should show host creation and state miss, the second an unchanged-state warm read, and the third a warm mutation with GCS and PostgreSQL milestones.
