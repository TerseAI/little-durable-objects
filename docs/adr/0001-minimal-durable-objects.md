# ADR 0001: Reset to a minimal durable-object runtime

Status: Accepted

## Goal

Make durable objects easy to run, understand, and explain. Keep the small set
of distributed guarantees that teach the durable-object model.

## Decisions

- Export only `Actor`, `configureDurableObjects`, and `ActorInvocationError`
  from the package root. Keep `Actor.get(id)`, protected `this.id`, typed remote
  methods, and per-call timeouts. Remove `Actor.signal`, public invocation
  context, JSON utility types, and the extra public error classes.
- Treat routing, ownership, persistence, and sandbox-provider changes as
  internal implementation details; they must not require application rewrites.
- Keep one trusted backend admin credential in environment variables.
- The control plane issues project/namespace-scoped workflow JWTs and
  host-session JWTs from its own signing key.
- A host-session JWT identifies its host, session, and selected region and is
  used only for lease renewal and internal control-plane/host traffic.
- Remove workflow-to-host credentials and public host authorization. Workflow
  JWTs authenticate only to the control plane, and no longer carry a caller-
  selected region.
- Store object updates as NDJSON; remove SQLite, WAL, and LTX.
- Each committed NDJSON record contains the object's complete serialized state.
- Use one durability tier: GCS `STANDARD`. Remove Rapid storage entirely.
- Configure a direct mapping from each supported sandbox region to one nearby
  Standard bucket. An object's home region selects its bucket, and both its
  updates and compacted state remain there.
- Regional Rust hosts read and write their mapped bucket directly using short-
  lived GCS signed URLs issued by the control plane. Sandboxes and actor code
  never receive reusable GCP storage credentials.
- A signed URL authorizes one HTTP method against one derived object key. PUT
  URLs include the expected GCS generation precondition.
- Before issuing a URL, the control plane verifies the requesting host's active
  lease, session, object ownership, owner epoch, and home-region bucket.
- On ownership transfer, the replacement host conditionally rewrites the latest
  blob under the new owner epoch before executing. This advances the GCS
  generation and fences URLs held by the previous owner.
- Store one bounded NDJSON blob per object at
  `objects/{namespace}/{type}/{id}.ndjson`.
- Load the blob on activation, append in memory, and replace the blob before
  returning a successful mutation.
- Treat each actor method as a transaction boundary. Compare its serialized
  state with the last committed state and skip storage when unchanged.
- Return success only after changed state is persisted. If the method throws,
  serialization fails, the state is oversized, or storage fails, evict the
  resident instance; the next call reloads the last committed state.
- Remove executor cancellation. Once an invocation enters the host queue, it
  runs to completion even if its caller times out or disconnects. The object's
  execution gate remains held until the method and any state commit finish.
- Keep invocation timeouts as caller wait limits only. Remove cancel commands,
  cancellation grace timers, and deadline-driven worker termination.
- The executor may retain an internal informational deadline, but it is not a
  public actor API and never drives cancellation.
- Compact inline after 64 records by retaining only the newest record.
- Also compact when the NDJSON blob reaches 4 MiB, whichever happens first.
- Reject a serialized object state larger than 1 MiB without committing it.
- Keep both lifecycle controls: objects leave memory after 60 seconds idle and
  empty sandbox hosts stop after five minutes idle. The existing
  `DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS` and
  `DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS` variables remain the only overrides.
- Use ordinary object-storage reads and writes, not append-specific APIs.
- Keep compaction, with one small set of defaults.
- Keep object identity, serialized execution, durable state, idle eviction,
  reactivation, and one globally selected sandbox provider.
- Keep a minimal distributed ownership model in Postgres:
  `host_id -> session, route, lease_expiry` and
  `object_id -> host_id, owner_epoch, home_region`.
- Keep regional sandbox placement and the minimal
  `sandbox region -> Standard bucket` mapping.
- The control plane chooses an object's home region automatically on first
  activation from the deployment's allowed sandbox regions. Callers do not
  provide a region. Replacement hosts reuse the stored home region.
- Allow at most one active sandbox host per project, code revision, and region.
  A host serves many objects; different objects may execute concurrently while
  calls to one object remain serialized.
- Do not implement fleet autoscaling, load balancing, or capacity-based host
  selection. After host loss or idle shutdown, launch one replacement and
  lazily transfer objects using new owner epochs.
- Support one active actor-code revision per project. A deployment replaces the
  current launch specification; future calls lazily move objects to a host for
  the new revision under a new owner epoch.
- Scope workflow JWTs to the project rather than a code revision. Do not support
  overlapping revisions, canaries, rollback orchestration, or automatic state
  migrations; actor code remains responsible for reading its existing state.
- Limit Postgres to namespaces/projects, launch specifications, host leases and
  sessions, and object owner/epoch/home-region placement.
- Do not store object state, checkpoints, compaction progress, transaction
  positions, log generations, or derivable bucket keys in Postgres.
- Remove LTX-specific manifests, transaction positions, checkpoint epochs,
  archive progress, and storage-tier reconciliation.
- Direct workflow-to-host routing and its host-scoped JWT flow.
- Provider cache volumes and durability tiers.
- Durable request receipts and exactly-once replay.
- The separate maintenance process.
- Production-oriented telemetry beyond structured logs.

## Invocation guarantee

A successful response means the new state was persisted. A failed or timed-out
request may have committed, and retrying it may run the method again.
