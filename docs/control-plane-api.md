# Control plane API

The control plane exposes two gRPC methods in `durable_object.v1.ActorControlPlaneService`. TLS terminates at the deployment platform; the Rust service listens on cleartext HTTP/2.

| Operation | Caller | Purpose |
| --- | --- | --- |
| `ResolveActorHost` | workflow | Return the current owner route, starting a compatible regional host through `SandboxProvider` when none is active. |
| `Execute` | host | Carry one authenticated lease, ownership, durability, recovery, or telemetry command. |

`Execute` uses a JSON command envelope plus detached binary payloads so LTX and checkpoint bytes are not base64-encoded.

## Execute commands

| Command | One-sentence contract |
| --- | --- |
| `register_lease` | Register or renew this exact authenticated host/session and its public route. |
| `get_lease_status` | Read whether a namespace-scoped host lease is currently active. |
| `unregister_lease` | End this authenticated host session's lease without affecting a successor session. |
| `get_manifest` | Read the canonical manifest for one namespace-scoped object. |
| `claim` | Acquire ownership with a manifest compare-and-swap after any previous owner's lease expires. |
| `publish` | Atomically advance the manifest with this owner's captured LTX bundle. |
| `recovery` | Return the verified checkpoint and contiguous LTX tail needed to rebuild SQLite. |
| `telemetry_batch` | Forward up to 100 host telemetry events after replacing their scope from the authenticated JWT. |

## Authentication

Every RPC carries `authorization: Bearer <jwt>`. The control plane verifies Ed25519 signatures through `DURABLE_OBJECT_JWKS_URL` and enforces issuer, audience, scope, expiration, namespace, process role, process identity, session, and region.

Required portable claims are:

| Claim | Meaning |
| --- | --- |
| `sub` | Same value as `processId`. |
| `tokenId` | Integration-owned credential identifier used only when asking the provider to bootstrap a host. |
| `namespaceId` | Tenant boundary for objects and hosts. |
| `processId` | `workflow.v1.<namespace>.<uuid>` or `host.v1.<namespace>.<uuid>`. |
| `sessionId` | UUID that fences restarts reusing a process identity. |
| `processRole` | `workflow` or `host`. |
| `storageRegion` | Canonical caller/host region. |
| `codeRevision` | Required for workflows so the provider can select immutable actor code. |
| `scope` | `actor:authority` for control-plane access or `actor:invoke` for direct host invocation. |

The default issuer is `durable-object-control-plane`; default audiences are `durable-object-authority` and `durable-object-invoke`.

## Provider API

When resolution has no active owner, the control plane sends this provider-neutral request to `DURABLE_OBJECT_SANDBOX_PROVIDER_URL`:

```json
{
  "namespaceId": "namespace-1",
  "principalId": "workflow.v1.namespace-1.…",
  "credentialId": "credential-1",
  "codeRevision": "immutable-revision",
  "canonicalRegion": "north-america-east"
}
```

The provider returns `{ hostId, route, canonicalRegion, cacheSource }`. Before routing, the control plane independently verifies that the host ID is namespace-scoped, the region matches, the route is an origin, and an active authoritative lease has exactly the same host ID and route.

The bundled TypeScript HTTP adapter also exposes authenticated lifecycle endpoints:

| Endpoint | Effect |
| --- | --- |
| `POST /hosts/ensure` | Start or reuse compute and return its route. |
| `POST /hosts/status` | Return `serving`, `warm`, or `cold`. |
| `POST /hosts/deactivate` | Stop compute while preserving its volume. |
| `POST /hosts/remove-local-cache` | Stop compute and delete only its disposable volume. |
