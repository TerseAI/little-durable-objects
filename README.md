# Durable object runtime

This repository contains a standalone durable-object system. Terse uses it by mapping each Terse project ID to a runtime namespace, but the runtime supports any integration that can supply namespace IDs.

The system has two deliverables:

- `durable-object-runtime`, a Rust binary that runs the control plane, regional hosts, and durability maintenance;
- `@terse/durable-objects`, the actor API, workflow client, JavaScript host executor, region catalog, and Modal command.

## Architecture

```text
trusted backend                                      Modal
  |                                                    ^
  | admin token                                        | local TypeScript command
  | EnsureNamespace / RegisterLaunchSpec               | (Modal SDK, no server)
  | IssueWorkflowToken                                 |
  v                                                    |
durable-object control plane --------------------------+
  | owns Ed25519 signing, JWKS, placement, leases, and durability
  | injects a host JWT when it creates a host sandbox
  |
  | project-scoped workflow JWT
  v
workflow sandbox -- ResolveActorHost --> control plane
  |
  `---------------- Invoke ------------> regional host sandbox
                                           | Rust host + gRPC
                                           | resident JS actor Workers
                                           | SQLite cache on Modal Volume
                                           ` LTX -> Rapid -> Standard storage
```

The control plane calls a bundled TypeScript executable over stdin/stdout when it needs Modal. There is no sandbox-provider web service. Rust passes a complete launch request; the command uses the Modal TypeScript SDK to create or reuse the sandbox and returns its handle.

## Terse integration

Terse needs one trusted backend credential: the value of `DURABLE_OBJECT_ADMIN_TOKEN` on the control plane. Keep the same value in the Terse backend environment. Workflows never receive it.

For each project deployment, the Terse backend calls `durable_object.v1.ActorAdminService`:

1. `EnsureNamespace({ namespace_id: project.id })`.
2. `RegisterLaunchSpec({ namespace_id: project.id, code_revision, modal_image_id, working_directory, actor_entrypoint })`.
3. Before each workflow execution, `IssueWorkflowToken({ namespace_id: project.id, execution_id, code_revision, region, deadline_unix_ms })`.
4. Inject the reply token and three runtime values into the workflow sandbox:

```text
DURABLE_OBJECT_TOKEN=<issued workflow JWT>
DURABLE_OBJECT_NAMESPACE_ID=<Terse project ID>
DURABLE_OBJECT_CONTROL_PLANE_URL=https://objects.example.com
DURABLE_OBJECT_INVOCATION_TIMEOUT_MS=30000  # optional
```

All three mutating admin RPCs use `authorization: Bearer <DURABLE_OBJECT_ADMIN_TOKEN>`. `GetJwks` is public so hosts and diagnostics can read the system's verification keys. Launch specs are immutable for a `(namespace, code revision)` pair; a changed image or entrypoint needs a new revision.

Application code can rely on the injected environment or configure the client directly:

```ts
import { Actor, configureDurableObjects } from "@terse/durable-objects"

configureDurableObjects({
    token: process.env.DURABLE_OBJECT_TOKEN!,
    namespaceId: process.env.DURABLE_OBJECT_NAMESPACE_ID!,
    controlPlaneUrl: process.env.DURABLE_OBJECT_CONTROL_PLANE_URL!
})

export class Counter extends Actor {
    count = 0

    increment(): number {
        return ++this.count
    }
}

await Counter.get("account-1").increment()
```

The SDK sends the same short-lived JWT for route resolution and direct invocation. It retries only when the runtime proves execution did not start, preserving the same request ID. A transport failure after dispatch returns `outcome_unknown` and is never retried automatically.

## Authentication

The control plane owns one Ed25519 signing key supplied as base64-encoded PKCS#8 in `DURABLE_OBJECT_JWT_SIGNING_KEY`. It issues:

- a workflow JWT scoped to one namespace, execution, revision, region, and workflow deadline, with `actor:resolve actor:invoke`;
- a host JWT scoped to one namespace, host ID, session, revision, and region, with `actor:authority`.

The initial host JWT and verification keys are injected when Modal creates the sandbox. The control plane returns a replacement host JWT through lease renewal before expiration. Modal credentials stay in the control-plane process and its short-lived local command; they never enter workflow or host sandboxes.

## Lifecycle

Object memory and host compute recycle independently:

- A successful actor instance remains resident for `DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS`, default `60000`. The host then terminates its Worker and restores it from durable state on the next call.
- The Rust host exits after `DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS` without active invocations, default `300000`. Because Rust is Modal's main sandbox process, this deactivates compute while preserving the volume cache.
- Both values must be between `1` and `86400000` milliseconds. Active work is never evicted or stopped.
- A host keeps at most 32 resident actor Workers. Each actor is serial and admits at most 32 queued calls; excess calls get a retryable pre-execution `resource_exhausted` result.

The provider cache states remain separate from in-memory residency:

```text
serving -- host idle exit --> warm -- remove local cache --> cold
   ^                            |                            |
   `----------- next invocation restores or reuses --------'
```

None of these transitions deletes canonical object history.

## Control-plane setup

Install or bundle the npm package so `terse-durable-objects-modal` is on `PATH`, then configure the control plane:

```sh
DURABLE_OBJECT_PROCESS_ROLE=control-plane \
DURABLE_OBJECT_STORAGE=local \
DURABLE_OBJECT_LOCAL_STORE_ROOT=.local/control-plane \
DURABLE_OBJECT_JWT_SIGNING_KEY='<base64 PKCS#8 Ed25519 key>' \
DURABLE_OBJECT_ADMIN_TOKEN='<shared backend admin token>' \
DURABLE_OBJECT_CONTROL_PLANE_URL=https://objects.example.com \
MODAL_TOKEN_ID='<modal token id>' \
MODAL_TOKEN_SECRET='<modal token secret>' \
cargo run
```

Set `DURABLE_OBJECT_MODAL_COMMAND` when the compiled command is not on `PATH`. The command receives only `MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET`, and `PATH` from Rust; launch credentials and runtime values travel in its JSON request.

Production storage uses PostgreSQL for manifests, leases, namespaces, and launch specs; a zonal GCS Rapid bucket for synchronous commit logs in each canonical region; and Standard multi-region buckets for archives and consolidated checkpoints.

## Build and test

```sh
cargo build --release
cargo test --no-fail-fast
pnpm --dir npm test
```

Set `DURABLE_OBJECT_TEST_POSTGRES_URL` to run the PostgreSQL integration suite against a real database.
