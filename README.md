# Durable object runtime

This directory is a standalone durable-object system. Terse integrates with it, but the runtime does not depend on Terse projects, deployments, credentials, or backend APIs.

It contains one Rust crate and a companion npm package:

- `durable-object-runtime` runs the control plane, regional hosts, and durability maintenance.
- [`npm/`](npm/) publishes `@terse/durable-objects`: the actor API, JavaScript host executor, canonical region catalog, provider contract, and Modal adapter.

The directory is intentionally self-contained so it can move to its own repository without changing its protocol or runtime architecture.

## Architecture

```text
application
    |
    | resolve through control plane, then invoke host directly
    v
durable-object control plane
    |-- verifies namespace-scoped JWKS credentials
    |-- owns placement, leases, routing, and fencing
    |-- calls a configured SandboxProvider
    `-- coordinates Postgres + object storage durability
                 |
                 v
        regional Modal host sandbox
        |-- Rust runtime
        |-- JavaScript executor
        |-- SQLite cache on a regional Modal Volume
        `-- canonical LTX history in durable storage
```

The sandbox provider starts or reuses one host for a `(namespace, code revision, canonical region)` tuple. A host multiplexes many objects. The control plane remains the authority for leases and routing; the provider volume is only a disposable SQLite cache.

## Object identity

An object is identified by `(namespace_id, actor_type, actor_id)`. The runtime does not know about organizations or projects. An integration maps its own tenancy model to a namespace.

The first claim fixes an object's canonical home region. Provider-specific locations never appear in object identity or manifests. The Modal adapter and storage configuration map the canonical region independently.

## Lifecycle

Runtime and cache state are separate from durable existence:

- `serving`: a live host has a valid lease.
- `warm`: no host is serving, but its regional provider volume remains.
- `cold`: no host or local cache exists; activation restores from durable storage.
- `deleted`: the durable object has been tombstoned.

`SandboxProvider.deactivate()` stops compute while preserving the cache. `removeLocalCache()` stops compute and deletes the provider volume, making the next activation cold. Neither operation deletes durable history.

## Modal adapter

`@terse/durable-objects/modal` implements the sandbox lifecycle. The caller supplies two integration hooks:

```ts
import { ModalSandboxProvider } from "@terse/durable-objects/modal"
import { startSandboxProviderServer } from "@terse/durable-objects/provider-http"

const provider = new ModalSandboxProvider({
  tokenId: process.env.MODAL_TOKEN_ID!,
  tokenSecret: process.env.MODAL_TOKEN_SECRET!,
  controlPlaneUrl: "https://objects.example.com",
  credentialsUrl: "https://identity.example.com/host-credentials",
  hooks: {
    resolveArtifact: async request => ({
      imageId: await imageForRevision(request.codeRevision),
      workingDirectory: "/workspace",
    }),
    issueHostBootstrap: async request => ({
      credential: await bootstrapCredential(request.namespaceId),
    }),
  },
})

await startSandboxProviderServer({
  provider,
  token: process.env.DURABLE_OBJECT_SANDBOX_PROVIDER_TOKEN!,
})
```

Point `DURABLE_OBJECT_SANDBOX_PROVIDER_URL` at the server's `/hosts/ensure` endpoint. Provider credentials stay in the provider process and never enter workflow sandboxes.

Application processes may use environment variables or configure the client directly:

```ts
import { configureDurableObjects } from "@terse/durable-objects"

configureDurableObjects({
  credential: process.env.OBJECT_CREDENTIAL!,
  credentialsUrl: "https://identity.example.com/workflow-credentials",
  controlPlaneUrl: "https://objects.example.com",
  codeRevision: "git-sha",
  region: "north-america-east",
})
```

## Authentication

The Rust control plane and hosts verify Ed25519 JWTs through JWKS. Credentials use the portable namespace, host, session, region, code-revision, and action-scope claims defined by the runtime. The credential issuer remains an integration boundary.

The control plane accepts authority credentials. Hosts accept separate invocation credentials. A host exchanges its bootstrap credential through `DURABLE_OBJECT_CREDENTIALS_URL`; the bootstrap credential is never sent to the control plane.

## Storage

The first production adapter uses:

- PostgreSQL for manifests and host leases.
- A zonal GCS Rapid bucket for synchronous commit logs in each canonical region.
- A Standard multi-region GCS bucket for asynchronous archives and consolidated checkpoints.

The storage traits remain public Rust boundaries. Rapid and GCS are adapters and durability-policy choices, not part of object identity.

## Run locally

```sh
DURABLE_OBJECT_PROCESS_ROLE=control-plane \
DURABLE_OBJECT_JWKS_URL=http://127.0.0.1:3001/.well-known/jwks.json \
DURABLE_OBJECT_STORAGE=local \
DURABLE_OBJECT_LOCAL_STORE_ROOT=.local/control-plane \
cargo run
```

The local store is intended for development and tests. A usable application also needs a credential issuer and either a sandbox-provider endpoint or an already running host.

See [configuration](docs/configuration.md) for every environment variable.

## Build and test

```sh
cargo build --release
cargo test --no-fail-fast
pnpm --filter @terse/durable-objects build
```

The normal Rust suite uses filesystem-backed stores. Set `DURABLE_OBJECT_TEST_POSTGRES_URL` to run the PostgreSQL integration suite against a real database.
