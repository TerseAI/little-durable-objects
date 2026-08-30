# Durable object architecture

The runtime is standalone. Product integrations provide credentials, code artifacts, and a `SandboxProvider`; they do not participate in actor execution or durability.

```text
application process
  - @terse/durable-objects proxy
  - workflow credential
          |
          | 1. resolve(namespace, type, id)
          v
durable-object control plane -----------------------> SandboxProvider
  - JWT/JWKS authentication                            - start/reuse regional host
  - immutable home-region placement                    - preserve/remove cache volume
  - host leases and fencing                            - Modal first; adapters later
  - manifests and durability                                      |
          |                                                        v
          | 2. return active lease route              regional host sandbox
          |                                             - Rust host
          +-------------------------------------------> - JavaScript executor
                    3. direct invocation gRPC           - SQLite cache on volume
                                                        - LTX -> Rapid -> Standard
```

## Main boundaries

### Application

- Defines actor classes and calls `Actor.get(id)`.
- Exchanges its bootstrap credential through an integration-owned credential endpoint.
- Resolves every call through the control plane, then invokes the returned host directly.
- Does not start hosts or receive storage credentials.

### Control plane

- Authenticates every process with short-lived, namespace-scoped JWTs discovered through JWKS.
- Stores the object's immutable canonical home region in its first manifest.
- Treats a live host lease as the only routing authority.
- Calls the configured `SandboxProvider` only when no active owner can serve the object.

### Sandbox provider

- Maps a canonical region to provider-specific placement.
- Starts or reuses one host for a namespace, code revision, and canonical region.
- Preserves the regional volume when compute is deactivated.
- Can delete only the disposable local cache without touching canonical durability.

### Host sandbox

- Multiplexes objects with the same namespace, code revision, and region.
- Executes JavaScript through one private Unix-socket executor connection.
- Serializes each object's invocations, maintains SQLite, and publishes LTX before acknowledging writes.
- Reuses a persisted SQLite cache only when its watermark matches the canonical manifest and SQLite passes an integrity check.

## Identity and placement

The stable object identity is `(namespace_id, actor_type, actor_id)`. Provider names, credentials, code revisions, process IDs, and physical bucket locations are not part of object identity.

The first claim records a canonical home region. A caller in another region still resolves and invokes the owner in that home region. Modal and GCS each map the canonical region independently, allowing either provider to change without rewriting manifests.

## Lifecycle

```text
serving --deactivate--> warm --remove local cache--> cold
   ^                      |                              |
   +------ invoke --------+---------- invoke -----------+
                                  (canonical restore)

deleted: durable tombstone; independent of cache residency
```

- `serving`: an active host lease exists.
- `warm`: compute is stopped, but the provider-local SQLite cache remains.
- `cold`: activation must restore from Rapid/Standard durability.
- `deleted`: canonical object data is tombstoned; cache state is irrelevant.

The current public provider API implements serving, warm, and cold host/cache transitions. The Rust lifecycle types keep durable deletion separate so a future deletion API cannot accidentally equate cache eviction with data deletion.

## Durability

The host publishes immutable LTX before returning a successful mutation. Regional GCS Rapid storage is the synchronous landing tier. Maintenance copies consolidated checkpoints and history to a Standard multi-region bucket, then removes eligible Rapid data after its safety grace period. PostgreSQL stores manifests and leases, not actor contents.
