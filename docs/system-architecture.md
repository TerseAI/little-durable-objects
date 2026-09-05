# System architecture

```text
trusted backend
    | REST + admin token                                                   ^ authenticated socket-message event
    | ensure namespace + deployment / retire hosts / warm image / JWT      | + socket authorization
    v                                                                      |
+----------------------------- runtime services ---------------------------+
| HTTP/2 control plane           HTTP/1.1 WebSocket gateway                 |
| public HTTP + internal gRPC    connection registry + socket effects       |
|     | admin request                 |        ^                             |
|     v                               |        | socket effects              |
| AdminService                  target router  |                             |
|     |                               | placement + lease                    |
|     +-------------------------------+----------> Postgres                  |
|                              placement + CAS state head                    |
|                                     |                                     |
|                              HostProvisioner                              |
|                                     |                                     |
| command sandbox provider ---------->+----> Modal Sandbox V2 (GCP pools)   |
|   JSON host handle / lazy public gRPC route | current filesystem API      |
|                                               |                           |
| internal gRPC API <-------- host JWT ---------+----> Rust actor host       |
+---------------------------------------------------------------------------+
          ^                 ^                |                  |
          | workflow JWT    | WebSocket      | public lifecycle | Unix socket
          |                 | + key/ticket   | / method gRPC   | methods + lifecycle
          |                 |                v                  v
       workflow          clients        Rust actor host --> Node actor worker
          ^                                  |
          | queued durable-object event      | signed GET / create-only PUT
          +---- trusted backend              v
                                regional GCS immutable snapshots
```

The trusted backend alone holds the admin token. One PostgreSQL statement ensures a namespace and registers its active deployment. When that deployment changes, the control plane synchronously asks the sandbox provider to terminate the replaced revision's named hosts in every configured region before returning. Termination is best effort and never deletes actor state or placement; the next invocation claims a new host for the active revision and restores the exact snapshot. Registration may also include the region selected for new actors; after persisting the deployment, the control plane starts a best-effort background Modal V2 sandbox from that image in that region and terminates it as soon as its main process runs. This warms provider image caches without creating an actor, placement, lease, or long-lived host, and failures do not change the registration response. Workflows receive a namespace-scoped JWT carrying the requested storage region and use it to resolve an actor through the public HTTP API. Expiration is enforced strictly at authentication, so expired credentials stop at the trust boundary as unauthenticated rather than reaching host resolution. The requested region selects the home for a new actor; an existing actor retains its original home. Resolution returns a short-lived capability bound to one actor, host session, owner epoch, state version, and an exact signed snapshot-read URL. Ordinary workflow invocations call that host directly over gRPC and cache the target until its capability nears expiration. The control plane owns routing, placement, leases, host activation, the authoritative state head, physical WebSockets, and signed state capabilities through injected service boundaries. Customer actor code runs in isolated Node workers without admin, provider, database, or cloud-storage credentials. Cancellation tokens stop host tasks as one tree.

Physical WebSockets terminate at the HTTP/1.1 gateway, while ordinary actor routing and host callbacks continue through the HTTP/2 control plane. The shared listener uses an HTTP/1+HTTP/2 server with upgrade support so WebSocket streams survive the handshake alongside gRPC traffic. Trusted workflow connections use their namespace JWT. External clients connect through a trigger-scoped route with either a stable server credential or a short-lived browser ticket; the gateway validates that credential through an injected Terse authorization endpoint and accepts trusted connection metadata from its response. Every authenticated client can send. Busy/generating state, message queuing, and client-visible presence are actor properties and application behavior rather than gateway roles.

The gateway retains physical connections, JSON metadata, and tags. Each connect, message, or disconnect resolves the actor's stable placement and invokes the current host over its public gRPC route. The host may therefore shut down between frames; the next frame provisions it again and restores the actor from its durable snapshot. The gateway sends the current logical connection catalog with every lifecycle event and applies outbound socket effects only after the actor state commit succeeds. Socket metadata, messages, tags, close frames, and effect payloads are validated at the actor-host and gateway boundaries; invalid effects cannot commit state or mutate live connections. A successful connect automatically sends the new connection a standard state event containing the committed actor properties; a rejected connection receives no snapshot. Effects returned by ordinary workflow actor methods are posted to the gateway, allowing `this.broadcast(...)` outside a socket callback. Workflows can also post a broadcast effect directly with `Actor.get(id).broadcast(...)`; this transient path fans out to currently connected clients without resolving, starting, invoking, or persisting an actor. After an accepted external message, the gateway sends an authenticated event containing its exact trigger ID to Terse, which queues that `durableObject.onMessage` workflow. Gateway connection state is process-local in this milestone, so production pins the gateway to one instance; a restart closes attached clients and they must reconnect.

The Rust-to-Node actor session allows a 32 MiB internal envelope so a 16 MiB actor state plus protocol framing and results can cross the process boundary. It maps oversized requests or responses to `resource_exhausted`; an oversized response also evicts the in-memory actor so unpublished state cannot survive the rejected invocation. Actor state and individual WebSocket messages are limited to 16 MiB. A state-changing invocation uploads a uniquely named snapshot through a create-only signed URL, then asks the control plane to advance the PostgreSQL state head. That single update is conditional on the active host session, owner epoch, and expected state version. The commit response includes a best-effort write ticket for the next version, removing one control-plane round trip from the normal warm path. If a commit response is lost, the host retains the pending snapshot and retries the same idempotent commit before executing another request. Unique snapshot names avoid GCS's same-object write-rate limit; PostgreSQL remains the source of truth for which snapshot is current.

The sandbox provider translates each canonical home region into a Modal GCP placement. The North American defaults use the broader `us-east`, `us-central`, and `us-west` scheduler pools and public HTTP/2 routes for method and socket lifecycle traffic. These pools preserve the actor's logical home but do not guarantee placement in the exact region of its GCS bucket or control plane; benchmarks record the actual Modal region. Custom placements marked for private networking must pin actor hosts and workflow sandboxes to the same exact region, where method invocations can connect directly over i6pn. Hosted workflow JWTs may grant private routing; local and older workflow tokens default to public routing so they never receive an unreachable i6pn address. Private actor hosts advertise their i6pn gRPC address in the lease and resolve the public route lazily for control-plane-dispatched socket lifecycle events. Public actor hosts wait for the HTTP/2 tunnel during activation and advertise that route for both traffic classes.

The workflow client, control plane, provider adapter, and actor host emit one completion event per timed operation:

```text
actor_client_invocation
    |
    +---- request_id ----> actor_target_resolution
    |                          |
    |                          +---- cold path ----> actor_host_provisioning
    |                                                   |
    |                                                   +----> actor_host_startup
    |
    +---- request_id ----> actor_host_invocation ----> actor_state_write
```

Each event uses a process-local monotonic origin and flat `*_at_ms` milestones, with `started_at_ms` equal to zero and `completed_at_ms` marking the observed boundary. Missing milestones identify paths that did not occur: a warm target omits host provisioning, a cached actor state omits download and decode, and an unchanged invocation omits state publication. The provider event separates Rust child-process startup and I/O, Node and Modal SDK loading, resource lookup, sandbox scheduling, readiness observation, route retrieval, metadata persistence, and lease validation. Modal's dashboard remains authoritative for the provider-owned interval between sandbox scheduling and readiness. Events are emitted for successful and failed client, target, startup, invocation, and state-write operations; a provider command failure retains the control-plane completion boundary even when the failed child cannot return its inner milestones. Telemetry excludes credentials, signed URLs, arguments, results, and actor state.

Release images use native builders for each supported architecture:

```text
GitHub release
    |
    +----> Blacksmith amd64 build ----+
    |                                 |
    +----> Blacksmith arm64 build ----+----> Artifact Registry manifest + provenance attestation
```
