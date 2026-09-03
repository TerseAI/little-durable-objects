# System architecture

```text
trusted backend
    |
    | REST + admin token
    | atomically ensure namespace + deployment / retire replaced hosts (+ warm region) / issue workflow JWT
    v
+----------------------------- control plane ------------------------------+
| public HTTP API                                                          |
|     | admin request                 | resolve actor target                 |
|     v                               v                                     |
| AdminService                  target router                               |
|     |                               | placement + lease                    |
|     +-------------------------------+----------> Postgres                  |
|                              placement + CAS state head                    |
|                                     |                                     |
|                              HostProvisioner                              |
|                                     |                                     |
| command sandbox provider ---------->+----> Modal Sandbox V2               |
|   JSON host handle / image warmup         | current filesystem API        |
|                                               |                           |
| internal gRPC API <-------- host JWT ---------+----> Rust actor host       |
+---------------------------------------------------------------------------+
          ^                          |                  |
          | REST + workflow JWT      | direct gRPC      | Unix socket
          | requested initial home   | + target JWT     |
          |                          v                  v
       workflow ---------------------------> Rust host --> Node actor executor
                                                |
                                                | signed GET / create-only PUT
                                                v
                                   regional GCS immutable snapshots
```

The trusted backend alone holds the admin token. One PostgreSQL statement ensures a namespace and registers its active deployment. When that deployment changes, the control plane synchronously asks the sandbox provider to terminate the replaced revision's named hosts in every configured region before returning. Termination is best effort and never deletes actor state or placement; the next invocation claims a new host for the active revision and restores the exact snapshot. Registration may also include the region selected for new actors; after persisting the deployment, the control plane starts a best-effort background Modal V2 sandbox from that image in that region and terminates it as soon as its main process runs. This warms provider image caches without creating an actor, placement, lease, or long-lived host, and failures do not change the registration response. Workflows receive a namespace-scoped JWT carrying the requested storage region and use it to resolve an actor through the public HTTP API. Expiration is enforced strictly at authentication, so expired credentials stop at the trust boundary as unauthenticated rather than reaching host resolution. The requested region selects the home for a new actor; an existing actor retains its original home. Resolution returns a short-lived capability bound to one actor, host session, owner epoch, state version, and an exact signed snapshot-read URL. The workflow invokes that host directly over gRPC and caches the target until its capability nears expiration. The direct target path is the only invocation path; the control plane does not proxy actor calls. The control plane owns routing, placement, leases, host activation, the authoritative state head, and signed state capabilities through injected service boundaries. Customer actor code runs in isolated Node workers without admin, provider, database, or cloud-storage credentials. Cancellation tokens stop host tasks as one tree.

Rust actor hosts serialize each actor's invocations and keep its latest state resident for the host lifetime. The Rust-to-Node actor session enforces its message limit before delivery and maps oversized requests or responses to `resource_exhausted`; an oversized response also evicts the in-memory actor so unpublished state cannot survive the rejected invocation. A state-changing invocation uploads a uniquely named snapshot through a create-only signed URL, then asks the control plane to advance the PostgreSQL state head. That single update is conditional on the active host session, owner epoch, and expected state version. The commit response includes a best-effort write ticket for the next version, removing one control-plane round trip from the normal warm path. If a commit response is lost, the host retains the pending snapshot and retries the same idempotent commit before executing another request. Unique snapshot names avoid GCS's same-object write-rate limit; PostgreSQL remains the source of truth for which snapshot is current.

The sandbox provider translates each canonical home region into Modal's region and cloud constraints. Modal's placement response is authoritative; host activation begins immediately without running a blocking placement probe inside the sandbox.

Both processes emit single-line JSON telemetry. `request_id` correlates direct workflow invocations with `actor_host_invocation` and `actor_state_write` events in Modal. `actor_host_provisioning` separates provider lookup, creation, tunnel, readiness, metadata, and lease-validation latency. Telemetry excludes credentials, signed URLs, arguments, results, and actor state.

Release images use native builders for each supported architecture:

```text
GitHub release
    |
    +----> Blacksmith amd64 build ----+
    |                                 |
    +----> Blacksmith arm64 build ----+----> Artifact Registry manifest + provenance attestation
```
