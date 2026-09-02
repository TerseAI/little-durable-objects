# System architecture

```text
trusted backend
    |
    | REST + admin token
    | atomically ensure namespace + deployment (+ warm region) / issue workflow JWT
    v
+----------------------------- control plane ------------------------------+
| public HTTP API                                                          |
|     | admin request                 | resolve actor target                 |
|     v                               v                                     |
| AdminService                  target router                               |
|     |                               | placement + lease                    |
|     +-------------------------------+----------> Postgres                  |
|                                      (Refinery migrations)                 |
|                                     |                                     |
|                              HostProvisioner                              |
|                                     |                                     |
| command sandbox provider ---------->+----> Modal Sandbox V2               |
|   JSON host handle / image warmup         | current filesystem API        |
|                                               |                           |
| internal gRPC API <-------- host JWT ---------+----> Rust actor host       |
+---------------------------------------------------------------------------+
          ^                          |                  |             |
          | REST + workflow JWT      | direct gRPC      | Unix socket | signed GET /
          | requested initial home   | + target JWT     |             |
          |                          v                  v             | conditional PUT
       workflow ---------------------------> Rust host --> Node actor executor
                                                |
                                                v
                                         regional GCS NDJSON state
```

The trusted backend alone holds the admin token. One PostgreSQL statement ensures a namespace and registers its active deployment. Registration may include the region selected for new actors; after persisting the deployment, the control plane starts a best-effort background Modal V2 sandbox from that image in that region and terminates it as soon as its main process runs. This warms provider image caches without creating an actor, placement, lease, or long-lived host, and failures do not change the registration response. Workflows receive a namespace-scoped JWT carrying the requested storage region and use it to resolve an actor through the public HTTP API. The requested region selects the home for a new actor; an existing actor retains its original home. Resolution returns a short-lived capability bound to one actor, host session, owner epoch, and signed state-read URL. The workflow invokes that host directly over gRPC and caches the target until its capability nears expiration. The control plane owns routing, placement, leases, host activation, and signed state capabilities through injected service boundaries. Rust actor hosts serialize execution and persistence; customer actor code runs in isolated Node workers without admin, provider, database, or cloud-storage credentials. Cancellation tokens stop host tasks as one tree. The proxied HTTP invocation remains available for older clients.

The sandbox provider translates each canonical home region into Modal's region and cloud constraints. Modal's placement response is authoritative; host activation begins immediately without running a blocking placement probe inside the sandbox.

Both processes emit single-line JSON telemetry. `request_id` correlates proxied `actor_invocation` and `actor_host_rpc` events with `actor_host_invocation` and `actor_state_write` in Modal; direct invocations begin at the host. `actor_host_provisioning` separates provider lookup, creation, tunnel, readiness, metadata, and lease-validation latency. Telemetry excludes credentials, signed URLs, arguments, results, and actor state.

Release images use native builders for each supported architecture:

```text
GitHub release
    |
    +----> Blacksmith amd64 build ----+
    |                                 |
    +----> Blacksmith arm64 build ----+----> Artifact Registry manifest + provenance attestation
```
