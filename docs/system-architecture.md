# System architecture

```text
trusted backend
    |
    | REST + admin token
    | atomically ensure namespace + deployment / issue workflow JWT
    v
+----------------------------- control plane ------------------------------+
| public HTTP API                                                          |
|     | admin request                 | invocation                          |
|     v                               v                                     |
| AdminService                  invocation router                           |
|     |                               | placement + lease                    |
|     +-------------------------------+----------> Postgres                  |
|                                      (Refinery migrations)                 |
|                                     |                                     |
|                       HostProvisioner / HostInvoker                        |
|                                     |                                     |
| command sandbox provider ---------->+----> Modal Sandbox V2               |
|   JSON host handle + phase timings        | current filesystem API        |
|                                               |                           |
| internal gRPC API <-------- host JWT ---------+----> Rust actor host       |
+---------------------------------------------------------------------------+
          ^                                             |             |
          | REST + workflow JWT                         | Unix socket | signed GET /
          | requested initial home                     |             |
          |                                             v             | conditional PUT
       workflow                                  Node actor executor   v
                                                                  regional GCS
                                                                   NDJSON state
```

The trusted backend alone holds the admin token. One PostgreSQL statement ensures a namespace and registers its active deployment. Workflows receive a namespace-scoped JWT carrying the requested storage region and invoke actors only through the public HTTP API. The requested region selects the home for a new actor; an existing actor retains its original home and the control plane routes the invocation over gRPC to that region. The control plane owns routing, placement, leases, host activation, and signed state capabilities through injected service boundaries. Internal gRPC uses host-scoped JWTs. Rust actor hosts serialize execution and persistence; customer actor code runs in isolated Node workers without admin, provider, database, or cloud-storage credentials. Cancellation tokens stop host tasks as one tree.

Both processes emit single-line JSON telemetry. `request_id` correlates `actor_invocation` and `actor_host_rpc` in the control plane with `actor_host_invocation` and `actor_state_write` in Modal. `actor_host_provisioning` separates provider lookup, creation, placement, tunnel, readiness, metadata, and lease-validation latency. Telemetry excludes credentials, signed URLs, arguments, results, and actor state.

Release images use native builders for each supported architecture:

```text
GitHub release
    |
    +----> Blacksmith amd64 build ----+
    |                                 |
    +----> Blacksmith arm64 build ----+----> Artifact Registry manifest + provenance attestation
```
