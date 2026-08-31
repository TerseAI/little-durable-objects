# System architecture

- Why am I SQLite?
- SQlite DB
- NDJSON --> { id: 1234, name: "ASDF" } --->

Actor.get("")

```text
+----------------------+       admin token       +-------------------------+
| Terse API            |------------------------>| Rust control plane      |
| - Terse project ID   | EnsureNamespace         | - JWT issuer + JWKS     |
| - image + revision   | RegisterLaunchSpec      | - leases + fencing      |
| - workflow deadline  | IssueWorkflowToken      | - manifests + LTX       |
+----------+-----------+                         +-----------+-------------+
           |                                                 |
           | inject project JWT                              | JSON over stdin/stdout
           v                                                 v
+----------------------+                         +-------------------------+
| Workflow sandbox     |                         | Sandbox provider command|
| - Actor.get() proxy  |---- ResolveActorHost -->| - selected globally     |
| - one workflow JWT   |                         | - Modal adapter today   |
+----------+-----------+                         +-----------+-------------+
           |                                                 |
           | direct Invoke gRPC                              | create/reuse
           v                                                 v
                              +------------------------------+-------------+
                              | Regional durable-object host sandbox       |
                              | - Rust host is the provider's main process  |
                              | - JS resident actor Workers                 |
                              | - SQLite cache on provider volume          |
                              +----------------------+---------------------+
                                                     |
                                                     | synchronous LTX publish
                                                     v
                              +----------------------+---------------------+
                              | PostgreSQL + object storage                |
                              | - namespaces, launch specs, leases         |
                              | - manifests and ownership epochs           |
                              | - Rapid logs + Standard checkpoints        |
                              +--------------------------------------------+
```
