# System architecture

```text
+----------------------+       admin token       +-------------------------+
| Trusted backend      |------------------------>| Rust control plane      |
| - Terse project ID   | EnsureNamespace         | - JWT issuer + JWKS     |
| - image + revision   | RegisterLaunchSpec      | - leases + fencing      |
| - workflow deadline  | IssueWorkflowToken      | - manifests + LTX       |
+----------+-----------+                         +-----------+-------------+
           |                                                 |
           | inject project JWT                              | JSON over stdin/stdout
           v                                                 v
+----------------------+                         +-------------------------+
| Workflow sandbox     |                         | Modal SDK command       |
| - Actor.get() proxy  |---- ResolveActorHost -->| - TypeScript executable |
| - one workflow JWT   |                         | - no network server     |
+----------+-----------+                         +-----------+-------------+
           |                                                 |
           | direct Invoke gRPC                              | create/reuse
           v                                                 v
                              +------------------------------+-------------+
                              | Regional durable-object host sandbox       |
                              | - Rust host is Modal's main process         |
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
