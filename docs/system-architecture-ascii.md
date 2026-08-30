# System architecture

```text
                         +--------------------------+
                         | Credential issuer (hook) |
                         | - JWT signing + JWKS     |
                         | - bootstrap exchange     |
                         +------------+-------------+
                                      |
                                      | short-lived scoped JWTs
                                      v
+----------------------+     +----------------------+     +----------------------+
| Application process  |     | Rust control plane   |     | SandboxProvider      |
| - Actor.get() proxy   |---->| - resolve + route    |---->| - Modal adapter      |
| - actor definitions   |     | - leases + fencing   |     | - regional volumes   |
| - workflow credential |     | - manifests + LTX    |     | - host lifecycle     |
+----------+-----------+     +----------+-----------+     +----------+-----------+
           |                            |                            |
           | direct invocation gRPC     | authority gRPC             | start/reuse
           |                            |                            v
           |                  +---------+------------------------------+
           +----------------->| Regional durable-object host sandbox   |
                              | - Rust host + gRPC                      |
                              | - JS executor on a private Unix socket |
                              | - SQLite cache on provider volume      |
                              +----------------+------------------------+
                                               |
                                               | synchronous LTX publish
                                               v
                              +----------------+------------------------+
                              | PostgreSQL + object storage             |
                              | - leases and manifests                  |
                              | - regional Rapid commit landing         |
                              | - Standard multi-region checkpoints     |
                              +-----------------------------------------+
```

The control plane is involved in routing and durability, not normal method execution. The provider owns compute lifecycle, not object authority. The host volume is a cache: a manifest watermark mismatch or SQLite integrity failure always falls back to canonical recovery.
