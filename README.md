# Durable object runtime

This repository contains a standalone durable-object system.

[Architecture diagram](docs/system-architecture-ascii.md)

## Quickstart

A project maps to one durable-object namespace. Its trusted backend holds the admin credential; each workflow receives a short-lived, project-scoped JWT.

1. Authenticate to GCP, then build and start the control plane:

   ```sh
   gcloud auth application-default login
   pnpm install
   pnpm build
   chmod +x npm/dist/providers/modalCli.js

   export DURABLE_OBJECT_PROCESS_ROLE=control-plane
   export DURABLE_OBJECT_STORAGE=rapid
   export DURABLE_OBJECT_POSTGRES_URL=postgresql://postgres@127.0.0.1:5432/durable_objects?sslmode=disable
   export DURABLE_OBJECT_RAPID_BUCKETS='{"north-america-east":"replace-with-rapid-bucket"}'
   export DURABLE_OBJECT_STANDARD_BUCKETS='{"north-america-east":"replace-with-checkpoint-bucket"}'
   export DURABLE_OBJECT_CONTROL_PLANE_BIND=0.0.0.0:7100
   export DURABLE_OBJECT_CONTROL_PLANE_URL=https://durable-objects.example.com
   export DURABLE_OBJECT_JWT_SIGNING_KEY="$(openssl genpkey -algorithm Ed25519 -outform DER | base64 | tr -d '\n')"
   export DURABLE_OBJECT_ADMIN_TOKEN="$(openssl rand -hex 32)"
   export DURABLE_OBJECT_SANDBOX_PROVIDER=modal
   export DURABLE_OBJECT_SANDBOX_COMMAND="$PWD/npm/dist/providers/modalCli.js"
   export MODAL_TOKEN_ID=...
   export MODAL_TOKEN_SECRET=...

   ./target/release/durable-object-runtime
   ```

   The Rapid bucket must be zonal with `RAPID` storage; the checkpoint bucket must use `STANDARD` storage in the `US`, `EU`, or `ASIA` multi-region. Both maps must use the same actor-region keys. The control-plane URL must be an HTTP/2 gRPC endpoint reachable from the sandbox provider.

2. In the application, install `@terse/durable-objects` and export actor classes from `src/durable-objects.ts`:

   ```sh
   pnpm add @terse/durable-objects
   # For a sibling checkout, use: pnpm add @terse/durable-objects@file:../durable-objects/npm
   ```

   ```ts
   import { Actor } from "@terse/durable-objects"

   export class Counter extends Actor {
     count = 0

     async increment(): Promise<number> {
       return ++this.count
     }
   }
   ```

3. Build a Modal image containing Node.js, the application and its dependencies, and `durable-object-runtime` at `/usr/local/bin/durable-object-runtime`. Record its Modal image ID.

4. From the trusted backend, call the admin service in [`proto/durable_object.proto`](proto/durable_object.proto) with `authorization: Bearer <DURABLE_OBJECT_ADMIN_TOKEN>`:

   - Once per project: `EnsureNamespace(namespace_id)`.
   - Once per deploy: `RegisterLaunchSpec(namespace_id, code_revision, image_ref, working_directory, actor_entrypoint)`.
   - Once per workflow: `IssueWorkflowToken(namespace_id, execution_id, code_revision, region, deadline_unix_ms)`. The region must match a configured bucket-map key.

5. Inject the issued token into the workflow and use the actor:

   ```sh
   DURABLE_OBJECT_TOKEN=<IssueWorkflowToken.token>
   DURABLE_OBJECT_NAMESPACE_ID=my-project
   DURABLE_OBJECT_CONTROL_PLANE_URL=https://durable-objects.example.com
   ```

   ```ts
   await Counter.get("account-1").increment()
   ```

Actors leave memory after 60 seconds idle; empty Modal host sandboxes stop after 5 minutes. Override these with `DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS` and `DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS` on the control plane.
