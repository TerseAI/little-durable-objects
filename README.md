# Durable objects

A small, provider-neutral durable-object runtime. Modal is the first sandbox provider; actor state is stored as NDJSON in regional GCS buckets and coordination lives in Postgres.

## Quickstart

1. Create a Postgres database and one GCS `STANDARD` bucket. Give the service account in `GOOGLE_APPLICATION_CREDENTIALS` object access to the bucket.

2. Build the Rust runtime and TypeScript package:

   ```sh
   pnpm install
   pnpm build
   chmod +x npm/dist/providers/modalCli.js
   ```

3. Start the control plane. Its HTTP origin serves the public REST API and the internal host gRPC API, so it must be reachable from Modal with HTTP/2 enabled.

   ```sh
   export DURABLE_OBJECT_PROCESS_ROLE=control_plane
   export DURABLE_OBJECT_POSTGRES_URL='postgresql://localhost/durable_objects?sslmode=disable'
   export DURABLE_OBJECT_STANDARD_BUCKETS='{"north-america-east":"my-actor-state-bucket"}'
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   export DURABLE_OBJECT_CONTROL_PLANE_BIND=0.0.0.0:7100
   export DURABLE_OBJECT_CONTROL_PLANE_URL=https://objects.example.com
   export DURABLE_OBJECT_JWT_SIGNING_KEY="$(openssl genpkey -algorithm Ed25519 -outform DER | base64 | tr -d '\n')"
   export DURABLE_OBJECT_ADMIN_TOKEN="$(openssl rand -hex 32)"
   export DURABLE_OBJECT_SANDBOX_PROVIDER=modal
   export DURABLE_OBJECT_SANDBOX_COMMAND="$PWD/npm/dist/providers/modalCli.js"
   export MODAL_TOKEN_ID=...
   export MODAL_TOKEN_SECRET=...

   ./target/release/durable-object-runtime
   ```

4. Export actors from `src/durable-objects.ts` in your project:

   ```ts
   import { Actor } from "@terse/durable-objects"

   export class Counter extends Actor {
     count = 0
     async increment(): Promise<number> {
       return ++this.count
     }
   }
   ```

5. From your trusted backend, call the JSON API using `Authorization: Bearer $DURABLE_OBJECT_ADMIN_TOKEN`:

   ```text
   PUT  /v1/namespaces/{namespaceId}/deployment
   POST /v1/namespaces/{namespaceId}/workflow-tokens
   ```

   The deployment call atomically ensures the namespace and registers its active deployment. Its body is `{ "codeRevision", "imageRef", "workingDirectory", "actorEntrypoint" }`. The workflow-token body is `{ "executionId", "deadlineUnixMs" }`.

   `image_ref` is a Modal image containing Node.js, your built project, its dependencies, and `durable-object-runtime` at `/usr/local/bin/durable-object-runtime`.

6. Give the issued project token to the workflow and call the actor. The package sends an authenticated JSON `POST` to the control plane:

   ```ts
   import { configureDurableObjects } from "@terse/durable-objects"
   import { Counter } from "./durable-objects.js"

   configureDurableObjects({
     token: process.env.DURABLE_OBJECT_TOKEN!,
     namespaceId: "my-project",
     controlPlaneUrl: "https://objects.example.com",
   })

   await Counter.get("account-1").increment()
   ```

Actors leave memory after 60 seconds idle; empty host sandboxes stop after 5 minutes. Override those defaults with `DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS` and `DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS` on the control plane.

See the [developer guide](docs/developer-guide.md), [Terse integration quickstart](docs/terse-integration.md), and short [architecture diagram](docs/system-architecture-ascii.md).
