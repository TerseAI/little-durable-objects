# little-durable-objects

A small, provider-neutral durable-object runtime. Modal is the first sandbox provider; immutable actor-state snapshots live in regional GCS buckets, while placement and each actor's current state head live in Postgres.

## Install

Install the TypeScript API in actor and workflow projects:

```sh
pnpm add little-durable-objects
```

Install the Rust runtime from crates.io:

```sh
cargo install little-durable-objects --locked
```

Container builds can copy the binary from `us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/little-durable-objects:latest`. The image is a runtime base, not a complete actor sandbox: actor images also need Node.js, the project code, and `little-durable-objects` in the project's dependencies.

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

   ./target/release/little-durable-objects
   ```

4. Export actors from `src/durable-objects.ts` in your project:

   ```ts
   import { Actor } from "little-durable-objects"

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

   The deployment call atomically ensures the namespace and registers its active deployment. When it replaces an existing deployment, the control plane terminates that revision's cached actor hosts in every configured region before returning; actor state and placement remain durable for reactivation on the new revision. Its body is `{ "codeRevision", "imageRef", "workingDirectory", "actorEntrypoint", "warmRegion" }`; `warmRegion` is optional and starts a disposable background sandbox to warm the image cache without delaying or failing registration. The workflow-token body is `{ "executionId", "deadlineUnixMs", "storageRegion" }`.

   `storageRegion` selects the home region only when an actor is first created. Later invocations resolve the actor's existing host region.

   `image_ref` is a Modal image containing Node.js, your built project, its dependencies, and `little-durable-objects` at `/usr/local/bin/little-durable-objects`.

6. Give the issued project token to the workflow and call the actor. The package resolves a short-lived actor target through the control plane, caches it, and invokes the regional host directly over gRPC:

   ```ts
   import { configureDurableObjects } from "little-durable-objects"
   import { Counter } from "./durable-objects.js"

   configureDurableObjects({
     token: process.env.DURABLE_OBJECT_TOKEN!,
     namespaceId: "my-project",
     controlPlaneUrl: "https://objects.example.com",
   })

   await Counter.get("account-1").increment()
   ```

Actors leave memory after 60 seconds idle; empty host sandboxes stop after 5 minutes. Override those defaults with `DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS` and `DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS` on the control plane.

See the [architecture diagram](docs/system-architecture.md) for the request, credential, placement, and state flow.

Release maintainers should follow the [release guide](docs/releasing.md).

## License

MIT © 2026 Terse
