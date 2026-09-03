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

   The deployment call atomically ensures the namespace and registers its active deployment. When it replaces an existing deployment, the control plane terminates that revision's cached actor hosts in every configured region before returning; actor state and placement remain durable for reactivation on the new revision. Its body is `{ "codeRevision", "imageRef", "workingDirectory", "actorEntrypoint", "warmRegion" }`; `warmRegion` is optional and starts a disposable background sandbox to warm the image cache without delaying or failing registration. The workflow-token body is `{ "executionId", "deadlineUnixMs", "storageRegion", "privateRouting" }`; `privateRouting` defaults to `false` and should be enabled only for callers in the provider's private network.

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

## WebSockets

Actors can own WebSockets without a context object or an explicit accept step. Define any lifecycle hooks you need and attach JSON-serializable, typed metadata to each connection:

```ts
import { Actor } from "little-durable-objects"
import type { ActorSocket } from "little-durable-objects"

interface Session {
  userId: string
  connectedAt: number
}

export class ChatRoom extends Actor {
  async onConnect(socket: ActorSocket<Session>): Promise<void> {
    socket.setTags("member")
    socket.send("ready")
  }

  async onMessage(socket: ActorSocket<Session>, message: string | Uint8Array): Promise<void> {
    this.broadcast(message)
  }

  async onDisconnect(socket: ActorSocket<Session>, code: number, reason: string, wasClean: boolean): Promise<void> {
    console.log(socket.metadata.userId, code, reason, wasClean)
  }
}
```

Connect from a trusted Node.js workflow with the same configured client:

```ts
const socket = await ChatRoom.get("lobby").connect({
  userId: "user-1",
  connectedAt: Date.now(),
})

socket.addEventListener("message", event => console.log(event.data))
socket.send("hello")
```

`socket.metadata` can be replaced during a lifecycle hook, and `socket.setTags(...)` updates its indexed membership. `this.connections` exposes the actor's current sockets; `this.broadcast(message, { except, tags })` filters them without application-maintained connection maps. `socket.close(...)` works throughout the open lifecycle, while `socket.reject(...)` is available during `onConnect`.

The control-plane gateway owns the network connections, metadata, and tags. After a connection succeeds, the runtime sends it `{ "type": "state", "state": { ... } }` containing the actor's current durable properties; `onConnect` is optional and exists only for custom connection behavior. A socket frame resolves and wakes the actor host, runs the matching lifecycle hook, commits actor state, and then applies its socket effects. The Modal actor sandbox can therefore stop while client sockets remain attached to the gateway. Actor methods invoked by workflows return their socket effects to the caller, which forwards them to the gateway so `this.broadcast(...)` reaches connected clients.

Set `DURABLE_OBJECT_SOCKET_EVENT_URL` and `DURABLE_OBJECT_SOCKET_EVENT_TOKEN` together on the gateway to deliver accepted message events to an authenticated workflow trigger endpoint. Set `DURABLE_OBJECT_SOCKET_AUTH_URL` and `DURABLE_OBJECT_SOCKET_AUTH_TOKEN` together to expose `/v1/socket/{triggerId}/{actorId}` to external clients. Trusted server clients send their trigger key as a Bearer token. Browser clients send `terse-do` and `terse-ticket.<short-lived-ticket>` as WebSocket subprotocols; the gateway negotiates only `terse-do`.

Every authenticated connection can send and receive. Applications model generation status, session history, and presence in actor state, then broadcast later state changes to clients. They may reject or queue messages while busy. The gateway keeps transport connections in process memory, so a gateway restart closes attached clients and they must reconnect.

Current limits are 16 MiB of serialized state per actor, 128 connections per actor, 64 KiB of metadata per connection, 16 MiB per inbound frame/message, 128 tags, and 8 KiB total tag data. Connection metadata must be JSON serializable.

Actors leave memory after 60 seconds idle; empty host sandboxes stop after 5 minutes. Override those defaults with `DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS` and `DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS` on the control plane.

See the [architecture diagram](docs/system-architecture.md) for the request, credential, placement, and state flow.

Release maintainers should follow the [release guide](docs/releasing.md).

## License

MIT © 2026 Terse
