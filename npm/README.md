# little-durable-objects

The package provides the typed `Actor` API, control-plane HTTP client, JavaScript host executor, region catalog, and provider contract. It also bundles the Modal command used by the Rust control plane.

Requires Node.js 20 or newer.

```sh
pnpm add little-durable-objects
```

Workflow sandboxes receive one system-issued project JWT. The client uses it to resolve a short-lived actor target through the control plane, then invokes the regional actor host directly over gRPC:

```ts
import { Actor, configureDurableObjects } from "little-durable-objects"

configureDurableObjects({
  token: process.env.DURABLE_OBJECT_TOKEN!,
  namespaceId: process.env.DURABLE_OBJECT_NAMESPACE_ID!,
  controlPlaneUrl: process.env.DURABLE_OBJECT_CONTROL_PLANE_URL!,
})

export class Counter extends Actor {
  count = 0

  async increment(): Promise<number> {
    return ++this.count
  }
}

await Counter.get("account-1").increment()
```

Actors can also own hibernatable WebSockets through lifecycle hooks:

```ts
import { Actor } from "little-durable-objects"
import type { ActorSocket } from "little-durable-objects"

interface Session {
  userId: string
}

export class ChatRoom extends Actor {
  async onMessage(socket: ActorSocket<Session>, message: string | Uint8Array): Promise<void> {
    this.broadcast(message)
  }

  async onDisconnect(socket: ActorSocket<Session>): Promise<void> {
    console.log(`${socket.metadata.userId} left`)
  }
}

const socket = await ChatRoom.get("lobby").connect({ userId: "user-1" })
socket.send("hello")
```

The control-plane gateway retains live sockets, metadata, and tags. Every successful connection automatically receives `{ "type": "state", "state": { ... } }` with the actor's current durable properties, so `onConnect` is only needed for custom behavior. Each lifecycle event wakes the actor host as needed, and ordinary workflow actor methods forward returned socket effects to the gateway. `this.connections`, `this.broadcast(...)`, `socket.setTags(...)`, `socket.close(...)`, and connect-time `socket.reject(...)` cover connection membership and the full lifecycle without a context object.

Calling `configureDurableObjects` is optional when these environment variables are already present:

```text
DURABLE_OBJECT_TOKEN
DURABLE_OBJECT_NAMESPACE_ID
DURABLE_OBJECT_CONTROL_PLANE_URL
```

The control plane selects one sandbox provider globally. For Modal, set `DURABLE_OBJECT_SANDBOX_PROVIDER=modal`; optionally override its executable with `DURABLE_OBJECT_SANDBOX_COMMAND`.

The `little-durable-objects-modal` executable reads one host-provisioning or disposable image-warmup command from stdin, uses `MODAL_TOKEN_ID` and `MODAL_TOKEN_SECRET` through the Modal TypeScript SDK, and writes one JSON result to stdout. The Rust control plane invokes it locally; no provider HTTP server is required.

Actor hosts conventionally load `src/durable-objects.ts`. See the [runtime repository](https://github.com/TerseAI/little-durable-objects) for backend configuration, the REST admin API, authentication, and lifecycle behavior.

## License

MIT © 2026 Terse
