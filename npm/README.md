# @terse/durable-objects

The package provides the typed `Actor` API, control-plane HTTP client, JavaScript host executor, canonical-region catalog, provider contract, and bundled Modal command.

Workflow sandboxes receive one system-issued project JWT and use it only with the control plane:

```ts
import { Actor, configureDurableObjects } from "@terse/durable-objects"

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

Calling `configureDurableObjects` is optional when these environment variables are already present:

```text
DURABLE_OBJECT_TOKEN
DURABLE_OBJECT_NAMESPACE_ID
DURABLE_OBJECT_CONTROL_PLANE_URL
```

The control plane selects one sandbox provider globally. For Modal, set `DURABLE_OBJECT_SANDBOX_PROVIDER=modal`; optionally override its executable with `DURABLE_OBJECT_SANDBOX_COMMAND`.

The `terse-durable-objects-modal` executable reads one JSON command from stdin, uses `MODAL_TOKEN_ID` and `MODAL_TOKEN_SECRET` through the Modal TypeScript SDK, and writes one JSON result to stdout. The Rust control plane invokes it locally; no provider HTTP server is required.

Actor hosts conventionally load `src/durable-objects.ts`. See the [runtime repository](https://github.com/TerseAI/durable-objects) for backend configuration, the REST admin API, authentication, and lifecycle behavior.
