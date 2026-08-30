# @terse/durable-objects

Provider-neutral durable objects for sandbox compute. The package includes:

- the typed `Actor` API and direct gRPC application client;
- the JavaScript executor used inside a regional host sandbox;
- a stable canonical-region catalog;
- a generic `SandboxProvider` contract and authenticated HTTP bridge;
- the first provider adapter for Modal sandboxes and Volumes.

```ts
import { Actor, configureDurableObjects } from "@terse/durable-objects"

configureDurableObjects({
  credential: process.env.OBJECT_CREDENTIAL!,
  credentialsUrl: "https://identity.example.com/workflow-credentials",
  controlPlaneUrl: "https://objects.example.com",
  codeRevision: "git-sha",
  region: "north-america-east",
})

export class Counter extends Actor {
  count = 0

  async increment(): Promise<number> {
    return ++this.count
  }
}

await Counter.get("account-1").increment()
```

Actor hosts conventionally load `src/durable-objects.ts`. See the [architecture and self-hosting documentation](https://github.com/TerseAI/durable-objects) in the source repository.
