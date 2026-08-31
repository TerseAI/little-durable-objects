import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"

import { loadActorEntrypoint } from "../actorModule.js"

import { ActorWorkerSupervisor } from "./supervisor.js"

const actorIdentity = {
    namespace_id: "namespace-1",
    actor_type: "SessionCounter",
    actor_id: "counter-1"
}

test("continues an invocation after its caller deadline and preserves ordering", async () => {
    const consumerRoot = await createTypeScriptConsumer()
    const entrypoint = pathToFileURL(path.join(consumerRoot, "src/durable-objects.ts")).href
    try {
        await exerciseContinuedExecution(entrypoint)
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

async function exerciseContinuedExecution(entrypoint: string): Promise<void> {
    assert.deepEqual(await loadActorEntrypoint(entrypoint), ["SessionCounter"])
    const runtime = new ActorWorkerSupervisor({ actorEntrypointUrl: entrypoint })

    const invocation = runtime.handle({
        type: "invoke",
        request_id: "slow-request",
        actor: actorIdentity,
        method: "incrementAfter",
        args: [20, 2],
        state: null,
        timeout_ms: 1
    })
    await Promise.resolve()

    const queued = runtime.handle({
        type: "invoke",
        request_id: "parallel-request",
        actor: actorIdentity,
        method: "increment",
        args: [3],
        state: { count: 0 },
        timeout_ms: 30_000
    })

    assert.deepEqual(await invocation, { type: "invoked", result: 2, state: { count: 2 } })
    assert.deepEqual(await queued, { type: "invoked", result: 5, state: { count: 5 } })
}

async function createTypeScriptConsumer(): Promise<string> {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-worker-"))
    const source = path.join(root, "src")
    await mkdir(source)
    const compiledSdkRoot = fileURLToPath(new URL("../../", import.meta.url))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(source, "durable-objects.ts"),
        `import { Actor } from ${JSON.stringify(pathToFileURL(path.join(compiledSdkRoot, "index.js")).href)}

export class SessionCounter extends Actor {
    count = 0

    async increment(amount = 1): Promise<number> {
        this.count += amount
        return this.count
    }

    async incrementAfter(delayMs: number, amount = 1): Promise<number> {
        await new Promise(resolve => setTimeout(resolve, delayMs))
        return this.increment(amount)
    }
}
`
    )
    return root
}
