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

test("terminates an uncooperative actor Worker and recreates it from durable state", async () => {
    const consumerRoot = await createTypeScriptConsumer()
    const entrypoint = pathToFileURL(path.join(consumerRoot, "src/durable-objects.ts")).href
    try {
        await exerciseHardCancellation(entrypoint)
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

async function exerciseHardCancellation(entrypoint: string): Promise<void> {
    assert.deepEqual(await loadActorEntrypoint(entrypoint), ["SessionCounter"])
    const runtime = new ActorWorkerSupervisor({
        cancellationGraceMs: 20,
        actorEntrypointUrl: entrypoint
    })

    const invocation = runtime.handle({
        type: "invoke",
        request_id: "spin-request",
        actor: actorIdentity,
        method: "spinForever",
        args: [],
        state: null,
        timeout_ms: 30_000
    })
    await Promise.resolve()

    const queued = runtime.handle({
        type: "invoke",
        request_id: "parallel-request",
        actor: actorIdentity,
        method: "increment",
        args: [2],
        state: { count: 4 },
        timeout_ms: 30_000
    })

    assert.deepEqual(
        await runtime.handle({
            type: "cancel",
            request_id: "spin-request",
            actor: actorIdentity
        }),
        { type: "cancelled" }
    )
    assert.deepEqual(await invocation, {
        type: "failed",
        code: "actor_worker_terminated",
        message: "actor invocation did not terminate within 20ms of cancellation"
    })
    assert.deepEqual(await queued, { type: "invoked", result: 6, state: { count: 6 } })
    const recovered = await runtime.handle({
        type: "invoke",
        request_id: "recovered-request",
        actor: actorIdentity,
        method: "increment",
        args: [2],
        state: { count: 0 },
        timeout_ms: 30_000
    })
    assert.deepEqual(recovered, { type: "invoked", result: 8, state: { count: 8 } })
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

    async spinForever(): Promise<never> {
        while (true) {}
    }

    async announceThenSpin(): Promise<never> {
        await SessionCounter.get("worker-start-observer").increment()
        return this.spinForever()
    }
}
`
    )
    return root
}
