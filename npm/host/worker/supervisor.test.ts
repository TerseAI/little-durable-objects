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

test("keeps an actor resident until Rust explicitly evicts it", async () => {
    const consumerRoot = await createTypeScriptConsumer()
    const entrypoint = pathToFileURL(path.join(consumerRoot, "src/durable-objects.ts")).href
    try {
        await exerciseResidency(entrypoint)
        await exerciseIdleRecycling(entrypoint)
        await exerciseSocketHibernation(entrypoint)
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

async function exerciseResidency(entrypoint: string): Promise<void> {
    assert.deepEqual(await loadActorEntrypoint(entrypoint), ["SessionCounter"])
    const runtime = new ActorWorkerSupervisor({ actorEntrypointUrl: entrypoint })

    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-1",
            actor: actorIdentity,
            method: "increment",
            args: [2],
            state: null
        }),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-2",
            actor: actorIdentity,
            method: "increment",
            args: [3],
            state: { count: 0 }
        }),
        { type: "invoked", result: 5, state: { count: 5 } }
    )
    assert.deepEqual(await runtime.handle({ type: "evict", actor: actorIdentity }), { type: "evicted" })
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-3",
            actor: actorIdentity,
            method: "getCount",
            args: [],
            state: { count: 2 }
        }),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
}

async function exerciseSocketHibernation(entrypoint: string): Promise<void> {
    const runtime = new ActorWorkerSupervisor({ actorEntrypointUrl: entrypoint, actorIdleTimeoutMs: 10 })
    const connection = { id: "socket-1", metadata: { userId: "user-1" }, tags: [] }
    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "socket-request-1",
            actor: actorIdentity,
            event: { type: "connect", connection },
            connections: [connection],
            state: null
        }),
        {
            type: "websocket_handled",
            state: { count: 1 },
            effects: [
                {
                    type: "send",
                    connection_id: "socket-1",
                    message: { type: "text", data: '{"type":"state","state":{"count":1}}' }
                }
            ]
        }
    )
    await new Promise(resolve => setTimeout(resolve, 30))
    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "socket-request-2",
            actor: actorIdentity,
            event: { type: "message", connection_id: "socket-1", message: { type: "text", data: "hello" } },
            connections: [connection],
            state: { count: 1 }
        }),
        {
            type: "websocket_handled",
            state: { count: 2 },
            effects: [{ type: "send", connection_id: "socket-1", message: { type: "text", data: "user-1:hello" } }]
        }
    )
}

async function exerciseIdleRecycling(entrypoint: string): Promise<void> {
    const runtime = new ActorWorkerSupervisor({ actorEntrypointUrl: entrypoint, actorIdleTimeoutMs: 10 })
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "idle-request-1",
            actor: actorIdentity,
            method: "increment",
            args: [2],
            state: null
        }),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
    await new Promise(resolve => setTimeout(resolve, 30))
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "idle-request-2",
            actor: actorIdentity,
            method: "getCount",
            args: [],
            state: { count: 9 }
        }),
        { type: "invoked", result: 9, state: { count: 9 } }
    )
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

    async getCount(): Promise<number> {
        return this.count
    }

    async onConnect(): Promise<void> {
        this.count += 1
    }

    async onMessage(socket: { metadata: { userId: string }, send(message: string): void }, message: string): Promise<void> {
        this.count += 1
        socket.send(\`${"${socket.metadata.userId}"}:${"${message}"}\`)
    }
}
`
    )
    return root
}
