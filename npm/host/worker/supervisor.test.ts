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

test("starts one speculative Worker and gives it to the first actor", async () => {
    const consumerRoot = await createTypeScriptConsumer("PreloadedCounter")
    const entrypoint = pathToFileURL(path.join(consumerRoot, "src/durable-objects.ts")).href
    const created: number[] = []
    try {
        await loadActorEntrypoint(entrypoint)
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            createWorker: () => {
                created.push(created.length + 1)
                return {
                    async ready() {
                        return ["PreloadedCounter"]
                    },
                    async execute() {
                        return { type: "invoked", result: null, state: {} }
                    },
                    terminate() {}
                }
            }
        })

        assert.equal(created.length, 1)
        await runtime.handle(invokeCommand("counter-1", "PreloadedCounter"))
        assert.equal(created.length, 1)
        await runtime.handle(invokeCommand("counter-2", "PreloadedCounter"))
        assert.equal(created.length, 2)
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

test("expires an unused speculative Worker without replenishing it", async () => {
    let created = 0
    let terminated = 0
    let finish: (() => void) | undefined
    const expired = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("preload did not expire")), 1_000)
        finish = () => {
            clearTimeout(timeout)
            resolve()
        }
    })
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.ts",
        actorIdleTimeoutMs: 50,
        createWorker: () => {
            created += 1
            return {
                ready: () => new Promise(() => undefined),
                async execute() {
                    return { type: "invoked", result: null, state: {} }
                },
                terminate() {
                    terminated += 1
                    finish?.()
                }
            }
        }
    })
    await expired
    assert.equal(terminated, 1)
    await new Promise(resolve => setTimeout(resolve, 100))
    assert.equal(created, 1)
    supervisor.close()
})

test("eviction during Worker startup settles the invocation and allows recovery", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer("CancelledCounter")
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    const command = invokeCommand("counter-1", "CancelledCounter")
    try {
        await loadActorEntrypoint(entrypoint)
        const runtime = new ActorWorkerSupervisor({ actorEntrypointUrl: entrypoint })
        await runtime.ready()
        const pending = runtime.handle(command)
        await runtime.handle({ type: "evict", actor: command.actor })
        const reply = await pending
        assert.equal(reply.type, "failed")
        if (reply.type === "failed") assert.equal(reply.code, "actor_worker_terminated")
        assert.deepEqual(await runtime.handle({ ...command, state: { count: 9 } }), { type: "invoked", result: 10, state: { count: 10 } })
        await runtime.handle({ type: "evict", actor: command.actor })
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("discards a failed preload before accepting the first actor", async () => {
    const root = await createTypeScriptConsumer("RetryPreloadCounter")
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    let created = 0
    let terminated = 0
    try {
        await loadActorEntrypoint(entrypoint)
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            createWorker: () => {
                const failed = created++ === 0
                return {
                    ready: () => (failed ? Promise.reject(new Error("preload failed")) : Promise.resolve(["RetryPreloadCounter"])),
                    async execute() {
                        if (failed) throw new Error("preload failed")
                        return { type: "invoked", result: 1, state: { count: 1 } }
                    },
                    terminate() {
                        terminated += 1
                    }
                }
            }
        })
        await new Promise(resolve => setImmediate(resolve))
        assert.equal(terminated, 1)
        assert.deepEqual(await runtime.handle(invokeCommand("counter-1", "RetryPreloadCounter")), { type: "invoked", result: 1, state: { count: 1 } })
        assert.equal(created, 2)
        runtime.close()
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("closing the supervisor terminates an unused Worker and rejects new work", async () => {
    let terminated = 0
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.ts",
        createWorker: () => ({
            async ready() {
                return ["UnusedCounter"]
            },
            async execute() {
                throw new Error("closed supervisor must not execute")
            },
            terminate() {
                terminated += 1
            }
        })
    })
    runtime.close()
    runtime.close()
    assert.equal(terminated, 1)
    const reply = await runtime.handle(invokeCommand("counter-1", "UnusedCounter"))
    assert.equal(reply.type, "failed")
    if (reply.type === "failed") assert.equal(reply.code, "actor_worker_terminated")
})

test("an actor module that fails inside a Worker returns a failure without hanging", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer("FailedImportCounter", 'import { isMainThread } from "node:worker_threads"\nif (!isMainThread) throw new Error("worker import failed")')
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    try {
        await loadActorEntrypoint(entrypoint)
        const runtime = new ActorWorkerSupervisor({ actorEntrypointUrl: entrypoint })
        try {
            const reply = await runtime.handle(invokeCommand("counter-1", "FailedImportCounter"))
            assert.equal(reply.type, "failed")
            if (reply.type === "failed") assert.match(reply.message, /worker import failed/)
        } finally {
            runtime.close()
        }
    } finally {
        await rm(root, { recursive: true, force: true })
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

async function createTypeScriptConsumer(actorType = "SessionCounter", preamble = ""): Promise<string> {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-worker-"))
    const source = path.join(root, "src")
    await mkdir(source)
    const compiledSdkRoot = fileURLToPath(new URL("../../", import.meta.url))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(source, "durable-objects.ts"),
        `import { Actor } from ${JSON.stringify(pathToFileURL(path.join(compiledSdkRoot, "index.js")).href)}
${preamble}

export class ${actorType} extends Actor {
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

function invokeCommand(actorId: string, actorType: string) {
    return {
        type: "invoke" as const,
        request_id: `request-${actorId}`,
        actor: { ...actorIdentity, actor_type: actorType, actor_id: actorId },
        method: "increment",
        args: [],
        state: null
    }
}
