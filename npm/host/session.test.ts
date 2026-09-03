import assert from "node:assert/strict"
import { once } from "node:events"
import { unlink } from "node:fs/promises"
import { createServer } from "node:net"
import type { Socket } from "node:net"
import { createInterface } from "node:readline"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

test("the actor session carries only owned execution commands", async () => {
    const socketPath = `/tmp/ta-session-${process.pid}.sock`
    await removeSocket(socketPath)
    const server = createServer()
    const customerSocketPromise = new Promise<Socket>(resolve => {
        server.once("connection", resolve)
    })
    const { ActorSession, ActorSessionSettings } = await import("./session.js")
    server.listen(socketPath)
    await once(server, "listening")
    const session = new ActorSession(
        ActorSessionSettings.fromEnvironment({
            DURABLE_OBJECT_EXECUTOR_SOCKET: socketPath,
            DURABLE_OBJECT_ENTRYPOINT: fileURLToPath(new URL("../fixtures/actorSession.js", import.meta.url))
        })
    )
    const startup = session.start()

    try {
        const customerSocket = await customerSocketPromise
        const lines = createInterface({ input: customerSocket, crlfDelay: Infinity })
        const iterator = lines[Symbol.asyncIterator]()

        assert.deepEqual(await readMessage(iterator), {
            type: "attach",
            protocol: 11,
            actor_types: ["SessionCounter"]
        })
        customerSocket.write(`${JSON.stringify({ type: "attached", protocol: 11 })}\n`)
        await startup

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 1,
                command: {
                    type: "invoke",
                    request_id: "request-1",
                    actor: {
                        namespace_id: "namespace-1",
                        actor_type: "SessionCounter",
                        actor_id: "counter-1"
                    },
                    method: "increment",
                    args: [4],
                    state: null
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 1,
            reply: { type: "invoked", result: 4, state: { count: 4 } }
        })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 2,
                command: {
                    type: "invoke",
                    request_id: "request-2",
                    actor: actorIdentity(),
                    method: "sizedResponse",
                    args: [16 * 1024 * 1024],
                    state: { count: 4 }
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 2,
            reply: { type: "failed", code: "resource_exhausted", message: "actor session response exceeds 16777216 bytes" }
        })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 3,
                command: {
                    type: "invoke",
                    request_id: "request-3",
                    actor: actorIdentity(),
                    method: "increment",
                    args: [1],
                    state: { count: 4 }
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 3,
            reply: { type: "invoked", result: 5, state: { count: 5 } }
        })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 4,
                command: {
                    type: "evict",
                    actor: actorIdentity()
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 4,
            reply: { type: "evicted" }
        })

        customerSocket.end()
        await session.waitUntilDisconnected()
        lines.close()
        server.close()
        await once(server, "close")
    } finally {
        if (server.listening) server.close()
        await removeSocket(socketPath)
    }
})

function actorIdentity(): Record<string, string> {
    return {
        namespace_id: "namespace-1",
        actor_type: "SessionCounter",
        actor_id: "counter-1"
    }
}

async function readMessage(iterator: AsyncIterator<string>): Promise<unknown> {
    const next = await iterator.next()
    assert.equal(next.done, false)
    const message: unknown = JSON.parse(next.value ?? "")
    return message
}

async function removeSocket(socketPath: string): Promise<void> {
    try {
        await unlink(socketPath)
    } catch (error) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error
    }
}
