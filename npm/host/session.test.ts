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
    const previousSocket = process.env.DURABLE_OBJECT_EXECUTOR_SOCKET
    const previousEntrypoint = process.env.DURABLE_OBJECT_ENTRYPOINT
    await removeSocket(socketPath)
    const server = createServer()
    const customerSocketPromise = new Promise<Socket>(resolve => {
        server.once("connection", resolve)
    })
    process.env.DURABLE_OBJECT_EXECUTOR_SOCKET = socketPath
    process.env.DURABLE_OBJECT_ENTRYPOINT = fileURLToPath(new URL("../fixtures/actorSession.js", import.meta.url))

    const { ActorSession, ActorSessionSettings } = await import("./session.js")
    server.listen(socketPath)
    await once(server, "listening")
    const session = new ActorSession({ cancellationGraceMs: 20 })
    const startup = session.start()

    try {
        const customerSocket = await customerSocketPromise
        const lines = createInterface({ input: customerSocket, crlfDelay: Infinity })
        const iterator = lines[Symbol.asyncIterator]()

        assert.deepEqual(await readMessage(iterator), {
            type: "attach",
            protocol: 8,
            actor_types: ["SessionCounter"]
        })
        customerSocket.write(`${JSON.stringify({ type: "attached", protocol: 8 })}\n`)
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
                    state: null,
                    timeout_ms: 30_000
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
                    request_id: "spin-request",
                    actor: actorIdentity(),
                    method: "spinForever",
                    args: [],
                    state: { count: 4 },
                    timeout_ms: 30_000
                }
            })}\n`
        )
        await new Promise(resolve => setTimeout(resolve, 75))
        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 3,
                command: {
                    type: "cancel",
                    request_id: "spin-request",
                    actor: actorIdentity()
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 3,
            reply: { type: "cancelled" }
        })
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 2,
            reply: {
                type: "failed",
                code: "actor_worker_terminated",
                message: "actor invocation did not terminate within 20ms of cancellation"
            }
        })

        customerSocket.end()
        await session.waitUntilDisconnected()
        lines.close()
        server.close()
        await once(server, "close")
    } finally {
        ActorSessionSettings.resetForTests()
        if (previousSocket === undefined) delete process.env.DURABLE_OBJECT_EXECUTOR_SOCKET
        else process.env.DURABLE_OBJECT_EXECUTOR_SOCKET = previousSocket
        if (previousEntrypoint === undefined) delete process.env.DURABLE_OBJECT_ENTRYPOINT
        else process.env.DURABLE_OBJECT_ENTRYPOINT = previousEntrypoint
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
