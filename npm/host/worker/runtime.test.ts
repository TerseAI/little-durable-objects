import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../shared/actor.js"
import type { ActorConnection, ActorSocket } from "../../shared/socket.js"
import { runWithActorClientForTests } from "../../workflow/client.js"

import { ActorRuntime } from "./runtime.js"

export class Counter extends Actor {
    private count = 0

    async increment(amount = 1): Promise<number> {
        this.count += amount
        return this.count
    }

    async getCount(): Promise<number> {
        return this.count
    }

    async getIdentity(): Promise<string> {
        return this.id
    }

    async explode(): Promise<never> {
        this.count = 999
        throw new CounterExplosionError()
    }
}

export class Forwarder extends Actor {
    async incrementCounter(): Promise<number> {
        return Counter.get("counter-1").increment()
    }
}

interface ChatSession {
    readonly userId: string
    readonly connectedAt: number
}

export class ChatRoom extends Actor {
    private events: string[] = []

    async onConnect(socket: ActorSocket<ChatSession>): Promise<void> {
        this.events.push(`connect:${socket.metadata.userId}:${this.connections.length}`)
        socket.metadata = { ...socket.metadata, connectedAt: 2 }
        socket.setTags("member")
        socket.send("ready")
    }

    async onMessage(socket: ActorSocket<ChatSession>, message: string | Uint8Array): Promise<void> {
        this.events.push(`message:${socket.metadata.userId}:${typeof message === "string" ? message : message.byteLength}`)
        this.broadcast(message)
    }

    async onDisconnect(socket: ActorSocket<ChatSession>, code: number, reason: string): Promise<void> {
        this.events.push(`disconnect:${socket.metadata.userId}:${code}:${reason}:${this.connections.length}`)
    }

    async getEvents(): Promise<readonly string[]> {
        return this.events
    }

    async announce(message: string): Promise<void> {
        this.broadcast(message)
    }
}

export class RejectingRoom extends Actor {
    async onConnect(socket: ActorSocket): Promise<void> {
        socket.reject(3000, "closed")
    }
}

const actorIdentity = {
    namespace_id: "namespace-1",
    actor_type: "Counter",
    actor_id: "counter-1"
}

const forwarderIdentity = {
    ...actorIdentity,
    actor_type: "Forwarder",
    actor_id: "forwarder-1"
}

const counterDefinition = registerActorClass(Counter)
const forwarderDefinition = registerActorClass(Forwarder)
const chatDefinition = registerActorClass(ChatRoom)
const rejectingDefinition = registerActorClass(RejectingRoom)

test("keeps a successful actor instance resident and restores it after failure", async () => {
    const runtime = new ActorRuntime(counterDefinition)
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
            state: { count: 2 }
        }),
        { type: "invoked", result: 5, state: { count: 5 } }
    )
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-3",
            actor: actorIdentity,
            method: "getCount",
            args: [],
            state: { count: 2 }
        }),
        { type: "invoked", result: 5, state: { count: 5 } }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-4",
            actor: actorIdentity,
            method: "getIdentity",
            args: [],
            state: { count: 2 }
        }),
        { type: "invoked", result: "counter-1", state: { count: 5 } }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-5",
            actor: actorIdentity,
            method: "explode",
            args: [],
            state: { count: 2 }
        }),
        { type: "failed", code: "actor_method_failed", message: "boom" }
    )
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-6",
            actor: actorIdentity,
            method: "getCount",
            args: [],
            state: { count: 2 }
        }),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
})

test("Actor.get returns a typed forwarding reference", async () => {
    const calls: unknown[] = []
    await runWithActorClientForTests(
        {
            requestId: () => "fixed-request",
            invoke: async request => {
                calls.push(request)
                return 7
            }
        },
        async () => {
            const counter = Counter.get("counter-1")

            assert.equal(Reflect.get(counter, "missingMethod"), undefined)
            assert.equal(await counter.increment(3), 7)
            assert.deepEqual(calls, [
                {
                    requestId: "fixed-request",
                    actorType: "Counter",
                    actorId: "counter-1",
                    method: "increment",
                    args: [3]
                }
            ])
        }
    )
})

test("Actor.get connects with typed metadata", async () => {
    const calls: unknown[] = []
    const connection: ActorConnection = {
        readyState: 1,
        send: () => undefined,
        close: () => undefined,
        addEventListener: () => undefined,
        removeEventListener: () => undefined
    }
    await runWithActorClientForTests(
        {
            requestId: () => "fixed-request",
            invoke: async () => undefined,
            connect: async request => {
                calls.push(request)
                return connection
            }
        },
        async () => {
            assert.equal(await ChatRoom.get("room-1").connect({ userId: "user-1", connectedAt: 1 }), connection)
        }
    )
    assert.deepEqual(calls, [
        {
            requestId: "fixed-request",
            actorType: "ChatRoom",
            actorId: "room-1",
            metadata: { userId: "user-1", connectedAt: 1 }
        }
    ])
})

test("sends durable actor properties when a connection has no onConnect hook", async () => {
    const runtime = new ActorRuntime(counterDefinition)
    const connection = { id: "connection-1", metadata: {}, tags: [] }

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-state",
            actor: actorIdentity,
            event: { type: "connect", connection },
            connections: [connection],
            state: null
        }),
        {
            type: "websocket_handled",
            state: { count: 0 },
            effects: [
                {
                    type: "send",
                    connection_id: "connection-1",
                    message: { type: "text", data: '{"type":"state","state":{"count":0}}' }
                }
            ]
        }
    )
})

test("does not expose actor properties to a rejected connection", async () => {
    const runtime = new ActorRuntime(rejectingDefinition)
    const actor = { ...actorIdentity, actor_type: "RejectingRoom", actor_id: "room-1" }
    const connection = { id: "connection-1", metadata: {}, tags: [] }

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-reject",
            actor,
            event: { type: "connect", connection },
            connections: [connection],
            state: null
        }),
        {
            type: "websocket_handled",
            state: {},
            effects: [{ type: "reject", connection_id: "connection-1", code: 3000, reason: "closed" }]
        }
    )
})

test("runs the full socket lifecycle and exposes live actor connections", async () => {
    const runtime = new ActorRuntime(chatDefinition)
    const actor = { ...actorIdentity, actor_type: "ChatRoom", actor_id: "room-1" }
    const connection = { id: "connection-1", metadata: { userId: "user-1", connectedAt: 1 }, tags: [] }

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-1",
            actor,
            event: { type: "connect", connection },
            connections: [connection],
            state: null
        }),
        {
            type: "websocket_handled",
            state: { events: ["connect:user-1:1"] },
            effects: [
                { type: "set_metadata", connection_id: "connection-1", metadata: { userId: "user-1", connectedAt: 2 } },
                { type: "set_tags", connection_id: "connection-1", tags: ["member"] },
                { type: "send", connection_id: "connection-1", message: { type: "text", data: "ready" } },
                {
                    type: "send",
                    connection_id: "connection-1",
                    message: { type: "text", data: '{"type":"state","state":{"events":["connect:user-1:1"]}}' }
                }
            ]
        }
    )

    const connected = { ...connection, metadata: { userId: "user-1", connectedAt: 2 }, tags: ["member"] }
    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-2",
            actor,
            event: { type: "message", connection_id: "connection-1", message: { type: "text", data: "hello" } },
            connections: [connected],
            state: { events: ["connect:user-1:1"] }
        }),
        {
            type: "websocket_handled",
            state: { events: ["connect:user-1:1", "message:user-1:hello"] },
            effects: [{ type: "broadcast", message: { type: "text", data: "hello" }, except_connection_ids: [], tags: [] }]
        }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-announce",
            actor,
            method: "announce",
            args: ["announcement"],
            connections: [],
            state: { events: ["connect:user-1:1", "message:user-1:hello"] }
        }),
        {
            type: "invoked",
            result: null,
            state: { events: ["connect:user-1:1", "message:user-1:hello"] },
            effects: [{ type: "broadcast", message: { type: "text", data: "announcement" }, except_connection_ids: [], tags: [] }]
        }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-3",
            actor,
            event: { type: "disconnect", connection: connected, code: 1000, reason: "done", was_clean: true },
            connections: [],
            state: { events: ["connect:user-1:1", "message:user-1:hello"] }
        }),
        {
            type: "websocket_handled",
            state: { events: ["connect:user-1:1", "message:user-1:hello", "disconnect:user-1:1000:done:0"] },
            effects: []
        }
    )
})

test("the injected invoker remains available inside actor unit tests", async () => {
    await runWithActorClientForTests(
        {
            requestId: () => "nested-request",
            invoke: async request => {
                throw new Error(`unexpected external nested invocation ${request.requestId}`)
            }
        },
        async () => {
            const runtime = new ActorRuntime(forwarderDefinition)
            assert.deepEqual(
                await runtime.handle({
                    type: "invoke",
                    request_id: "parent-request",
                    actor: forwarderIdentity,
                    method: "incrementCounter",
                    args: [],
                    state: null
                }),
                {
                    type: "failed",
                    code: "actor_method_failed",
                    message: "unexpected external nested invocation nested-request"
                }
            )
        }
    )
})

class CounterExplosionError extends Error {
    constructor() {
        super("boom")
        this.name = "CounterExplosionError"
    }
}
