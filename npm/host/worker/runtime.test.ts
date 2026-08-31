import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../shared/actor.js"
import { configureActorClientForTests } from "../../workflow/client.js"

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

    async waitForCancellation(): Promise<never> {
        return new Promise((_, reject) => {
            if (this.signal.aborted) {
                reject(this.signal.reason)
                return
            }
            this.signal.addEventListener("abort", () => reject(this.signal.reason), { once: true })
        })
    }
}

export class Forwarder extends Actor {
    async incrementCounter(): Promise<number> {
        return Counter.get("counter-1", { timeoutMs: 10_000 }).increment()
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

test("keeps a successful actor instance resident and restores it after failure", async () => {
    const runtime = new ActorRuntime(counterDefinition)
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-1",
            actor: actorIdentity,
            method: "increment",
            args: [2],
            state: null,
            timeout_ms: 30_000
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
            state: { count: 2 },
            timeout_ms: 30_000
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
            state: { count: 2 },
            timeout_ms: 30_000
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
            state: { count: 2 },
            timeout_ms: 30_000
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
            state: { count: 2 },
            timeout_ms: 30_000
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
            state: { count: 2 },
            timeout_ms: 30_000
        }),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
})

test("Actor.get returns a typed forwarding reference", async () => {
    const calls: unknown[] = []
    configureActorClientForTests({
        requestId: () => "fixed-request",
        invoke: async request => {
            calls.push(request)
            return 7
        }
    })
    try {
        const counter = Counter.get("counter-1", { timeoutMs: 1_234 })

        assert.equal(Reflect.get(counter, "missingMethod"), undefined)
        assert.equal(await counter.increment(3), 7)
        assert.deepEqual(calls, [
            {
                requestId: "fixed-request",
                actorType: "Counter",
                actorId: "counter-1",
                method: "increment",
                args: [3],
                timeoutMs: 1_234
            }
        ])
    } finally {
        configureActorClientForTests(undefined)
    }
})

test("Actor.get enforces its caller deadline", async () => {
    configureActorClientForTests({
        requestId: () => "deadline-request",
        invoke: async () => new Promise<never>(() => undefined)
    })
    try {
        await assert.rejects(Counter.get("counter-1", { timeoutMs: 10 }).increment(), error => {
            assert.equal(Reflect.get(error as object, "name"), "ActorInvocationError")
            assert.equal(Reflect.get(error as object, "code"), "deadline_exceeded")
            assert.equal(Reflect.get(error as object, "requestId"), "deadline-request")
            return true
        })
    } finally {
        configureActorClientForTests(undefined)
    }
})

test("the injected invoker remains available inside actor unit tests", async () => {
    configureActorClientForTests({
        requestId: () => "nested-request",
        invoke: async request => {
            throw new Error(`unexpected external nested invocation ${request.requestId}`)
        }
    })
    try {
        const runtime = new ActorRuntime(forwarderDefinition)
        assert.deepEqual(
            await runtime.handle({
                type: "invoke",
                request_id: "parent-request",
                actor: forwarderIdentity,
                method: "incrementCounter",
                args: [],
                state: null,
                timeout_ms: 500
            }),
            {
                type: "failed",
                code: "actor_method_failed",
                message: "unexpected external nested invocation nested-request"
            }
        )
    } finally {
        configureActorClientForTests(undefined)
    }
})

test("running actor methods receive cooperative cancellation through AbortSignal", async () => {
    const runtime = new ActorRuntime(counterDefinition)
    const invocation = runtime.handle({
        type: "invoke",
        request_id: "cancel-request",
        actor: actorIdentity,
        method: "waitForCancellation",
        args: [],
        state: { count: 2 },
        timeout_ms: 30_000
    })
    await Promise.resolve()

    assert.deepEqual(
        await runtime.handle({
            type: "cancel",
            request_id: "cancel-request",
            actor: actorIdentity
        }),
        { type: "cancelled" }
    )
    assert.deepEqual(await invocation, {
        type: "failed",
        code: "actor_method_failed",
        message: "actor invocation cancelled after its deadline expired"
    })
})

class CounterExplosionError extends Error {
    constructor() {
        super("boom")
        this.name = "CounterExplosionError"
    }
}
