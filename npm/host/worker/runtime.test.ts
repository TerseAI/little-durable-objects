import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../shared/actor.js"
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
