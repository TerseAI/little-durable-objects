import assert from "node:assert/strict"
import { test } from "node:test"

import { runWithActorClientForTests } from "../workflow/client.js"

import { Actor } from "./actor.js"

class ChatRoom extends Actor {
    async history(): Promise<readonly string[]> {
        return []
    }
}

test("actor references broadcast without invoking a customer actor method", async () => {
    const broadcasts: unknown[] = []
    await runWithActorClientForTests(
        {
            invoke: async () => assert.fail("broadcast invoked a customer actor method"),
            broadcast: async request => {
                broadcasts.push(request)
            },
            requestId: () => "request-1"
        },
        () => ChatRoom.get("room-1").broadcast("hello")
    )
    assert.deepEqual(broadcasts, [
        {
            requestId: "request-1",
            actorType: "ChatRoom",
            actorId: "room-1",
            message: { type: "text", data: "hello" }
        }
    ])
})
