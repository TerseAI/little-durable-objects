import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorConfigurationError } from "../shared/errors.js"

import { ActorSessionSettings } from "./session.js"

test("a managed socket needs no local actor credentials", () => {
    const settings = ActorSessionSettings.fromEnvironment({
        DURABLE_OBJECT_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
        DURABLE_OBJECT_ENTRYPOINT: "src/custom-actors.ts"
    })

    assert.equal(settings.socketPath, "/tmp/durable-object.sock")
    assert.equal(settings.actorEntrypoint, "src/custom-actors.ts")
    assert.equal(settings.startupTimeoutMs, 10_000)
})

test("actor-host startup timeout is configurable and bounded", () => {
    assert.equal(
        ActorSessionSettings.fromEnvironment({
            DURABLE_OBJECT_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
            DURABLE_OBJECT_HOST_STARTUP_MS: "2500"
        }).startupTimeoutMs,
        2_500
    )
    assert.throws(
        () =>
            ActorSessionSettings.fromEnvironment({
                DURABLE_OBJECT_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
                DURABLE_OBJECT_HOST_STARTUP_MS: "0"
            }),
        ActorConfigurationError
    )
})

test("actor-host settings require a private session socket", () => {
    assert.throws(() => ActorSessionSettings.fromEnvironment({}), ActorConfigurationError)
})
