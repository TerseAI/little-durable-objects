import assert from "node:assert/strict"
import { PassThrough, Readable } from "node:stream"
import { test } from "node:test"

import { runProviderCommands } from "./commandProcess.js"
import type { SandboxProvider } from "./types.js"

test("a persistent provider loads its SDK once and isolates command failures", async () => {
    let loads = 0
    let calls = 0
    const output = new PassThrough()
    let responses = ""
    output.on("data", chunk => {
        responses += String(chunk)
    })
    const input = Readable.from([`${JSON.stringify({ operation: "warm_image", request: {} })}\n`.repeat(2)])
    await runProviderCommands(
        input,
        output,
        async () => {
            loads += 1
            return {
                async warmImage() {
                    if (++calls === 1) throw new Error("test failure")
                    return { provider: "modal", resourceId: "sb-2", totalMs: 0 }
                }
            } as unknown as SandboxProvider
        },
        true
    )
    assert.equal(loads, 1)
    assert.deepEqual(
        responses
            .trim()
            .split("\n")
            .map(line => JSON.parse(line)),
        [
            { status: "failure", error: "test failure" },
            { status: "success", result: { provider: "modal", resourceId: "sb-2", totalMs: 0 } }
        ]
    )
})

test("provider command input is bounded before JSON decoding", async () => {
    const output = new PassThrough()
    await assert.rejects(
        runProviderCommands(
            Readable.from(["x".repeat(1024 * 1024 + 1)]),
            output,
            async () => {
                throw new Error("must not load")
            },
            true
        ),
        /command exceeds/
    )
})
