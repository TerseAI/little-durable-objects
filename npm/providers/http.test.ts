import assert from "node:assert/strict"
import type { AddressInfo } from "node:net"
import { test } from "node:test"

import { startSandboxProviderServer } from "./http.js"
import type { EnsureHostRequest, SandboxProvider } from "./types.js"

const request: EnsureHostRequest = {
    namespaceId: "namespace-1",
    principalId: "workflow.v1.namespace-1.00000000-0000-4000-8000-000000000001",
    credentialId: "credential-1",
    codeRevision: "revision-1",
    canonicalRegion: "north-america-east"
}

test("provider HTTP API authenticates and exposes cache lifecycle", async () => {
    const calls: string[] = []
    const provider: SandboxProvider = {
        async ensureHost(value) {
            assert.deepEqual(value, request)
            calls.push("ensure")
            return {
                hostId: "host.v1.namespace-1.00000000-0000-4000-8000-000000000002",
                route: "https://host.example.com",
                canonicalRegion: value.canonicalRegion,
                cacheSource: "volume"
            }
        },
        async status() {
            calls.push("status")
            return "warm"
        },
        async deactivate() {
            calls.push("deactivate")
        },
        async removeLocalCache() {
            calls.push("remove")
        }
    }
    const server = await startSandboxProviderServer({ provider, token: "secret", port: 0 })
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
    const post = (path: string, token = "secret") =>
        fetch(`${origin}${path}`, {
            method: "POST",
            headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
            body: JSON.stringify(request)
        })

    try {
        assert.equal((await post("/hosts/ensure", "wrong")).status, 401)
        const ensured = await post("/hosts/ensure")
        assert.equal(ensured.status, 200)
        assert.equal(((await ensured.json()) as { cacheSource: string }).cacheSource, "volume")
        const status = await post("/hosts/status")
        assert.deepEqual(await status.json(), { status: "warm" })
        assert.equal((await post("/hosts/deactivate")).status, 204)
        assert.equal((await post("/hosts/remove-local-cache")).status, 204)
        assert.deepEqual(calls, ["ensure", "status", "deactivate", "remove"])
    } finally {
        await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
    }
})
