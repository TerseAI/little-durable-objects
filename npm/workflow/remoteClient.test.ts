import assert from "node:assert/strict"
import { createServer } from "node:http"
import type { Server, ServerResponse } from "node:http"
import { test } from "node:test"

import { ActorInvocationError } from "../shared/errors.js"

import { RemoteActorClient } from "./remoteClient.js"

test("remote actor client resolves once and invokes the actor host directly", async () => {
    let resolutions = 0
    const hostInvocations: unknown[] = []
    const server = createServer(async (request, response) => {
        resolutions += 1
        assert.equal(request.method, "POST")
        assert.equal(request.url, "/v1/namespaces/project-1/actors/Counter/counter-1/target")
        assert.equal(request.headers.authorization, "Bearer workflow-token")
        json(response, 200, {
            route: "https://actor.example.com",
            token: "direct-token",
            ownerEpoch: 3,
            stateVersion: 7,
            stateReadUrl: "https://storage.example.com/state",
            expiresAtMs: 4_000_000_000_000
        })
    })
    const port = await listen(server)
    const client = new RemoteActorClient(
        {
            token: "workflow-token",
            namespaceId: "project-1",
            controlPlaneUrl: `http://127.0.0.1:${port}`
        },
        {
            requestId: () => "00000000-0000-4000-8000-000000000000",
            actorHost: {
                async invoke(target, invocation) {
                    hostInvocations.push({ target, invocation })
                    return { type: "completed", result: 7 }
                }
            }
        }
    )
    try {
        assert.equal(await client.invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(await client.invoke("Counter", "counter-1", "increment", [3]), 7)
        assert.equal(resolutions, 1)
        assert.equal(hostInvocations.length, 2)
        assert.deepEqual(hostInvocations[0], {
            target: {
                route: "https://actor.example.com",
                token: "direct-token",
                ownerEpoch: 3,
                stateVersion: 7,
                stateReadUrl: "https://storage.example.com/state",
                expiresAtMs: 4_000_000_000_000
            },
            invocation: {
                requestId: "00000000-0000-4000-8000-000000000000",
                namespaceId: "project-1",
                actorType: "Counter",
                actorId: "counter-1",
                method: "increment",
                args: [2]
            }
        })
    } finally {
        await close(server)
    }
})

test("does not retry a control-plane transport failure", async () => {
    let calls = 0
    const server = createServer(request => {
        calls += 1
        request.socket.destroy()
    })
    const port = await listen(server)
    const client = new RemoteActorClient({ token: "workflow-token", namespaceId: "project-1", controlPlaneUrl: `http://127.0.0.1:${port}` })
    try {
        await assert.rejects(client.invoke("Counter", "counter-1", "increment", [2]), error => error instanceof ActorInvocationError && error.code === "outcome_unknown")
        assert.equal(calls, 1)
    } finally {
        await close(server)
    }
})

test("requires the direct actor target endpoint", async () => {
    const calls: string[] = []
    const server = createServer((request, response) => {
        calls.push(request.url ?? "")
        json(response, 404, { error: { code: "not_found", message: "not found" } })
    })
    const port = await listen(server)
    const client = new RemoteActorClient({ token: "workflow-token", namespaceId: "project-1", controlPlaneUrl: `http://127.0.0.1:${port}` })
    try {
        await assert.rejects(client.invoke("Counter", "counter-1", "increment", [2]), error => error instanceof ActorInvocationError && error.code === "not_found")
        assert.deepEqual(calls, ["/v1/namespaces/project-1/actors/Counter/counter-1/target"])
    } finally {
        await close(server)
    }
})

test("preserves a structured actor failure from HTTP", async () => {
    const server = createServer((_request, response) => {
        json(response, 422, {
            error: {
                code: "actor_error",
                message: "actor method failed",
                requestId: "server-request"
            }
        })
    })
    const port = await listen(server)
    const client = new RemoteActorClient({ token: "workflow-token", namespaceId: "project-1", controlPlaneUrl: `http://127.0.0.1:${port}` })
    try {
        await assert.rejects(
            client.invoke("Counter", "counter-1", "increment", [2]),
            error => error instanceof ActorInvocationError && error.code === "actor_error" && error.requestId === "server-request"
        )
    } finally {
        await close(server)
    }
})

function json(response: ServerResponse, status: number, body: unknown): void {
    response.writeHead(status, { "content-type": "application/json" })
    response.end(JSON.stringify(body))
}

function listen(server: Server): Promise<number> {
    return new Promise((resolve, reject) => {
        server.once("error", reject)
        server.listen(0, "127.0.0.1", () => {
            server.off("error", reject)
            const address = server.address()
            if (address === null || typeof address === "string") reject(new Error("test HTTP server has no TCP address"))
            else resolve(address.port)
        })
    })
}

function close(server: Server): Promise<void> {
    return new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
}
