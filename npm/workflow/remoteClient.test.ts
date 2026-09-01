import assert from "node:assert/strict"
import { createServer } from "node:http"
import type { IncomingMessage, Server, ServerResponse } from "node:http"
import { test } from "node:test"

import { ActorInvocationError } from "../shared/errors.js"

import { RemoteActorClient } from "./remoteClient.js"

test("remote actor client sends one authenticated HTTP invocation", async () => {
    let calls = 0
    const server = createServer(async (request, response) => {
        calls += 1
        assert.equal(request.method, "POST")
        assert.equal(request.url, "/v1/namespaces/project-1/actors/Counter/counter-1/invocations")
        assert.equal(request.headers.authorization, "Bearer workflow-token")
        const body = JSON.parse(await readBody(request)) as Record<string, unknown>
        assert.equal(body.method, "increment")
        assert.deepEqual(body.args, [2])
        assert.equal(body.timeoutMs, undefined)
        json(response, 200, { result: 7 })
    })
    const port = await listen(server)
    RemoteActorClient.configure({
        token: "workflow-token",
        namespaceId: "project-1",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        assert.equal(await RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(calls, 1)
    } finally {
        RemoteActorClient.resetForTests()
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
    RemoteActorClient.configure({ token: "workflow-token", namespaceId: "project-1", controlPlaneUrl: `http://127.0.0.1:${port}` })
    try {
        await assert.rejects(RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), error => error instanceof ActorInvocationError && error.code === "outcome_unknown")
        assert.equal(calls, 1)
    } finally {
        RemoteActorClient.resetForTests()
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
    RemoteActorClient.configure({ token: "workflow-token", namespaceId: "project-1", controlPlaneUrl: `http://127.0.0.1:${port}` })
    try {
        await assert.rejects(
            RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]),
            error => error instanceof ActorInvocationError && error.code === "actor_error" && error.requestId === "server-request"
        )
    } finally {
        RemoteActorClient.resetForTests()
        await close(server)
    }
})

function readBody(request: IncomingMessage): Promise<string> {
    return new Promise((resolve, reject) => {
        let body = ""
        request.setEncoding("utf8")
        request.on("data", chunk => {
            body += chunk
        })
        request.once("end", () => resolve(body))
        request.once("error", reject)
    })
}

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
