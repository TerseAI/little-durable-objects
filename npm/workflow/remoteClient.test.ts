import * as grpc from "@grpc/grpc-js"
import assert from "node:assert/strict"
import { createServer as createHttpServer } from "node:http"
import type { AddressInfo } from "node:net"
import { test } from "node:test"

import { RemoteActorClient, actorGrpcProtocolForTests as protocol } from "./remoteClient.js"

test("remote actor client resolves every invocation, invokes directly, and reroutes before execution", async () => {
    const requestIds: string[] = []
    const authorization: string[] = []
    let resolutions = 0
    let invocations = 0
    const grpcServer = new grpc.Server()

    grpcServer.addService(
        {
            resolveActorHost: unaryDefinition("/durable_object.v1.ActorControlPlaneService/ResolveActorHost", protocol.resolveActorHostRequestType, protocol.resolvedActorHostType)
        },
        {
            resolveActorHost(call: grpc.ServerUnaryCall<Record<string, unknown>, Record<string, unknown>>, callback: grpc.sendUnaryData<Record<string, unknown>>) {
                resolutions += 1
                authorization.push(String(call.metadata.get("authorization")[0]))
                assert.deepEqual(call.request.actor, {
                    namespaceId: "namespace-1",
                    actorType: "Counter",
                    actorId: "counter-1"
                })
                callback(null, {
                    route: grpcOrigin
                })
            }
        }
    )
    grpcServer.addService(
        {
            invoke: unaryDefinition("/durable_object.v1.ActorHostService/Invoke", protocol.invokeActorRequestType, protocol.invokeActorReplyType)
        },
        {
            invoke(call: grpc.ServerUnaryCall<Record<string, unknown>, Record<string, unknown>>, callback: grpc.sendUnaryData<Record<string, unknown>>) {
                invocations += 1
                authorization.push(String(call.metadata.get("authorization")[0]))
                const requestId = String(call.request.requestId)
                requestIds.push(requestId)
                assert.deepEqual(JSON.parse((call.request.argsJson as Buffer).toString("utf8")), [2])
                if (invocations === 1) {
                    callback(null, { reroute: {} })
                    return
                }
                callback(null, { completed: { resultJson: Buffer.from("7") } })
            }
        }
    )

    let grpcOrigin = ""
    const grpcPort = await bind(grpcServer)
    grpcOrigin = `http://127.0.0.1:${grpcPort}`
    const tokenServer = createHttpServer((request, response) => {
        assert.equal(request.method, "POST")
        assert.equal(request.url, "/credentials")
        assert.equal(request.headers.authorization, "Bearer bootstrap-credential")
        response.writeHead(200, { "content-type": "application/json" })
        response.end(
            JSON.stringify({
                namespaceId: "namespace-1",
                processId: "workflow.v1.namespace-1.00000000-0000-4000-8000-000000000099",
                sessionId: "00000000-0000-4000-8000-000000000099",
                authorityToken: "authority-token",
                invokeToken: "invoke-token",
                expiresAtMs: Date.now() + 60_000
            })
        )
    })
    await new Promise<void>(resolve => tokenServer.listen(0, "127.0.0.1", resolve))
    const tokenPort = (tokenServer.address() as AddressInfo).port

    RemoteActorClient.configure({
        credential: "bootstrap-credential",
        credentialsUrl: `http://127.0.0.1:${tokenPort}/credentials`,
        controlPlaneUrl: grpcOrigin,
        codeRevision: "revision-1",
        region: "us-east",
        invocationTimeoutMs: 5_000
    })

    try {
        assert.equal(await RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(await RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(resolutions, 3)
        assert.equal(invocations, 3)
        assert.equal(requestIds[0], requestIds[1], "a routing retry must preserve the idempotency key")
        assert.notEqual(requestIds[1], requestIds[2], "a separate invocation must use a new idempotency key")
        assert.deepEqual(authorization, ["Bearer authority-token", "Bearer invoke-token", "Bearer authority-token", "Bearer invoke-token", "Bearer authority-token", "Bearer invoke-token"])
    } finally {
        RemoteActorClient.resetForTests()
        grpcServer.forceShutdown()
        await new Promise<void>((resolve, reject) => tokenServer.close(error => (error ? reject(error) : resolve())))
    }
})

function unaryDefinition(path: string, requestType: Parameters<typeof protocol.encode>[0], responseType: Parameters<typeof protocol.encode>[0]): grpc.MethodDefinition<unknown, unknown> {
    return {
        path,
        requestStream: false,
        responseStream: false,
        requestSerialize: value => protocol.encode(requestType, value),
        requestDeserialize: value => protocol.decode(requestType, value),
        responseSerialize: value => protocol.encode(responseType, value),
        responseDeserialize: value => protocol.decode(responseType, value),
        originalName: path.split("/").at(-1) ?? "unary"
    }
}

function bind(server: grpc.Server): Promise<number> {
    return new Promise((resolve, reject) => {
        server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (error, port) => {
            if (error) reject(error)
            else resolve(port)
        })
    })
}
