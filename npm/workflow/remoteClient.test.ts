import * as grpc from "@grpc/grpc-js"
import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorInvocationError } from "../shared/errors.js"

import { RemoteActorClient, actorGrpcProtocolForTests as protocol } from "./remoteClient.js"

test("remote actor client uses one token and preserves its request ID across proven pre-execution retries", async () => {
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
                    callback(null, { failed: { code: "host_unavailable", message: "host is draining" } })
                    return
                }
                if (invocations === 2) {
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
    RemoteActorClient.configure({
        token: "workflow-token",
        namespaceId: "namespace-1",
        controlPlaneUrl: grpcOrigin,
        invocationTimeoutMs: 5_000
    })

    try {
        assert.equal(await RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(await RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(resolutions, 4)
        assert.equal(invocations, 4)
        assert.equal(requestIds[0], requestIds[1], "a routing retry must preserve the idempotency key")
        assert.equal(requestIds[1], requestIds[2], "every pre-execution retry must preserve the idempotency key")
        assert.notEqual(requestIds[2], requestIds[3], "a separate invocation must use a new idempotency key")
        assert.deepEqual(
            authorization,
            Array.from({ length: 8 }, () => "Bearer workflow-token")
        )
    } finally {
        RemoteActorClient.resetForTests()
        grpcServer.forceShutdown()
    }
})

test("does not retry a transport failure after invocation dispatch", async () => {
    let resolutions = 0
    let invocations = 0
    const grpcServer = new grpc.Server()
    grpcServer.addService(
        {
            resolveActorHost: unaryDefinition("/durable_object.v1.ActorControlPlaneService/ResolveActorHost", protocol.resolveActorHostRequestType, protocol.resolvedActorHostType)
        },
        {
            resolveActorHost(_call: grpc.ServerUnaryCall<Record<string, unknown>, Record<string, unknown>>, callback: grpc.sendUnaryData<Record<string, unknown>>) {
                resolutions += 1
                callback(null, { route: grpcOrigin })
            }
        }
    )
    grpcServer.addService(
        {
            invoke: unaryDefinition("/durable_object.v1.ActorHostService/Invoke", protocol.invokeActorRequestType, protocol.invokeActorReplyType)
        },
        {
            invoke(_call: grpc.ServerUnaryCall<Record<string, unknown>, Record<string, unknown>>, callback: grpc.sendUnaryData<Record<string, unknown>>) {
                invocations += 1
                callback(serviceError(grpc.status.UNAVAILABLE, "connection was lost"))
            }
        }
    )
    let grpcOrigin = ""
    const grpcPort = await bind(grpcServer)
    grpcOrigin = `http://127.0.0.1:${grpcPort}`
    RemoteActorClient.configure({ token: "workflow-token", namespaceId: "namespace-1", controlPlaneUrl: grpcOrigin, invocationTimeoutMs: 5_000 })

    try {
        await assert.rejects(RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), error => error instanceof ActorInvocationError && error.code === "outcome_unknown")
        assert.equal(resolutions, 1)
        assert.equal(invocations, 1)
    } finally {
        RemoteActorClient.resetForTests()
        grpcServer.forceShutdown()
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

function serviceError(code: grpc.status, message: string): grpc.ServiceError {
    return Object.assign(new Error(message), {
        code,
        details: message,
        metadata: new grpc.Metadata()
    })
}
