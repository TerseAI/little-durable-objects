import * as grpc from "@grpc/grpc-js"
import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorInvocationError } from "../shared/errors.js"

import { RemoteActorClient, actorGrpcProtocolForTests as protocol } from "./remoteClient.js"

test("remote actor client sends one authenticated control-plane invocation", async () => {
    let calls = 0
    const server = new grpc.Server()
    server.addService(
        { invoke: unaryDefinition("/durable_object.v1.ActorControlPlaneService/Invoke", protocol.invokeActorRequestType, protocol.invokeActorReplyType) },
        {
            invoke(call: grpc.ServerUnaryCall<Record<string, unknown>, Record<string, unknown>>, callback: grpc.sendUnaryData<Record<string, unknown>>) {
                calls += 1
                assert.equal(call.metadata.get("authorization")[0], "Bearer workflow-token")
                assert.deepEqual(call.request.actor, { namespaceId: "project-1", actorType: "Counter", actorId: "counter-1" })
                assert.deepEqual(JSON.parse((call.request.argsJson as Buffer).toString("utf8")), [2])
                callback(null, { completed: { resultJson: Buffer.from("7") } })
            }
        }
    )
    const port = await bind(server)
    RemoteActorClient.configure({
        token: "workflow-token",
        namespaceId: "project-1",
        controlPlaneUrl: `http://127.0.0.1:${port}`,
        invocationTimeoutMs: 5_000
    })
    try {
        assert.equal(await RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(calls, 1)
    } finally {
        RemoteActorClient.resetForTests()
        server.forceShutdown()
    }
})

test("does not retry a control-plane transport failure", async () => {
    let calls = 0
    const server = new grpc.Server()
    server.addService(
        { invoke: unaryDefinition("/durable_object.v1.ActorControlPlaneService/Invoke", protocol.invokeActorRequestType, protocol.invokeActorReplyType) },
        {
            invoke(_call: grpc.ServerUnaryCall<Record<string, unknown>, Record<string, unknown>>, callback: grpc.sendUnaryData<Record<string, unknown>>) {
                calls += 1
                callback(serviceError(grpc.status.UNAVAILABLE, "connection was lost"))
            }
        }
    )
    const port = await bind(server)
    RemoteActorClient.configure({ token: "workflow-token", namespaceId: "project-1", controlPlaneUrl: `http://127.0.0.1:${port}`, invocationTimeoutMs: 5_000 })
    try {
        await assert.rejects(RemoteActorClient.getInstance().invoke("Counter", "counter-1", "increment", [2]), error => error instanceof ActorInvocationError && error.code === "outcome_unknown")
        assert.equal(calls, 1)
    } finally {
        RemoteActorClient.resetForTests()
        server.forceShutdown()
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
        originalName: "invoke"
    }
}

function bind(server: grpc.Server): Promise<number> {
    return new Promise((resolve, reject) => {
        server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (error, port) => (error ? reject(error) : resolve(port)))
    })
}

function serviceError(code: grpc.status, message: string): grpc.ServiceError {
    return Object.assign(new Error(message), { code, details: message, metadata: new grpc.Metadata() })
}
