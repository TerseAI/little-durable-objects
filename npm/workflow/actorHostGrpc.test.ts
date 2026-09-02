import { Server, ServerCredentials, loadPackageDefinition } from "@grpc/grpc-js"
import type { ServerUnaryCall, ServiceClientConstructor, sendUnaryData } from "@grpc/grpc-js"
import { loadSync } from "@grpc/proto-loader"
import assert from "node:assert/strict"
import { resolve } from "node:path"
import { test } from "node:test"

import { GrpcActorHostTransport } from "./actorHostGrpc.js"

test("direct transport speaks the actor host protobuf contract", async () => {
    const server = new Server()
    const definition = loadPackageDefinition(
        loadSync(resolve("../proto/durable_object.proto"), {
            defaults: true,
            longs: Number,
            oneofs: true
        })
    ) as unknown as GrpcPackages
    server.addService(definition.durable_object.v1.ActorHostService.service, {
        invoke(call: ServerUnaryCall<HostRequest, HostReply>, callback: sendUnaryData<HostReply>) {
            assert.equal(call.metadata.get("authorization")[0], "Bearer direct-token")
            assert.deepEqual(call.request, {
                invocation: {
                    requestId: "request-1",
                    actor: { namespaceId: "project-1", actorType: "Counter", actorId: "counter-1" },
                    method: "increment",
                    argsJson: Buffer.from("[2]")
                },
                ownerEpoch: 3,
                stateVersion: 7,
                stateReadUrl: "https://storage.example.com/state"
            })
            callback(null, { completed: { resultJson: Buffer.from("7") }, result: "completed" })
        }
    })
    const port = await listen(server)
    const transport = new GrpcActorHostTransport()
    try {
        assert.deepEqual(
            await transport.invoke(
                {
                    route: `http://127.0.0.1:${port}`,
                    token: "direct-token",
                    ownerEpoch: 3,
                    stateVersion: 7,
                    stateReadUrl: "https://storage.example.com/state",
                    expiresAtMs: 4_000_000_000_000
                },
                {
                    requestId: "request-1",
                    namespaceId: "project-1",
                    actorType: "Counter",
                    actorId: "counter-1",
                    method: "increment",
                    args: [2]
                }
            ),
            { type: "completed", result: 7 }
        )
    } finally {
        server.forceShutdown()
    }
})

function listen(server: Server): Promise<number> {
    return new Promise((resolvePort, reject) => {
        server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => {
            if (error) reject(error)
            else resolvePort(port)
        })
    })
}

interface GrpcPackages {
    readonly durable_object: {
        readonly v1: {
            readonly ActorHostService: ServiceClientConstructor
        }
    }
}

interface HostRequest {
    readonly invocation: {
        readonly requestId: string
        readonly actor: { readonly namespaceId: string; readonly actorType: string; readonly actorId: string }
        readonly method: string
        readonly argsJson: Buffer
    }
    readonly ownerEpoch: number
    readonly stateVersion: number
    readonly stateReadUrl: string
}

type HostReply = { readonly completed: { readonly resultJson: Buffer }; readonly result: "completed" }
