import { Client, Metadata, credentials } from "@grpc/grpc-js"
import protobuf from "protobufjs"

import { ActorProtocolError } from "../shared/errors.js"
import type { JsonValue, SocketEffect } from "../shared/types.js"

const { Field, OneOf, Type } = protobuf
const MAX_MESSAGE_BYTES = 32 * 1024 * 1024

interface ActorHostTarget {
    readonly route: string
    readonly token: string
    readonly ownerEpoch: number
    readonly stateVersion: number
    readonly stateReadUrl: string
    readonly expiresAtMs: number
}

interface DirectActorInvocation {
    readonly requestId: string
    readonly namespaceId: string
    readonly actorType: string
    readonly actorId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}

type ActorHostReply =
    | { readonly type: "completed"; readonly result: unknown; readonly effects: readonly SocketEffect[] }
    | { readonly type: "failed"; readonly code: string; readonly message: string }
    | { readonly type: "reroute" }

interface ActorHostTransport {
    invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply>
}

class GrpcActorHostTransport implements ActorHostTransport {
    private readonly clients = new Map<string, Client>()

    async invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply> {
        const metadata = new Metadata()
        metadata.set("authorization", `Bearer ${target.token}`)
        const request: HostInvokeActorRequest = {
            invocation: {
                requestId: invocation.requestId,
                actor: {
                    namespaceId: invocation.namespaceId,
                    actorType: invocation.actorType,
                    actorId: invocation.actorId
                },
                method: invocation.method,
                argsJson: Buffer.from(JSON.stringify(invocation.args))
            },
            ownerEpoch: target.ownerEpoch,
            stateVersion: target.stateVersion,
            stateReadUrl: target.stateReadUrl
        }
        const reply = await unaryRequest(this.client(target.route), request, metadata)
        return decodeReply(reply)
    }

    private client(route: string): Client {
        const existing = this.clients.get(route)
        if (existing) return existing
        const url = actorHostUrl(route)
        const address = url.port ? url.host : `${url.hostname}:${url.protocol === "https:" ? 443 : 80}`
        const client = new Client(address, url.protocol === "https:" ? credentials.createSsl() : credentials.createInsecure(), {
            "grpc.max_receive_message_length": MAX_MESSAGE_BYTES,
            "grpc.max_send_message_length": MAX_MESSAGE_BYTES
        })
        this.clients.set(route, client)
        return client
    }
}

function unaryRequest(client: Client, request: HostInvokeActorRequest, metadata: Metadata): Promise<InvokeActorReply> {
    return new Promise((resolve, reject) => {
        client.makeUnaryRequest("/durable_object.v1.ActorHostService/Invoke", serializeHostRequest, deserializeHostReply, request, metadata, (error, reply) => {
            if (error) reject(error)
            else if (reply) resolve(reply)
            else reject(new ActorProtocolError("actor host gRPC response was empty"))
        })
    })
}

function decodeReply(reply: InvokeActorReply): ActorHostReply {
    if (reply.completed) {
        try {
            return {
                type: "completed",
                result: JSON.parse(Buffer.from(reply.completed.resultJson).toString("utf8")) as unknown,
                effects: JSON.parse(Buffer.from(reply.completed.socketEffectsJson).toString("utf8")) as SocketEffect[]
            }
        } catch (error) {
            throw new ActorProtocolError("actor host result was not valid JSON", { cause: error })
        }
    }
    if (reply.failed) return { type: "failed", code: reply.failed.code, message: reply.failed.message }
    if (reply.reroute) return { type: "reroute" }
    throw new ActorProtocolError("actor host response did not contain a result")
}

function actorHostUrl(route: string): URL {
    const url = new URL(route)
    if (!/^https?:$/u.test(url.protocol) || !url.hostname || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
        throw new ActorProtocolError("actor host route must be an HTTP or HTTPS origin")
    }
    return url
}

const actorKeyType = new Type("ActorKey")
    .add(new Field("namespaceId", 1, "string"))
    .add(new Field("actorType", 2, "string"))
    .add(new Field("actorId", 3, "string"))
const invocationType = new Type("InvokeActorRequest")
    .add(new Field("requestId", 1, "string"))
    .add(new Field("actor", 2, "ActorKey"))
    .add(new Field("method", 3, "string"))
    .add(new Field("argsJson", 4, "bytes"))
    .add(actorKeyType)
const hostRequestType = new Type("HostInvokeActorRequest")
    .add(new Field("invocation", 1, "InvokeActorRequest"))
    .add(new Field("ownerEpoch", 2, "uint64"))
    .add(new Field("stateReadUrl", 3, "string"))
    .add(new Field("stateVersion", 4, "uint64"))
    .add(invocationType)
const completedType = new Type("ActorCompleted").add(new Field("resultJson", 1, "bytes")).add(new Field("socketEffectsJson", 2, "bytes"))
const failedType = new Type("ActorFailed").add(new Field("code", 1, "string")).add(new Field("message", 2, "string"))
const rerouteType = new Type("Reroute")
const replyType = new Type("InvokeActorReply")
    .add(new Field("completed", 1, "ActorCompleted"))
    .add(new Field("failed", 2, "ActorFailed"))
    .add(new Field("reroute", 3, "Reroute"))
    .add(new OneOf("result", ["completed", "failed", "reroute"]))
    .add(completedType)
    .add(failedType)
    .add(rerouteType)

function serializeHostRequest(request: HostInvokeActorRequest): Buffer {
    return Buffer.from(hostRequestType.encode(request).finish())
}

function deserializeHostReply(bytes: Buffer): InvokeActorReply {
    return replyType.decode(bytes) as unknown as InvokeActorReply
}

interface HostInvokeActorRequest {
    readonly invocation: {
        readonly requestId: string
        readonly actor: {
            readonly namespaceId: string
            readonly actorType: string
            readonly actorId: string
        }
        readonly method: string
        readonly argsJson: Uint8Array
    }
    readonly ownerEpoch: number
    readonly stateVersion: number
    readonly stateReadUrl: string
}

interface InvokeActorReply {
    readonly completed?: { readonly resultJson: Uint8Array; readonly socketEffectsJson: Uint8Array }
    readonly failed?: { readonly code: string; readonly message: string }
    readonly reroute?: Record<string, never>
}

export { GrpcActorHostTransport }
export type { ActorHostReply, ActorHostTarget, ActorHostTransport, DirectActorInvocation }
