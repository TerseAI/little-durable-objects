import * as grpc from "@grpc/grpc-js"
import protobuf from "protobufjs"
import { z } from "zod"

import { ActorConfigurationError, ActorInvocationError, ActorProtocolError } from "../shared/errors.js"
import { currentActorInvocation } from "../shared/invocationContext.js"
import { JsonActorStateSerializer, MAX_ACTOR_INVOCATION_TIMEOUT_MS, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

import { actorGrpcSchema } from "./generated/actorGrpcSchema.js"

const CONTROL_PLANE_INVOKE_PATH = "/durable_object.v1.ActorControlPlaneService/Invoke"

const namespaceIdSchema = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const remoteSettingsSchema = z.object({
    DURABLE_OBJECT_TOKEN: z.string().trim().min(1),
    DURABLE_OBJECT_NAMESPACE_ID: namespaceIdSchema,
    DURABLE_OBJECT_CONTROL_PLANE_URL: z.string().url(),
    DURABLE_OBJECT_INVOCATION_TIMEOUT_MS: z.coerce.number().int().positive().max(MAX_ACTOR_INVOCATION_TIMEOUT_MS).default(30_000)
})

const clientOptionsSchema = z.object({
    token: z.string().trim().min(1),
    namespaceId: namespaceIdSchema,
    controlPlaneUrl: z.string().url(),
    invocationTimeoutMs: z.number().int().positive().max(MAX_ACTOR_INVOCATION_TIMEOUT_MS).default(30_000)
})

class RemoteActorClient {
    private static instance: RemoteActorClient | undefined

    private readonly serializer = new JsonActorStateSerializer()
    private grpcClientValue: grpc.Client | undefined
    private settingsValue: RemoteActorSettings | undefined

    private constructor(
        private readonly environment: NodeJS.ProcessEnv = process.env,
        configuredSettings?: RemoteActorSettings
    ) {
        this.settingsValue = configuredSettings
    }

    private get settings(): RemoteActorSettings {
        if (this.settingsValue !== undefined) return this.settingsValue
        const result = remoteSettingsSchema.safeParse(this.environment)
        if (!result.success) throw new ActorConfigurationError(`remote actor settings are invalid: ${result.error.message}`)
        this.settingsValue = {
            token: result.data.DURABLE_OBJECT_TOKEN,
            namespaceId: result.data.DURABLE_OBJECT_NAMESPACE_ID,
            controlPlaneUrl: result.data.DURABLE_OBJECT_CONTROL_PLANE_URL,
            invocationTimeoutMs: result.data.DURABLE_OBJECT_INVOCATION_TIMEOUT_MS
        }
        return this.settingsValue
    }

    static getInstance(): RemoteActorClient {
        this.instance ??= new RemoteActorClient()
        return this.instance
    }

    static configure(options: DurableObjectsClientOptions): void {
        const result = clientOptionsSchema.safeParse(options)
        if (!result.success) throw new ActorConfigurationError(`durable-object client settings are invalid: ${result.error.message}`)
        this.instance?.close()
        this.instance = new RemoteActorClient(process.env, result.data)
    }

    static resetForTests(): void {
        this.instance?.close()
        this.instance = undefined
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[], timeoutMs?: number): Promise<unknown> {
        const requestId = validateActorComponent("request ID", globalThis.crypto.randomUUID())
        if (currentActorInvocation() !== undefined) {
            throw new ActorInvocationError("actor_error", requestId, "actor-to-actor calls are not available")
        }
        const callerTimeoutMs = timeoutMs ?? this.settings.invocationTimeoutMs
        let reply: InvokeActorReplyMessage
        try {
            reply = await callUnaryRpc<InvokeActorRequestMessage, InvokeActorReplyMessage>(
                this.grpcClient(),
                CONTROL_PLANE_INVOKE_PATH,
                invokeActorRequestType,
                invokeActorReplyType,
                {
                    requestId,
                    actor: {
                        namespaceId: this.settings.namespaceId,
                        actorType: validateActorComponent("actor type", actorType),
                        actorId: validateActorComponent("actor ID", actorId)
                    },
                    method: validateActorComponent("actor method", method),
                    argsJson: Buffer.from(JSON.stringify(this.jsonArguments(args))),
                    timeoutMs: callerTimeoutMs
                },
                this.settings.token,
                callerTimeoutMs
            )
        } catch (error) {
            if (error instanceof ActorConfigurationError || error instanceof ActorProtocolError) throw error
            if (isAuthenticationGrpcError(error)) {
                throw new ActorInvocationError("unauthenticated", requestId, "the durable-object workflow token was rejected")
            }
            if (isGrpcError(error, grpc.status.DEADLINE_EXCEEDED)) throw deadlineExceeded(requestId)
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", requestId, `control-plane RPC failed after dispatch: ${message}`)
        }

        if (reply.completed) return parseJson(reply.completed.resultJson, "actor result")
        if (reply.failed) throw new ActorInvocationError(reply.failed.code, requestId, reply.failed.message)
        if (reply.reroute) throw new ActorProtocolError("control plane exposed an internal reroute reply")
        throw new ActorProtocolError("control-plane reply did not contain a result")
    }

    private jsonArguments(args: readonly unknown[]): readonly JsonValue[] {
        const value = this.serializer.clone(args, "actor arguments")
        if (!Array.isArray(value)) throw new ActorProtocolError("actor arguments must be a JSON array")
        return value
    }

    private grpcClient(): grpc.Client {
        if (this.grpcClientValue) return this.grpcClientValue
        const parsed = endpoint(this.settings.controlPlaneUrl)
        this.grpcClientValue = new grpc.Client(parsed.target, parsed.credentials)
        return this.grpcClientValue
    }

    private close(): void {
        this.grpcClientValue?.close()
        this.grpcClientValue = undefined
    }
}

function endpoint(origin: string): { target: string; credentials: grpc.ChannelCredentials } {
    let url: URL
    try {
        url = new URL(origin)
    } catch (error) {
        throw new ActorConfigurationError(`actor gRPC origin is invalid: ${origin}`, { cause: error })
    }
    if (!/^https?:$/u.test(url.protocol) || !url.hostname || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
        throw new ActorConfigurationError(`actor gRPC origin must be an HTTP or HTTPS origin: ${origin}`)
    }
    const secure = url.protocol === "https:"
    return {
        target: url.port ? url.host : `${url.hostname}:${secure ? "443" : "80"}`,
        credentials: secure ? grpc.credentials.createSsl() : grpc.credentials.createInsecure()
    }
}

function callUnaryRpc<Request, Reply>(client: grpc.Client, path: string, requestType: protobuf.Type, replyType: protobuf.Type, request: Request, token: string, timeoutMs: number): Promise<Reply> {
    const metadata = new grpc.Metadata()
    metadata.set("authorization", `Bearer ${token}`)
    return new Promise((resolve, reject) => {
        client.makeUnaryRequest<Request, Reply>(
            path,
            value => encode(requestType, value),
            value => decode<Reply>(replyType, value),
            request,
            metadata,
            { deadline: Date.now() + timeoutMs },
            (error, reply) => {
                if (error) reject(error)
                else if (reply === undefined) reject(new ActorProtocolError("actor gRPC call returned no reply"))
                else resolve(reply)
            }
        )
    })
}

function encode(type: protobuf.Type, value: unknown): Buffer {
    const error = type.verify(value as Record<string, unknown>)
    if (error) throw new ActorProtocolError(`could not encode ${type.name}: ${error}`)
    return Buffer.from(type.encode(type.create(value as Record<string, unknown>)).finish())
}

function decode<T>(type: protobuf.Type, value: Buffer): T {
    return type.toObject(type.decode(value), { bytes: Buffer, longs: Number, oneofs: true }) as T
}

function parseJson(value: Buffer, label: string): unknown {
    try {
        return JSON.parse(value.toString("utf8")) as unknown
    } catch (error) {
        throw new ActorProtocolError(`${label} is not valid JSON`, { cause: error })
    }
}

function deadlineExceeded(requestId: string): ActorInvocationError {
    return new ActorInvocationError("deadline_exceeded", requestId, "actor invocation deadline exceeded; execution may still complete")
}

function isGrpcError(error: unknown, code: grpc.status): boolean {
    return error instanceof Error && Reflect.get(error, "code") === code
}

function isAuthenticationGrpcError(error: unknown): boolean {
    return isGrpcError(error, grpc.status.UNAUTHENTICATED) || isGrpcError(error, grpc.status.PERMISSION_DENIED)
}

const root = protobuf.Root.fromJSON(actorGrpcSchema)
const invokeActorRequestType = root.lookupType("durable_object.v1.InvokeActorRequest")
const invokeActorReplyType = root.lookupType("durable_object.v1.InvokeActorReply")

interface RemoteActorSettings {
    readonly token: string
    readonly namespaceId: string
    readonly controlPlaneUrl: string
    readonly invocationTimeoutMs: number
}

interface DurableObjectsClientOptions {
    readonly token: string
    readonly namespaceId: string
    readonly controlPlaneUrl: string
    readonly invocationTimeoutMs?: number
}

interface InvokeActorRequestMessage {
    readonly requestId: string
    readonly actor: { readonly namespaceId: string; readonly actorType: string; readonly actorId: string }
    readonly method: string
    readonly argsJson: Buffer
    readonly timeoutMs: number
}

interface InvokeActorReplyMessage {
    readonly completed?: { readonly resultJson: Buffer }
    readonly failed?: { readonly code: string; readonly message: string }
    readonly reroute?: Record<string, never>
}

const actorGrpcProtocolForTests = { invokeActorRequestType, invokeActorReplyType, encode, decode }

export { RemoteActorClient, actorGrpcProtocolForTests }
export type { DurableObjectsClientOptions }
