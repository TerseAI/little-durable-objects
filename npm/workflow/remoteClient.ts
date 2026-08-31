import * as grpc from "@grpc/grpc-js"
import protobuf from "protobufjs"
import { z } from "zod"

import { ActorConfigurationError, ActorInvocationError, ActorProtocolError } from "../shared/errors.js"
import { currentActorInvocation } from "../shared/invocationContext.js"
import { JsonActorStateSerializer, MAX_ACTOR_INVOCATION_TIMEOUT_MS, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

import { actorGrpcSchema } from "./generated/actorGrpcSchema.js"

const CONTROL_PLANE_RESOLVE_PATH = "/durable_object.v1.ActorControlPlaneService/ResolveActorHost"
const HOST_INVOKE_PATH = "/durable_object.v1.ActorHostService/Invoke"
const MAX_CACHED_GRPC_CLIENTS = 256
const MAX_RETRY_BACKOFF_MS = 250

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
    private readonly grpcClients = new Map<string, grpc.Client>()
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
        if (!result.success) {
            throw new ActorConfigurationError(`remote actor settings are invalid: ${result.error.message}`)
        }
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
        if (!result.success) {
            throw new ActorConfigurationError(`durable-object client settings are invalid: ${result.error.message}`)
        }
        this.instance?.close()
        this.instance = new RemoteActorClient(process.env, result.data)
    }

    static resetForTests(): void {
        this.instance?.close()
        this.instance = undefined
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[], timeoutMs?: number): Promise<unknown> {
        const requestId = validateActorComponent("request ID", globalThis.crypto.randomUUID())
        const parent = currentActorInvocation()
        if (parent !== undefined) {
            throw new ActorInvocationError("nested_actor_calls_unsupported", requestId, "actor-to-actor calls are not available in the MVP runtime")
        }
        const callerTimeoutMs = timeoutMs ?? this.settings.invocationTimeoutMs
        const deadline = performance.now() + callerTimeoutMs
        const actor: ActorKeyMessage = {
            namespaceId: this.settings.namespaceId,
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId)
        }
        const invocation = {
            method: validateActorComponent("actor method", method),
            args: this.jsonArguments(args)
        }

        for (let attempt = 0; ; attempt += 1) {
            let route: ResolvedActorHost
            try {
                route = await this.resolveHost(actor, requireRemainingTimeoutMs(deadline, requestId))
            } catch (error) {
                if (error instanceof ActorConfigurationError || error instanceof ActorProtocolError) throw error
                if (isRetryablePreExecutionGrpcError(error)) {
                    await waitForRetry(attempt, deadline, requestId)
                    continue
                }
                if (isAuthenticationGrpcError(error)) {
                    throw new ActorInvocationError("unauthenticated", requestId, "the durable-object workflow token was rejected")
                }
                throw grpcInvocationError(error, requestId, false)
            }

            let reply: InvokeActorReplyMessage
            try {
                reply = await callUnaryRpc<InvokeActorRequestMessage, InvokeActorReplyMessage>(
                    this.grpcClient(route.route),
                    HOST_INVOKE_PATH,
                    invokeActorRequestType,
                    invokeActorReplyType,
                    {
                        requestId,
                        actor,
                        method: invocation.method,
                        argsJson: Buffer.from(JSON.stringify(invocation.args)),
                        timeoutMs: requireRemainingTimeoutMs(deadline, requestId)
                    },
                    this.settings.token,
                    requireRemainingTimeoutMs(deadline, requestId)
                )
            } catch (error) {
                if (error instanceof ActorConfigurationError || error instanceof ActorProtocolError) throw error
                throw grpcInvocationError(error, requestId, true)
            }

            if (reply.completed) return parseJson(reply.completed.resultJson, "actor result")
            if (reply.reroute || (reply.failed && isRetryableBeforeExecutionCode(reply.failed.code))) {
                await waitForRetry(attempt, deadline, requestId)
                continue
            }
            if (reply.failed) throw new ActorInvocationError(reply.failed.code, requestId, reply.failed.message)
            throw new ActorProtocolError("actor host reply did not contain a result")
        }
    }

    private async resolveHost(actor: ActorKeyMessage, timeoutMs: number): Promise<ResolvedActorHost> {
        const resolved = await callUnaryRpc<ResolveActorHostRequestMessage, ResolvedActorHost>(
            this.grpcClient(this.settings.controlPlaneUrl),
            CONTROL_PLANE_RESOLVE_PATH,
            resolveActorHostRequestType,
            resolvedActorHostType,
            { actor },
            this.settings.token,
            timeoutMs
        )
        if (!resolved.route) throw new ActorProtocolError("control plane returned an incomplete actor-host route")
        endpoint(resolved.route)
        return resolved
    }

    private jsonArguments(args: readonly unknown[]): readonly JsonValue[] {
        const value = this.serializer.clone(args, "actor arguments")
        if (!Array.isArray(value)) throw new ActorProtocolError("actor arguments must be a JSON array")
        return value
    }

    private grpcClient(origin: string): grpc.Client {
        const existing = this.grpcClients.get(origin)
        if (existing) return existing
        const parsed = endpoint(origin)
        const grpcClient = new grpc.Client(parsed.target, parsed.credentials, parsed.options)
        if (this.grpcClients.size >= MAX_CACHED_GRPC_CLIENTS) {
            const evictedOrigin = [...this.grpcClients.keys()].find(candidate => candidate !== this.settings.controlPlaneUrl) ?? this.grpcClients.keys().next().value
            if (evictedOrigin !== undefined) {
                this.grpcClients.get(evictedOrigin)?.close()
                this.grpcClients.delete(evictedOrigin)
            }
        }
        this.grpcClients.set(origin, grpcClient)
        return grpcClient
    }

    private close(): void {
        this.grpcClients.forEach(client => client.close())
        this.grpcClients.clear()
    }
}

function endpoint(origin: string): { target: string; credentials: grpc.ChannelCredentials; options?: grpc.ClientOptions } {
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
    const target = url.port ? url.host : `${url.hostname}:${secure ? "443" : "80"}`
    return { target, credentials: secure ? grpc.credentials.createSsl() : grpc.credentials.createInsecure() }
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
    const object = value as Record<string, unknown>
    const error = type.verify(object)
    if (error) throw new ActorProtocolError(`could not encode ${type.name}: ${error}`)
    return Buffer.from(type.encode(type.create(object)).finish())
}

function decode<T>(type: protobuf.Type, value: Buffer): T {
    const message = type.decode(value)
    return type.toObject(message, { bytes: Buffer, longs: Number, oneofs: true }) as T
}

function parseJson(value: Buffer, label: string): unknown {
    try {
        return JSON.parse(value.toString("utf8")) as unknown
    } catch (error) {
        throw new ActorProtocolError(`${label} is not valid JSON`, { cause: error })
    }
}

function requireRemainingTimeoutMs(deadline: number, requestId: string): number {
    const remainingMs = Math.ceil(deadline - performance.now())
    if (remainingMs <= 0) throw deadlineExceeded(requestId)
    return remainingMs
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

function isRetryablePreExecutionGrpcError(error: unknown): boolean {
    return isGrpcError(error, grpc.status.UNAVAILABLE) || isGrpcError(error, grpc.status.RESOURCE_EXHAUSTED)
}

function isRetryableBeforeExecutionCode(code: string): boolean {
    return code === "host_unavailable" || code === "resource_exhausted"
}

async function waitForRetry(attempt: number, deadline: number, requestId: string): Promise<void> {
    const maximumDelayMs = Math.min(MAX_RETRY_BACKOFF_MS, 25 * 2 ** Math.min(attempt, 10))
    const delayMs = Math.max(1, Math.floor(maximumDelayMs / 2 + Math.random() * (maximumDelayMs / 2)))
    if (deadline - performance.now() <= delayMs) throw deadlineExceeded(requestId)
    await new Promise(resolve => setTimeout(resolve, delayMs))
}

function grpcInvocationError(error: unknown, requestId: string, dispatched: boolean): ActorInvocationError {
    if (isGrpcError(error, grpc.status.DEADLINE_EXCEEDED)) return deadlineExceeded(requestId)
    const message = error instanceof Error ? error.message : String(error)
    return dispatched
        ? new ActorInvocationError("outcome_unknown", requestId, `actor host RPC failed after dispatch: ${message}`)
        : new ActorInvocationError("host_unavailable", requestId, `actor host could not be resolved: ${message}`)
}

const root = protobuf.Root.fromJSON(actorGrpcSchema)

const resolveActorHostRequestType = root.lookupType("durable_object.v1.ResolveActorHostRequest")
const resolvedActorHostType = root.lookupType("durable_object.v1.ResolvedActorHost")
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

interface ActorKeyMessage {
    readonly namespaceId: string
    readonly actorType: string
    readonly actorId: string
}

interface ResolveActorHostRequestMessage {
    readonly actor: ActorKeyMessage
}

interface ResolvedActorHost {
    readonly route: string
}

interface InvokeActorRequestMessage {
    readonly requestId: string
    readonly actor: ActorKeyMessage
    readonly method: string
    readonly argsJson: Buffer
    readonly timeoutMs: number
}

interface InvokeActorReplyMessage {
    readonly completed?: { readonly resultJson: Buffer }
    readonly failed?: { readonly code: string; readonly message: string }
    readonly reroute?: Record<string, never>
}

const actorGrpcProtocolForTests = {
    resolveActorHostRequestType,
    resolvedActorHostType,
    invokeActorRequestType,
    invokeActorReplyType,
    encode,
    decode
}

export { RemoteActorClient, actorGrpcProtocolForTests }
export type { DurableObjectsClientOptions }
