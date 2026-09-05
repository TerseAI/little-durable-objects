import WebSocket from "ws"
import { z } from "zod"

import { ActorConfigurationError, ActorInvocationError, ActorProtocolError } from "../shared/errors.js"
import { currentActorInvocation } from "../shared/invocationContext.js"
import { socketMessage } from "../shared/socket.js"
import type { ActorConnection, ActorSocketMessage } from "../shared/socket.js"
import { LatencyTimeline, stderrTelemetry } from "../shared/telemetry.js"
import type { TelemetrySink } from "../shared/telemetry.js"
import { JsonActorStateSerializer, validateActorComponent } from "../shared/types.js"
import type { JsonValue, SocketEffect } from "../shared/types.js"

import { GrpcActorHostTransport } from "./actorHostGrpc.js"
import type { ActorHostTarget, ActorHostTransport, DirectActorInvocation } from "./actorHostGrpc.js"
import { configuredSettings, validateOrigin } from "./clientSettings.js"

const namespaceIdSchema = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const remoteSettingsSchema = z.object({
    DURABLE_OBJECT_TOKEN: z.string().trim().min(1),
    DURABLE_OBJECT_NAMESPACE_ID: namespaceIdSchema,
    DURABLE_OBJECT_CONTROL_PLANE_URL: z.string().url(),
    DURABLE_OBJECT_SOCKET_GATEWAY_URL: z.string().url().optional()
})

const errorDocumentSchema = z.object({
    error: z.object({
        code: z.string().min(1),
        message: z.string(),
        requestId: z.string().optional()
    })
})

const actorHostTargetSchema = z.object({
    route: z.string().url(),
    token: z.string().trim().min(1),
    ownerEpoch: z.number().int().positive(),
    stateVersion: z.number().int().nonnegative(),
    stateReadUrl: z.union([z.literal(""), z.string().url()]),
    expiresAtMs: z.number().int().positive()
})

const TARGET_EXPIRATION_SAFETY_MS = 5_000

class RemoteActorClient {
    private readonly serializer = new JsonActorStateSerializer()
    private settingsValue: RemoteActorSettings | undefined
    private readonly environment: NodeJS.ProcessEnv
    private readonly fetchRequest: typeof globalThis.fetch
    private readonly requestId: () => string
    private readonly actorHost: ActorHostTransport
    private readonly now: () => number
    private readonly monotonicNow: () => number
    private readonly telemetry: TelemetrySink
    private readonly targets = new Map<string, Promise<ActorHostTarget>>()
    private readonly connectWebSocket: WebSocketConnector

    constructor(options?: DurableObjectsClientOptions, dependencies: RemoteActorClientDependencies = {}) {
        this.environment = dependencies.environment ?? process.env
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch
        this.requestId = dependencies.requestId ?? (() => globalThis.crypto.randomUUID())
        this.actorHost = dependencies.actorHost ?? new GrpcActorHostTransport()
        this.now = dependencies.now ?? Date.now
        this.monotonicNow = dependencies.monotonicNow ?? (() => performance.now())
        this.telemetry = dependencies.telemetry ?? stderrTelemetry
        this.connectWebSocket = dependencies.connectWebSocket ?? openWebSocket
        this.settingsValue = options === undefined ? undefined : configuredSettings(options)
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        const requestId = validateActorComponent("request ID", this.requestId())
        const timeline = new LatencyTimeline(this.monotonicNow)
        let outcome = "failed"
        try {
            if (currentActorInvocation() !== undefined) {
                throw new ActorInvocationError("actor_error", requestId, "actor-to-actor calls are not available")
            }
            const invocation = this.invocation(requestId, actorType, actorId, method, args)
            timeline.mark("invocation_built")
            const target = await this.target(invocation, timeline)
            timeline.mark("target_resolved")
            const result = await this.direct(target, invocation, true, timeline)
            outcome = "completed"
            return result
        } finally {
            this.telemetry({
                event: "actor_client_invocation",
                request_id: requestId,
                namespace_id: this.settings.namespaceId,
                actor_type: actorType,
                actor_id: actorId,
                method,
                ...timeline.finish(),
                outcome
            })
        }
    }

    async connect(actorType: string, actorId: string, metadata: unknown): Promise<ActorConnection> {
        const requestId = validateActorComponent("request ID", this.requestId())
        if (currentActorInvocation() !== undefined) throw new ActorInvocationError("actor_error", requestId, "actor-to-actor socket connections are not available")
        const actor = {
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId)
        }
        const attachment = this.serializer.clone(metadata, "socket metadata")
        if (Buffer.byteLength(JSON.stringify(attachment)) > 64 * 1024) throw new ActorProtocolError("socket metadata must not exceed 64 KiB")
        return this.connectWebSocket(socketUrl(this.settings.socketGatewayUrl, this.settings.namespaceId, actor.actorType, actor.actorId), this.settings.token, attachment)
    }

    async broadcast(actorType: string, actorId: string, message: ActorSocketMessage): Promise<void> {
        const requestId = validateActorComponent("request ID", this.requestId())
        if (currentActorInvocation() !== undefined) throw new ActorInvocationError("actor_error", requestId, "actor-to-actor socket broadcasts are not available")
        await this.deliverSocketEffects(
            validateActorComponent("actor type", actorType),
            validateActorComponent("actor ID", actorId),
            [{ type: "broadcast", message: socketMessage(message), except_connection_ids: [], tags: [] }],
            requestId,
            "actor socket broadcast"
        )
    }

    private async direct(target: ActorHostTarget, invocation: DirectActorInvocation, retryReroute: boolean, timeline: LatencyTimeline): Promise<unknown> {
        try {
            const reply = await this.actorHost.invoke(target, invocation)
            timeline.mark("host_rpc_completed")
            if (reply.type === "completed") {
                await this.applySocketEffects(invocation, reply.effects)
                timeline.mark("socket_effects_completed")
                return reply.result
            }
            if (reply.type === "failed") throw new ActorInvocationError(reply.code, invocation.requestId, reply.message)
            if (!retryReroute) throw new ActorInvocationError("unavailable", invocation.requestId, "actor ownership changed repeatedly before execution")
            this.targets.delete(actorKey(invocation.actorType, invocation.actorId))
            const rerouted = await this.target(invocation, timeline)
            return this.direct(rerouted, invocation, false, timeline)
        } catch (error) {
            if (error instanceof ActorInvocationError || error instanceof ActorProtocolError) throw error
            this.targets.delete(actorKey(invocation.actorType, invocation.actorId))
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", invocation.requestId, `actor-host gRPC request failed after dispatch: ${message}`)
        }
    }

    private async applySocketEffects(invocation: DirectActorInvocation, effects: readonly SocketEffect[]): Promise<void> {
        if (effects.length === 0) return
        return this.deliverSocketEffects(invocation.actorType, invocation.actorId, effects, invocation.requestId, "actor completed but socket effects")
    }

    private async deliverSocketEffects(actorType: string, actorId: string, effects: readonly SocketEffect[], requestId: string, failureContext: string): Promise<void> {
        let response: Response
        try {
            response = await this.fetchRequest(socketEffectsUrl(this.settings, actorType, actorId), {
                method: "POST",
                headers: { "content-type": "application/json", authorization: `Bearer ${this.settings.token}` },
                body: JSON.stringify({ effects })
            })
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", requestId, `${failureContext} could not be delivered: ${message}`)
        }
        if (!response.ok) {
            throw new ActorInvocationError("outcome_unknown", requestId, `${failureContext} returned HTTP ${response.status}`)
        }
    }

    private async target(invocation: DirectActorInvocation, timeline: LatencyTimeline): Promise<ActorHostTarget> {
        const key = actorKey(invocation.actorType, invocation.actorId)
        const current = this.targets.get(key)
        timeline.mark("target_cache_checked")
        if (current) {
            const target = await current
            if (target.expiresAtMs > this.now() + TARGET_EXPIRATION_SAFETY_MS) return target
            this.targets.delete(key)
        }
        const resolving = this.resolveTarget(invocation).catch(error => {
            this.targets.delete(key)
            throw error
        })
        this.targets.set(key, resolving)
        return resolving
    }

    private async resolveTarget(invocation: DirectActorInvocation): Promise<ActorHostTarget> {
        let response: Response
        try {
            response = await this.fetchRequest(targetUrl(this.settings, invocation.actorType, invocation.actorId), {
                method: "POST",
                headers: {
                    accept: "application/json",
                    authorization: `Bearer ${this.settings.token}`,
                    "x-request-id": invocation.requestId
                }
            })
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", invocation.requestId, `control-plane HTTP request failed before dispatch: ${message}`)
        }
        const document = await responseDocument(response)
        if (!response.ok) this.throwResponseFailure(response, document, invocation.requestId)
        const target = actorHostTargetSchema.safeParse(document)
        if (!target.success) throw new ActorProtocolError("control-plane response did not contain a valid actor host target")
        return target.data
    }

    private throwResponseFailure(response: Response, document: unknown, requestId: string): never {
        if (response.status === 401 || response.status === 403) {
            throw new ActorInvocationError("unauthenticated", requestId, "the durable-object workflow token was rejected")
        }
        const failure = errorDocumentSchema.safeParse(document)
        if (!failure.success) {
            throw new ActorProtocolError(`control-plane HTTP ${response.status} response did not contain a valid error`)
        }
        throw new ActorInvocationError(failure.data.error.code, failure.data.error.requestId ?? requestId, failure.data.error.message)
    }

    private invocation(requestId: string, actorType: string, actorId: string, method: string, args: readonly unknown[]): DirectActorInvocation {
        return {
            requestId,
            namespaceId: this.settings.namespaceId,
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            method: validateActorComponent("actor method", method),
            args: this.jsonArguments(args)
        }
    }

    private get settings(): RemoteActorSettings {
        if (this.settingsValue !== undefined) return this.settingsValue
        const result = remoteSettingsSchema.safeParse(this.environment)
        if (!result.success) throw new ActorConfigurationError(`remote actor settings are invalid: ${result.error.message}`)
        this.settingsValue = {
            token: result.data.DURABLE_OBJECT_TOKEN,
            namespaceId: result.data.DURABLE_OBJECT_NAMESPACE_ID,
            controlPlaneUrl: validateOrigin(result.data.DURABLE_OBJECT_CONTROL_PLANE_URL),
            socketGatewayUrl: validateOrigin(result.data.DURABLE_OBJECT_SOCKET_GATEWAY_URL ?? result.data.DURABLE_OBJECT_CONTROL_PLANE_URL)
        }
        return this.settingsValue
    }

    private jsonArguments(args: readonly unknown[]): readonly JsonValue[] {
        const value = this.serializer.clone(args, "actor arguments")
        if (!Array.isArray(value)) throw new ActorProtocolError("actor arguments must be a JSON array")
        return value
    }
}

function targetUrl(settings: RemoteActorSettings, actorType: string, actorId: string): string {
    const actor = validateActorComponent("actor type", actorType)
    const id = validateActorComponent("actor ID", actorId)
    return `${settings.controlPlaneUrl}/v1/namespaces/${encodeURIComponent(settings.namespaceId)}/actors/${encodeURIComponent(actor)}/${encodeURIComponent(id)}/target`
}

function socketUrl(route: string, namespaceId: string, actorType: string, actorId: string): string {
    const url = new URL(route)
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:"
    url.pathname = `/v1/namespaces/${encodeURIComponent(namespaceId)}/actors/${encodeURIComponent(actorType)}/${encodeURIComponent(actorId)}/websocket`
    return url.href
}

function socketEffectsUrl(settings: RemoteActorSettings, actorType: string, actorId: string): string {
    const actor = validateActorComponent("actor type", actorType)
    const id = validateActorComponent("actor ID", actorId)
    return `${settings.socketGatewayUrl}/v1/namespaces/${encodeURIComponent(settings.namespaceId)}/actors/${encodeURIComponent(actor)}/${encodeURIComponent(id)}/socket-effects`
}

function openWebSocket(url: string, token: string, metadata: JsonValue): Promise<ActorConnection> {
    const socket = new WebSocket(url, {
        headers: {
            authorization: `Bearer ${token}`
        }
    })
    return new Promise((resolve, reject) => {
        let opened = false
        socket.addEventListener(
            "open",
            () => {
                try {
                    socket.send(JSON.stringify({ type: "initialize", metadata }))
                    opened = true
                    resolve(socket as ActorConnection)
                } catch (error) {
                    socket.close()
                    reject(error)
                }
            },
            { once: true }
        )
        socket.addEventListener("error", () => {
            if (!opened) reject(new Error("actor WebSocket connection failed"))
        })
        socket.addEventListener("close", () => {
            if (!opened) reject(new Error("actor WebSocket closed before connecting"))
        })
    })
}

function actorKey(actorType: string, actorId: string): string {
    return `${actorType}\u001f${actorId}`
}

async function responseDocument(response: Response): Promise<unknown> {
    try {
        return (await response.json()) as unknown
    } catch (error) {
        throw new ActorProtocolError(`control-plane HTTP ${response.status} response was not valid JSON`, { cause: error })
    }
}

interface RemoteActorSettings {
    readonly token: string
    readonly namespaceId: string
    readonly controlPlaneUrl: string
    readonly socketGatewayUrl: string
}

interface DurableObjectsClientOptions {
    readonly token: string
    readonly namespaceId: string
    readonly controlPlaneUrl: string
    readonly socketGatewayUrl?: string
}

interface RemoteActorClientDependencies {
    readonly environment?: NodeJS.ProcessEnv
    readonly fetch?: typeof globalThis.fetch
    readonly requestId?: () => string
    readonly actorHost?: ActorHostTransport
    readonly now?: () => number
    readonly monotonicNow?: () => number
    readonly telemetry?: TelemetrySink
    readonly connectWebSocket?: WebSocketConnector
}

type WebSocketConnector = (url: string, token: string, metadata: JsonValue) => Promise<ActorConnection>

export { RemoteActorClient }
export type { DurableObjectsClientOptions, RemoteActorClientDependencies }
