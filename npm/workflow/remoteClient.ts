import { z } from "zod"

import { ActorConfigurationError, ActorInvocationError, ActorProtocolError } from "../shared/errors.js"
import { currentActorInvocation } from "../shared/invocationContext.js"
import { JsonActorStateSerializer, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

import { GrpcActorHostTransport } from "./actorHostGrpc.js"
import type { ActorHostTarget, ActorHostTransport, DirectActorInvocation } from "./actorHostGrpc.js"

const namespaceIdSchema = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const remoteSettingsSchema = z.object({
    DURABLE_OBJECT_TOKEN: z.string().trim().min(1),
    DURABLE_OBJECT_NAMESPACE_ID: namespaceIdSchema,
    DURABLE_OBJECT_CONTROL_PLANE_URL: z.string().url()
})

const clientOptionsSchema = z.object({
    token: z.string().trim().min(1),
    namespaceId: namespaceIdSchema,
    controlPlaneUrl: z.string().url()
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
    private readonly targets = new Map<string, Promise<ActorHostTarget | undefined>>()

    constructor(options?: DurableObjectsClientOptions, dependencies: RemoteActorClientDependencies = {}) {
        this.environment = dependencies.environment ?? process.env
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch
        this.requestId = dependencies.requestId ?? (() => globalThis.crypto.randomUUID())
        this.actorHost = dependencies.actorHost ?? new GrpcActorHostTransport()
        this.now = dependencies.now ?? Date.now
        this.settingsValue = options === undefined ? undefined : configuredSettings(options)
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        const requestId = validateActorComponent("request ID", this.requestId())
        if (currentActorInvocation() !== undefined) {
            throw new ActorInvocationError("actor_error", requestId, "actor-to-actor calls are not available")
        }
        const invocation = this.invocation(requestId, actorType, actorId, method, args)
        const target = await this.target(invocation)
        if (!target) return this.proxied(invocation)
        return this.direct(target, invocation, true)
    }

    private async direct(target: ActorHostTarget, invocation: DirectActorInvocation, retryReroute: boolean): Promise<unknown> {
        try {
            const reply = await this.actorHost.invoke(target, invocation)
            if (reply.type === "completed") return reply.result
            if (reply.type === "failed") throw new ActorInvocationError(reply.code, invocation.requestId, reply.message)
            if (!retryReroute) throw new ActorInvocationError("unavailable", invocation.requestId, "actor ownership changed repeatedly before execution")
            this.targets.delete(actorKey(invocation.actorType, invocation.actorId))
            const rerouted = await this.target(invocation)
            if (!rerouted) return this.proxied(invocation)
            return this.direct(rerouted, invocation, false)
        } catch (error) {
            if (error instanceof ActorInvocationError || error instanceof ActorProtocolError) throw error
            this.targets.delete(actorKey(invocation.actorType, invocation.actorId))
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", invocation.requestId, `actor-host gRPC request failed after dispatch: ${message}`)
        }
    }

    private async target(invocation: DirectActorInvocation): Promise<ActorHostTarget | undefined> {
        const key = actorKey(invocation.actorType, invocation.actorId)
        const current = this.targets.get(key)
        if (current) {
            const target = await current
            if (!target) {
                this.targets.delete(key)
                return undefined
            }
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

    private async resolveTarget(invocation: DirectActorInvocation): Promise<ActorHostTarget | undefined> {
        let response: Response
        try {
            response = await this.fetchRequest(targetUrl(this.settings, invocation.actorType, invocation.actorId), {
                method: "POST",
                headers: { accept: "application/json", authorization: `Bearer ${this.settings.token}` }
            })
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", invocation.requestId, `control-plane HTTP request failed before dispatch: ${message}`)
        }
        if (response.status === 404 || response.status === 405) return undefined
        const document = await responseDocument(response)
        if (!response.ok) this.throwResponseFailure(response, document, invocation.requestId)
        const target = actorHostTargetSchema.safeParse(document)
        if (!target.success) throw new ActorProtocolError("control-plane response did not contain a valid actor host target")
        return target.data
    }

    private async proxied(invocation: DirectActorInvocation): Promise<unknown> {
        let response: Response
        try {
            response = await this.fetchRequest(invocationUrl(this.settings, invocation.actorType, invocation.actorId), {
                method: "POST",
                headers: {
                    accept: "application/json",
                    authorization: `Bearer ${this.settings.token}`,
                    "content-type": "application/json"
                },
                body: JSON.stringify({ requestId: invocation.requestId, method: invocation.method, args: invocation.args })
            })
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", invocation.requestId, `control-plane HTTP request failed after dispatch: ${message}`)
        }
        const document = await responseDocument(response)
        if (response.ok) {
            if (isObject(document) && Object.hasOwn(document, "result")) return document.result
            throw new ActorProtocolError("control-plane response did not contain an actor result")
        }
        this.throwResponseFailure(response, document, invocation.requestId)
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
            controlPlaneUrl: validateOrigin(result.data.DURABLE_OBJECT_CONTROL_PLANE_URL)
        }
        return this.settingsValue
    }

    private jsonArguments(args: readonly unknown[]): readonly JsonValue[] {
        const value = this.serializer.clone(args, "actor arguments")
        if (!Array.isArray(value)) throw new ActorProtocolError("actor arguments must be a JSON array")
        return value
    }
}

function configuredSettings(options: DurableObjectsClientOptions): RemoteActorSettings {
    const result = clientOptionsSchema.safeParse(options)
    if (!result.success) throw new ActorConfigurationError(`durable-object client settings are invalid: ${result.error.message}`)
    return { ...result.data, controlPlaneUrl: validateOrigin(result.data.controlPlaneUrl) }
}

function validateOrigin(origin: string): string {
    let url: URL
    try {
        url = new URL(origin)
    } catch (error) {
        throw new ActorConfigurationError(`actor HTTP origin is invalid: ${origin}`, { cause: error })
    }
    if (!/^https?:$/u.test(url.protocol) || !url.hostname || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
        throw new ActorConfigurationError(`actor control-plane URL must be an HTTP or HTTPS origin: ${origin}`)
    }
    return url.origin
}

function invocationUrl(settings: RemoteActorSettings, actorType: string, actorId: string): string {
    const actor = validateActorComponent("actor type", actorType)
    const id = validateActorComponent("actor ID", actorId)
    return `${settings.controlPlaneUrl}/v1/namespaces/${encodeURIComponent(settings.namespaceId)}/actors/${encodeURIComponent(actor)}/${encodeURIComponent(id)}/invocations`
}

function targetUrl(settings: RemoteActorSettings, actorType: string, actorId: string): string {
    const actor = validateActorComponent("actor type", actorType)
    const id = validateActorComponent("actor ID", actorId)
    return `${settings.controlPlaneUrl}/v1/namespaces/${encodeURIComponent(settings.namespaceId)}/actors/${encodeURIComponent(actor)}/${encodeURIComponent(id)}/target`
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

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

interface RemoteActorSettings {
    readonly token: string
    readonly namespaceId: string
    readonly controlPlaneUrl: string
}

interface DurableObjectsClientOptions {
    readonly token: string
    readonly namespaceId: string
    readonly controlPlaneUrl: string
}

interface RemoteActorClientDependencies {
    readonly environment?: NodeJS.ProcessEnv
    readonly fetch?: typeof globalThis.fetch
    readonly requestId?: () => string
    readonly actorHost?: ActorHostTransport
    readonly now?: () => number
}

export { RemoteActorClient }
export type { DurableObjectsClientOptions, RemoteActorClientDependencies }
