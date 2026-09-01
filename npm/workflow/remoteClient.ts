import { z } from "zod"

import { ActorConfigurationError, ActorInvocationError, ActorProtocolError } from "../shared/errors.js"
import { currentActorInvocation } from "../shared/invocationContext.js"
import { JsonActorStateSerializer, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

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

class RemoteActorClient {
    private readonly serializer = new JsonActorStateSerializer()
    private settingsValue: RemoteActorSettings | undefined
    private readonly environment: NodeJS.ProcessEnv
    private readonly fetchRequest: typeof globalThis.fetch
    private readonly requestId: () => string

    constructor(options?: DurableObjectsClientOptions, dependencies: RemoteActorClientDependencies = {}) {
        this.environment = dependencies.environment ?? process.env
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch
        this.requestId = dependencies.requestId ?? (() => globalThis.crypto.randomUUID())
        this.settingsValue = options === undefined ? undefined : configuredSettings(options)
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        const requestId = validateActorComponent("request ID", this.requestId())
        if (currentActorInvocation() !== undefined) {
            throw new ActorInvocationError("actor_error", requestId, "actor-to-actor calls are not available")
        }
        const response = await this.send(requestId, actorType, actorId, method, args)
        return this.result(response, requestId)
    }

    private async send(requestId: string, actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<Response> {
        try {
            return await this.fetchRequest(invocationUrl(this.settings, actorType, actorId), {
                method: "POST",
                headers: {
                    accept: "application/json",
                    authorization: `Bearer ${this.settings.token}`,
                    "content-type": "application/json"
                },
                body: JSON.stringify({
                    requestId,
                    method: validateActorComponent("actor method", method),
                    args: this.jsonArguments(args)
                })
            })
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError("outcome_unknown", requestId, `control-plane HTTP request failed after dispatch: ${message}`)
        }
    }

    private async result(response: Response, requestId: string): Promise<unknown> {
        const document = await responseDocument(response)
        if (response.ok) {
            if (isObject(document) && Object.hasOwn(document, "result")) return document.result
            throw new ActorProtocolError("control-plane response did not contain an actor result")
        }
        if (response.status === 401 || response.status === 403) {
            throw new ActorInvocationError("unauthenticated", requestId, "the durable-object workflow token was rejected")
        }
        const failure = errorDocumentSchema.safeParse(document)
        if (!failure.success) {
            throw new ActorProtocolError(`control-plane HTTP ${response.status} response did not contain a valid error`)
        }
        throw new ActorInvocationError(failure.data.error.code, failure.data.error.requestId ?? requestId, failure.data.error.message)
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
}

export { RemoteActorClient }
export type { DurableObjectsClientOptions, RemoteActorClientDependencies }
