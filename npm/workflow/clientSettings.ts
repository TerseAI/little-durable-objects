import { z } from "zod"

import { ActorConfigurationError } from "../shared/errors.js"

import type { DurableObjectsClientOptions } from "./remoteClient.js"

const clientOptionsSchema = z.object({
    token: z.string().trim().min(1),
    namespaceId: z.string().regex(/^[A-Za-z0-9._-]+$/u),
    controlPlaneUrl: z.string().url(),
    socketGatewayUrl: z.string().url().optional()
})

function configuredSettings(options: DurableObjectsClientOptions) {
    const result = clientOptionsSchema.safeParse(options)
    if (!result.success) throw new ActorConfigurationError(`durable-object client settings are invalid: ${result.error.message}`)
    const controlPlaneUrl = validateOrigin(result.data.controlPlaneUrl)
    return { ...result.data, controlPlaneUrl, socketGatewayUrl: validateOrigin(result.data.socketGatewayUrl ?? controlPlaneUrl) }
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

export { configuredSettings, validateOrigin }
