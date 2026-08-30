import { stat } from "node:fs/promises"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { register } from "tsx/esm/api"

import type { ActorClass } from "../shared/actor.js"
import { Actor, registerActorClass } from "../shared/actor.js"
import { ActorConfigurationError, ActorDefinitionError } from "../shared/errors.js"

const DEFAULT_ACTOR_ENTRYPOINT = "src/durable-objects.ts"

async function resolveActorEntrypoint(configured: string | undefined): Promise<string> {
    const entrypointPath = path.resolve(configured ?? DEFAULT_ACTOR_ENTRYPOINT)
    await requireFile(entrypointPath, configured === undefined ? `default actor entrypoint ${DEFAULT_ACTOR_ENTRYPOINT}` : `configured actor entrypoint ${configured}`)
    return pathToFileURL(entrypointPath).href
}

async function loadActorEntrypoint(moduleUrl: string): Promise<string[]> {
    const unregister = register()
    let actorModule: Record<string, unknown>
    try {
        actorModule = (await import(moduleUrl)) as Record<string, unknown>
    } finally {
        await unregister()
    }
    const exports = Object.entries(actorModule)
    if (exports.length === 0) {
        throw new ActorDefinitionError(`actor entrypoint ${moduleUrl} has no named actor exports`)
    }
    const actorTypes = exports.map(([exportName, value]) => {
        if (exportName === "default") {
            throw new ActorDefinitionError("actor entrypoint must use named exports, not a default export")
        }
        if (typeof value !== "function" || value.prototype === undefined || Object.getPrototypeOf(value.prototype) !== Actor.prototype) {
            throw new ActorDefinitionError(`actor entrypoint export ${exportName} must be a class that directly extends Actor`)
        }
        if (value.name !== exportName) {
            throw new ActorDefinitionError(`actor entrypoint export ${exportName} must have the same class name`)
        }
        return registerActorClass(value as ActorClass).actorType
    })
    actorTypes.sort()
    return actorTypes
}

async function requireFile(filePath: string, label: string): Promise<void> {
    if (!(await isFile(filePath))) throw new ActorConfigurationError(`${label} is not a file`)
}

async function isFile(filePath: string): Promise<boolean> {
    try {
        return (await stat(filePath)).isFile()
    } catch {
        return false
    }
}

export { loadActorEntrypoint, resolveActorEntrypoint }
