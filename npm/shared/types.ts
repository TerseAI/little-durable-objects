import { z } from "zod"

import { ActorProtocolError, ActorSerializationError, ActorValidationError } from "./errors.js"

const MAX_ACTOR_INVOCATION_TIMEOUT_MS = 2_147_483_647

type JsonPrimitive = string | number | boolean | null
type JsonObject = { readonly [key: string]: JsonValue }
type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[]

const actorComponentSchema = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() => z.union([z.string(), z.number(), z.boolean(), z.null(), z.array(jsonValueSchema), z.record(z.string(), jsonValueSchema)]))

const actorIdentitySchema = z.object({
    namespace_id: actorComponentSchema,
    actor_type: actorComponentSchema,
    actor_id: actorComponentSchema
})

const invokeCommandSchema = z.object({
    type: z.literal("invoke"),
    request_id: actorComponentSchema,
    actor: actorIdentitySchema,
    method: actorComponentSchema,
    args: z.array(jsonValueSchema),
    state: jsonValueSchema.nullable(),
    timeout_ms: z.number().int().positive().max(MAX_ACTOR_INVOCATION_TIMEOUT_MS)
})

const cancelCommandSchema = z.object({
    type: z.literal("cancel"),
    request_id: actorComponentSchema,
    actor: actorIdentitySchema
})

const executorCommandSchema = z.discriminatedUnion("type", [invokeCommandSchema, cancelCommandSchema])

const actorSessionServerMessageSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("attached"), protocol: z.literal(8) }),
    z.object({
        type: z.literal("command"),
        message_id: z.number().int().nonnegative(),
        command: executorCommandSchema
    })
])

class JsonActorStateSerializer {
    clone(value: unknown, label: string): JsonValue {
        const encoded = this.encode(value, label)
        const decoded: unknown = JSON.parse(encoded)
        const result = jsonValueSchema.safeParse(decoded)
        if (!result.success) throw new ActorSerializationError(`${label} must be JSON serializable`)
        return result.data
    }

    snapshot(instance: object): JsonObject {
        const state = Object.fromEntries(Object.keys(instance).map(key => [key, Reflect.get(instance, key)]))
        return this.cloneObject(state, "actor state")
    }

    hydrate(instance: object, state: JsonObject): void {
        const restored = this.cloneObject(state, "actor state")
        Object.keys(instance).forEach(key => {
            if (!Reflect.deleteProperty(instance, key)) throw new ActorSerializationError(`actor field ${key} cannot be restored`)
        })
        Object.entries(restored).forEach(([key, value]) => {
            Object.defineProperty(instance, key, {
                configurable: true,
                enumerable: true,
                writable: true,
                value
            })
        })
    }

    private cloneObject(value: unknown, label: string): JsonObject {
        const cloned = this.clone(value, label)
        if (!isJsonObject(cloned)) throw new ActorSerializationError(`${label} must be a JSON object`)
        return cloned
    }

    private encode(value: unknown, label: string): string {
        try {
            const encoded = JSON.stringify(value)
            if (encoded === undefined) throw new ActorSerializationError(`${label} must be JSON serializable`)
            return encoded
        } catch (error) {
            if (error instanceof ActorSerializationError) throw error
            throw new ActorSerializationError(`${label} must be JSON serializable`, { cause: error })
        }
    }
}

function validateActorComponent(name: string, value: string): string {
    const result = actorComponentSchema.safeParse(value)
    if (!result.success) throw new ActorValidationError(`${name} may contain only ASCII letters, digits, '.', '-', and '_'`)
    return result.data
}

function parseActorSessionServerMessage(document: string): ActorSessionServerMessage {
    let value: unknown
    try {
        value = JSON.parse(document)
    } catch (error) {
        throw new ActorProtocolError("actor session message is not valid JSON", { cause: error })
    }
    const result = actorSessionServerMessageSchema.safeParse(value)
    if (!result.success) throw new ActorProtocolError(`actor session message is invalid: ${result.error.message}`)
    return result.data
}

function failedReply(code: string, message: string): FailedReply {
    return { type: "failed", code, message }
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error)
}

function isJsonObject(value: JsonValue): value is JsonObject {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

type InvokeCommand = z.infer<typeof invokeCommandSchema>
type CancelCommand = z.infer<typeof cancelCommandSchema>
type ActorExecutorCommand = z.infer<typeof executorCommandSchema>
type ActorSessionServerMessage = z.infer<typeof actorSessionServerMessageSchema>
type ActorExecutorReply = InvokedReply | FailedReply | CancelledReply
type ActorSessionClientMessage = AttachMessage | ReplyMessage

interface AttachMessage {
    readonly type: "attach"
    readonly protocol: 8
    readonly actor_types: readonly string[]
}

interface ReplyMessage {
    readonly type: "reply"
    readonly message_id: number
    readonly reply: ActorExecutorReply
}

interface InvokedReply {
    readonly type: "invoked"
    readonly result: JsonValue
    readonly state: JsonObject
}

interface FailedReply {
    readonly type: "failed"
    readonly code: string
    readonly message: string
}

interface CancelledReply {
    readonly type: "cancelled"
}

interface ActorWorkerData {
    readonly actorType: string
    readonly moduleUrl: string
}

type ActorWorkerRequest = { readonly type: "invoke"; readonly command: InvokeCommand } | { readonly type: "cancel"; readonly command: CancelCommand }

export { JsonActorStateSerializer, MAX_ACTOR_INVOCATION_TIMEOUT_MS, errorMessage, failedReply, parseActorSessionServerMessage, validateActorComponent }
export type {
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorSessionClientMessage,
    ActorSessionServerMessage,
    ActorWorkerData,
    ActorWorkerRequest,
    CancelCommand,
    InvokeCommand,
    JsonObject,
    JsonPrimitive,
    JsonValue
}
