import { z } from "zod"

import { ActorProtocolError, ActorSerializationError, ActorValidationError } from "./errors.js"

type JsonPrimitive = string | number | boolean | null
type JsonObject = { readonly [key: string]: JsonValue }
type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[]

const actorComponentSchema = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() => z.union([z.string(), z.number(), z.boolean(), z.null(), z.array(jsonValueSchema), z.record(z.string(), jsonValueSchema)]))
const socketConnectionIdSchema = actorComponentSchema.max(128)
const socketMetadataSchema = jsonValueSchema.refine(value => Buffer.byteLength(JSON.stringify(value)) <= 64 * 1024, "socket metadata must not exceed 64 KiB")
const socketTagSchema = z.string().min(1).max(256)
const socketTagsSchema = z
    .array(socketTagSchema)
    .max(128)
    .refine(tags => tags.reduce((bytes, tag) => bytes + Buffer.byteLength(tag), 0) <= 8 * 1024, "socket tags must not exceed 8 KiB")
const socketTextSchema = z.string().refine(value => Buffer.byteLength(value) <= 16 * 1024 * 1024, "socket message must not exceed 16 MiB")
const socketBinarySchema = z
    .string()
    .regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u, "socket binary message must be base64")
    .refine(value => Buffer.byteLength(value, "base64") <= 16 * 1024 * 1024, "socket message must not exceed 16 MiB")
const socketCloseCodeSchema = z
    .number()
    .int()
    .refine(code => code === 1000 || (code >= 3000 && code <= 4999), "socket close code must be 1000 or between 3000 and 4999")
const socketCloseReasonSchema = z.string().refine(reason => Buffer.byteLength(reason) <= 123, "socket close reason must not exceed 123 bytes")

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
    connections: z.array(z.lazy(() => socketConnectionSchema)).optional()
})

const socketConnectionSchema = z.object({
    id: socketConnectionIdSchema,
    metadata: socketMetadataSchema,
    tags: socketTagsSchema
})

const socketMessageSchema = z.discriminatedUnion("type", [z.object({ type: z.literal("text"), data: socketTextSchema }), z.object({ type: z.literal("binary"), data: socketBinarySchema })])

const socketEffectSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("send"), connection_id: socketConnectionIdSchema, message: socketMessageSchema }),
    z.object({
        type: z.literal("broadcast"),
        message: socketMessageSchema,
        except_connection_ids: z.array(socketConnectionIdSchema).max(128),
        tags: socketTagsSchema
    }),
    z.object({ type: z.literal("close"), connection_id: socketConnectionIdSchema, code: socketCloseCodeSchema, reason: socketCloseReasonSchema }),
    z.object({ type: z.literal("reject"), connection_id: socketConnectionIdSchema, code: socketCloseCodeSchema, reason: socketCloseReasonSchema }),
    z.object({ type: z.literal("set_metadata"), connection_id: socketConnectionIdSchema, metadata: socketMetadataSchema }),
    z.object({ type: z.literal("set_tags"), connection_id: socketConnectionIdSchema, tags: socketTagsSchema })
])

const socketEffectsSchema = z.array(socketEffectSchema)

const socketEventSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("connect"), connection: socketConnectionSchema }),
    z.object({ type: z.literal("message"), connection_id: actorComponentSchema, message: socketMessageSchema }),
    z.object({
        type: z.literal("disconnect"),
        connection: socketConnectionSchema,
        code: z.number().int().min(0).max(65535),
        reason: z.string(),
        was_clean: z.boolean()
    })
])

const websocketEventCommandSchema = z.object({
    type: z.literal("websocket_event"),
    request_id: actorComponentSchema,
    actor: actorIdentitySchema,
    event: socketEventSchema,
    connections: z.array(socketConnectionSchema),
    state: jsonValueSchema.nullable()
})

const evictCommandSchema = z.object({
    type: z.literal("evict"),
    actor: actorIdentitySchema
})

const executorCommandSchema = z.discriminatedUnion("type", [invokeCommandSchema, websocketEventCommandSchema, evictCommandSchema])

const actorSessionServerMessageSchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("attached"), protocol: z.literal(12) }),
    z.object({
        type: z.literal("command"),
        message_id: z.number().int().nonnegative(),
        command: executorCommandSchema
    })
])

class JsonActorStateSerializer {
    clone(value: unknown, label: string): JsonValue {
        return this.cloneValue(value, label)
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
        const cloned = this.cloneValue(value, label)
        if (!isJsonObject(cloned)) throw new ActorSerializationError(`${label} must be a JSON object`)
        return cloned
    }

    private cloneValue(value: unknown, label: string): JsonValue {
        const encoded = this.encode(value, label)
        const decoded: unknown = JSON.parse(encoded)
        const result = jsonValueSchema.safeParse(decoded)
        if (!result.success) throw new ActorSerializationError(`${label} must be JSON serializable`)
        return result.data
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

function parseSocketEffects(value: unknown): readonly SocketEffect[] {
    const result = socketEffectsSchema.safeParse(value)
    if (!result.success) throw new ActorProtocolError(`actor socket effects are invalid: ${result.error.message}`)
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
type EvictCommand = z.infer<typeof evictCommandSchema>
type ActorExecutorCommand = z.infer<typeof executorCommandSchema>
type ActorSessionServerMessage = z.infer<typeof actorSessionServerMessageSchema>
type ActorExecutorReply = InvokedReply | WebSocketHandledReply | FailedReply | EvictedReply
type ActorSessionClientMessage = AttachMessage | ReplyMessage

interface AttachMessage {
    readonly type: "attach"
    readonly protocol: 12
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
    readonly effects?: readonly SocketEffect[]
}

interface WebSocketHandledReply {
    readonly type: "websocket_handled"
    readonly state: JsonObject
    readonly effects: readonly SocketEffect[]
}

interface FailedReply {
    readonly type: "failed"
    readonly code: string
    readonly message: string
}

interface EvictedReply {
    readonly type: "evicted"
}

interface ActorWorkerData {
    readonly actorType: string
    readonly moduleUrl: string
}

type ActorWorkerRequest = { readonly type: "execute"; readonly command: InvokeCommand | WebSocketEventCommand }

type SocketConnection = z.infer<typeof socketConnectionSchema>
type SocketMessage = z.infer<typeof socketMessageSchema>
type SocketEvent = z.infer<typeof socketEventSchema>
type WebSocketEventCommand = z.infer<typeof websocketEventCommandSchema>
type SocketEffect =
    | { readonly type: "send"; readonly connection_id: string; readonly message: SocketMessage }
    | { readonly type: "broadcast"; readonly message: SocketMessage; readonly except_connection_ids: readonly string[]; readonly tags: readonly string[] }
    | { readonly type: "close" | "reject"; readonly connection_id: string; readonly code: number; readonly reason: string }
    | { readonly type: "set_metadata"; readonly connection_id: string; readonly metadata: JsonValue }
    | { readonly type: "set_tags"; readonly connection_id: string; readonly tags: readonly string[] }

export { JsonActorStateSerializer, errorMessage, failedReply, parseActorSessionServerMessage, parseSocketEffects, validateActorComponent }
export type {
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorSessionClientMessage,
    ActorSessionServerMessage,
    ActorWorkerData,
    ActorWorkerRequest,
    EvictCommand,
    InvokeCommand,
    JsonObject,
    JsonPrimitive,
    JsonValue,
    SocketConnection,
    SocketEffect,
    SocketEvent,
    SocketMessage,
    WebSocketEventCommand
}
