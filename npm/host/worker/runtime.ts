import type { ActorDefinition } from "../../shared/actor.js"
import { Actor, bindActorIdentity } from "../../shared/actor.js"
import { ActorProtocolError } from "../../shared/errors.js"
import { runInActorInvocation } from "../../shared/invocationContext.js"
import { decodeSocketMessage, runWithActorSockets } from "../../shared/socket.js"
import { JsonActorStateSerializer, errorMessage, failedReply } from "../../shared/types.js"
import type { ActorExecutorCommand, ActorExecutorReply, InvokeCommand, JsonObject, JsonValue, SocketEffect, WebSocketEventCommand } from "../../shared/types.js"

class ActorRuntime {
    private readonly serializer = new JsonActorStateSerializer()
    private instance: Actor | undefined
    private identity: ActorIdentity | undefined

    constructor(private readonly definition: ActorDefinition) {}

    async handle(command: InvokeCommand | WebSocketEventCommand): Promise<ActorExecutorReply> {
        return command.type === "invoke" ? this.invoke(command) : this.handleSocketEvent(command)
    }

    private async invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        const prepared = this.prepare(command)
        if (!(prepared instanceof Actor)) return prepared
        const instance = prepared
        if (!this.definition.methods.has(command.method)) {
            return failedReply("method_not_found", `actor method ${this.definition.actorType}.${command.method} was not found`)
        }
        const method: unknown = Reflect.get(instance, command.method)
        if (typeof method !== "function") {
            return failedReply("method_not_callable", `actor method ${this.definition.actorType}.${command.method} is not callable`)
        }

        try {
            const operation = await runWithActorSockets(instance, command.connections ?? [], async () =>
                runInActorInvocation(async () => Reflect.apply(method, instance, command.args) as Promise<unknown>)
            )
            const result: JsonValue = operation.value === undefined ? null : this.serializer.clone(operation.value, "actor result")
            return {
                type: "invoked",
                result,
                state: this.serializer.snapshot(instance),
                ...(operation.effects.length === 0 ? {} : { effects: operation.effects })
            }
        } catch (error) {
            this.reset()
            return failedReply("actor_method_failed", errorMessage(error))
        }
    }

    private async handleSocketEvent(command: WebSocketEventCommand): Promise<ActorExecutorReply> {
        const prepared = this.prepare(command)
        if (!(prepared instanceof Actor)) return prepared
        const instance = prepared
        const methodName = lifecycleMethod(command)
        const method: unknown = Reflect.get(instance, methodName)
        try {
            const operation = await runWithActorSockets(instance, command.connections, async scope => {
                if (method === undefined) return
                if (typeof method !== "function") throw new ActorProtocolError(`actor lifecycle hook ${this.definition.actorType}.${methodName} is not callable`)
                const args = lifecycleArguments(command, scope)
                await runInActorInvocation(async () => Reflect.apply(method, instance, args) as Promise<unknown>)
            })
            const state = this.serializer.snapshot(instance)
            return { type: "websocket_handled", state, effects: socketEffects(command, state, operation.effects) }
        } catch (error) {
            this.reset()
            return failedReply("actor_socket_failed", errorMessage(error))
        }
    }

    private prepare(command: InvokeCommand | WebSocketEventCommand): Actor | ActorExecutorReply {
        const identity = identityFrom(command.actor)
        if (identity.actorType !== this.definition.actorType) {
            return failedReply("actor_type_not_found", `actor type ${identity.actorType} is not loaded in this customer process`)
        }
        if (this.identity !== undefined && identityKey(this.identity) !== identityKey(identity)) {
            return failedReply("actor_identity_mismatch", "resident actor Worker received an invocation for a different actor")
        }
        return this.instance ?? this.createInstance(identity, command.state)
    }

    private reset(): void {
        this.instance = undefined
        this.identity = undefined
    }

    private createInstance(identity: ActorIdentity, state: JsonValue | null): Actor {
        const instance = Reflect.construct(this.definition.actorClass, []) as Actor
        bindActorIdentity(instance, identity.actorId)
        if (state !== null) this.serializer.hydrate(instance, persistedState(state))
        this.identity = identity
        this.instance = instance
        return instance
    }
}

function socketEffects(command: WebSocketEventCommand, state: JsonObject, effects: readonly SocketEffect[]): readonly SocketEffect[] {
    if (command.event.type !== "connect" || connectionWasRejected(command.event.connection.id, effects)) return effects
    return [
        ...effects,
        {
            type: "send",
            connection_id: command.event.connection.id,
            message: { type: "text", data: JSON.stringify({ type: "state", state }) }
        }
    ]
}

function connectionWasRejected(connectionId: string, effects: readonly SocketEffect[]): boolean {
    return effects.some(effect => effect.type === "reject" && effect.connection_id === connectionId)
}

function lifecycleMethod(command: WebSocketEventCommand): "onConnect" | "onMessage" | "onDisconnect" {
    switch (command.event.type) {
        case "connect":
            return "onConnect"
        case "message":
            return "onMessage"
        case "disconnect":
            return "onDisconnect"
    }
}

function lifecycleArguments(command: WebSocketEventCommand, scope: Parameters<Parameters<typeof runWithActorSockets>[2]>[0]): readonly unknown[] {
    switch (command.event.type) {
        case "connect":
            return [scope.eventSocket(command.event.connection, "connecting")]
        case "message":
            return [scope.connection(command.event.connection_id), decodeSocketMessage(command.event.message)]
        case "disconnect":
            return [scope.eventSocket(command.event.connection, "closed"), command.event.code, command.event.reason, command.event.was_clean]
    }
}

function persistedState(value: JsonValue): JsonObject {
    if (!isJsonObject(value)) {
        throw new ActorProtocolError("persisted actor state must be a JSON object")
    }
    return value
}

function isJsonObject(value: JsonValue): value is JsonObject {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

function identityFrom(value: ActorExecutorCommand["actor"]): ActorIdentity {
    return {
        namespaceId: value.namespace_id,
        actorType: value.actor_type,
        actorId: value.actor_id
    }
}

function identityKey(value: ActorIdentity): string {
    return `${value.namespaceId}\u001f${value.actorType}\u001f${value.actorId}`
}

interface ActorIdentity {
    readonly namespaceId: string
    readonly actorType: string
    readonly actorId: string
}

export { ActorRuntime }
