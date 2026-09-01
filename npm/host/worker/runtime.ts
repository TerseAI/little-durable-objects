import type { ActorDefinition } from "../../shared/actor.js"
import { Actor, bindActorIdentity } from "../../shared/actor.js"
import { ActorProtocolError } from "../../shared/errors.js"
import { runInActorInvocation } from "../../shared/invocationContext.js"
import { JsonActorStateSerializer, errorMessage, failedReply } from "../../shared/types.js"
import type { ActorExecutorReply, InvokeCommand, JsonObject, JsonValue } from "../../shared/types.js"

class ActorRuntime {
    private readonly serializer = new JsonActorStateSerializer()
    private instance: Actor | undefined
    private identity: ActorIdentity | undefined

    constructor(private readonly definition: ActorDefinition) {}

    async handle(command: InvokeCommand): Promise<ActorExecutorReply> {
        return this.invoke(command)
    }

    private async invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        const identity = identityFrom(command.actor)
        if (identity.actorType !== this.definition.actorType) {
            return failedReply("actor_type_not_found", `actor type ${identity.actorType} is not loaded in this customer process`)
        }
        if (this.identity !== undefined && identityKey(this.identity) !== identityKey(identity)) {
            return failedReply("actor_identity_mismatch", "resident actor Worker received an invocation for a different actor")
        }

        const instance = this.instance ?? this.createInstance(identity, command.state)
        if (!this.definition.methods.has(command.method)) {
            return failedReply("method_not_found", `actor method ${this.definition.actorType}.${command.method} was not found`)
        }
        const method: unknown = Reflect.get(instance, command.method)
        if (typeof method !== "function") {
            return failedReply("method_not_callable", `actor method ${this.definition.actorType}.${command.method} is not callable`)
        }

        try {
            const rawResult: unknown = await runInActorInvocation(async () => Reflect.apply(method, instance, command.args) as Promise<unknown>)
            const result: JsonValue = rawResult === undefined ? null : this.serializer.clone(rawResult, "actor result")
            return {
                type: "invoked",
                result,
                state: this.serializer.snapshot(instance)
            }
        } catch (error) {
            this.instance = undefined
            this.identity = undefined
            return failedReply("actor_method_failed", errorMessage(error))
        }
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

function persistedState(value: JsonValue): JsonObject {
    if (!isJsonObject(value)) {
        throw new ActorProtocolError("persisted actor state must be a JSON object")
    }
    return value
}

function isJsonObject(value: JsonValue): value is JsonObject {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

function identityFrom(value: InvokeCommand["actor"]): ActorIdentity {
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
