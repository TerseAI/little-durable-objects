import type { ActorDefinition } from "../../shared/actor.js"
import { Actor, bindActorIdentity } from "../../shared/actor.js"
import { ActorProtocolError } from "../../shared/errors.js"
import { runInActorInvocation } from "../../shared/invocationContext.js"
import type { ActorInvocationContext } from "../../shared/invocationContext.js"
import { JsonActorStateSerializer, errorMessage, failedReply } from "../../shared/types.js"
import type { ActorExecutorCommand, ActorExecutorReply, CancelCommand, InvokeCommand, JsonObject, JsonValue } from "../../shared/types.js"

class ActorRuntime {
    private readonly serializer = new JsonActorStateSerializer()
    private readonly activeInvocations = new Map<string, ActiveInvocation>()

    constructor(private readonly definition: ActorDefinition) {}

    async handle(command: ActorExecutorCommand): Promise<ActorExecutorReply> {
        switch (command.type) {
            case "invoke":
                return this.invoke(command)
            case "cancel":
                return this.cancel(command)
            default:
                throw command satisfies never
        }
    }

    private async invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        const identity = identityFrom(command.actor)
        if (identity.actorType !== this.definition.actorType) {
            return failedReply("actor_type_not_found", `actor type ${identity.actorType} is not loaded in this customer process`)
        }

        const key = identityKey(identity)
        const instance = Reflect.construct(this.definition.actorClass, []) as Actor
        bindActorIdentity(instance, command.actor.actor_id)
        if (command.state !== null) {
            this.serializer.hydrate(instance, persistedState(command.state))
        }
        if (!this.definition.methods.has(command.method)) {
            return failedReply("method_not_found", `actor method ${this.definition.actorType}.${command.method} was not found`)
        }
        const method: unknown = Reflect.get(instance, command.method)
        if (typeof method !== "function") {
            return failedReply("method_not_callable", `actor method ${this.definition.actorType}.${command.method} is not callable`)
        }

        const controller = new AbortController()
        const context = {
            deadline: performance.now() + command.timeout_ms,
            signal: controller.signal
        }
        const active = { requestId: command.request_id, context, controller }
        this.activeInvocations.set(key, active)
        try {
            const rawResult: unknown = await runInActorInvocation(context, async () => Reflect.apply(method, instance, command.args) as Promise<unknown>)
            const result: JsonValue = rawResult === undefined ? null : this.serializer.clone(rawResult, "actor result")
            return {
                type: "invoked",
                result,
                state: this.serializer.snapshot(instance)
            }
        } catch (error) {
            return failedReply("actor_method_failed", errorMessage(error))
        } finally {
            if (this.activeInvocations.get(key) === active) this.activeInvocations.delete(key)
        }
    }

    private cancel(command: CancelCommand): ActorExecutorReply {
        const active = this.activeInvocations.get(identityKey(identityFrom(command.actor)))
        if (active?.requestId === command.request_id) {
            runInActorInvocation(active.context, () => active.controller.abort(new Error("actor invocation cancelled after its deadline expired")))
        }
        return { type: "cancelled" }
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

interface ActiveInvocation {
    readonly requestId: string
    readonly context: ActorInvocationContext
    readonly controller: AbortController
}

interface ActorIdentity {
    readonly namespaceId: string
    readonly actorType: string
    readonly actorId: string
}

export { ActorRuntime }
