import { ActorConfigurationError, ActorInvocationError, ActorProtocolError } from "../shared/errors.js"
import { currentActorInvocation } from "../shared/invocationContext.js"
import { JsonActorStateSerializer, MAX_ACTOR_INVOCATION_TIMEOUT_MS, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

import { RemoteActorClient } from "./remoteClient.js"
import type { DurableObjectsClientOptions } from "./remoteClient.js"

interface ActorClientTransport {
    invoke(actorType: string, actorId: string, method: string, args: readonly unknown[], timeoutMs?: number): Promise<unknown>
}

function actorClient(): ActorClientTransport {
    return testActorClient ?? RemoteActorClient.getInstance()
}

function configureDurableObjects(options: DurableObjectsClientOptions): void {
    RemoteActorClient.configure(options)
}

let testActorClient: TestActorClient | undefined

class TestActorClient implements ActorClientTransport {
    private readonly serializer = new JsonActorStateSerializer()
    private readonly requestId: () => string
    private readonly invokeTest: ActorTestInvoker
    private readonly invocationTimeoutMs: number

    constructor(options: ActorTestClientOptions) {
        this.requestId = options.requestId ?? (() => globalThis.crypto.randomUUID())
        this.invokeTest = options.invoke
        this.invocationTimeoutMs = validateConfiguredInvocationTimeout(options.invocationTimeoutMs) ?? 30_000
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[], timeoutMs?: number): Promise<unknown> {
        const requestId = validateActorComponent("request ID", this.requestId())
        const callerTimeoutMs = timeoutMs ?? this.invocationTimeoutMs
        const parent = currentActorInvocation()
        const deadline = Math.min(performance.now() + callerTimeoutMs, parent?.deadline ?? Number.POSITIVE_INFINITY)
        const remainingTimeoutMs = requireRemainingTimeoutMs(deadline, requestId)
        const serializedArgs = this.serializer.clone(args, "actor arguments")
        if (!Array.isArray(serializedArgs)) throw new ActorProtocolError("actor arguments must be a JSON array")
        const request: ActorInvocationRequest = {
            requestId,
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            method: validateActorComponent("actor method", method),
            args: serializedArgs,
            timeoutMs: remainingTimeoutMs
        }
        return withCallerDeadline(this.invokeTest(request), requestId, remainingTimeoutMs)
    }
}

function configureActorClientForTests(options?: ActorTestClientOptions): void {
    testActorClient = options === undefined ? undefined : new TestActorClient(options)
}

function requireRemainingTimeoutMs(deadline: number, requestId: string): number {
    const remainingMs = Math.ceil(deadline - performance.now())
    if (remainingMs <= 0) throw deadlineExceeded(requestId)
    return remainingMs
}

function deadlineExceeded(requestId: string): ActorInvocationError {
    return new ActorInvocationError("deadline_exceeded", requestId, "actor invocation deadline exceeded; execution may still complete")
}

function withCallerDeadline<T>(operation: Promise<T>, requestId: string, timeoutMs: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
        const timeout = setTimeout(() => reject(deadlineExceeded(requestId)), timeoutMs)
        void operation.then(
            value => {
                clearTimeout(timeout)
                resolve(value)
            },
            error => {
                clearTimeout(timeout)
                reject(error)
            }
        )
    })
}

function validateConfiguredInvocationTimeout(timeoutMs: number | undefined): number | undefined {
    if (timeoutMs === undefined) return undefined
    if (!Number.isInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > MAX_ACTOR_INVOCATION_TIMEOUT_MS) {
        throw new ActorConfigurationError(`actor invocation timeout must be an integer between 1 and ${MAX_ACTOR_INVOCATION_TIMEOUT_MS}ms`)
    }
    return timeoutMs
}

interface ActorInvocationRequest {
    readonly requestId: string
    readonly actorType: string
    readonly actorId: string
    readonly method: string
    readonly args: readonly JsonValue[]
    readonly timeoutMs: number
}

interface ActorTestClientOptions {
    readonly requestId?: () => string
    readonly invoke: ActorTestInvoker
    readonly invocationTimeoutMs?: number
}

type ActorTestInvoker = (request: ActorInvocationRequest) => Promise<unknown>

export { actorClient, configureActorClientForTests, configureDurableObjects }
export type { ActorInvocationRequest, ActorTestClientOptions, DurableObjectsClientOptions }
