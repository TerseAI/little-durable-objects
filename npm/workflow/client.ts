import { AsyncLocalStorage } from "node:async_hooks"

import { ActorProtocolError } from "../shared/errors.js"
import { JsonActorStateSerializer, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

import { RemoteActorClient } from "./remoteClient.js"
import type { DurableObjectsClientOptions } from "./remoteClient.js"

interface ActorClientTransport {
    invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>
}

const scopedClients = new AsyncLocalStorage<ActorClientTransport>()

function actorClient(): ActorClientTransport {
    return scopedClients.getStore() ?? defaultClients.get()
}

function configureDurableObjects(options: DurableObjectsClientOptions): void {
    defaultClients.configure(options)
}

function runWithActorClientForTests<T>(options: ActorTestClientOptions, operation: () => T): T {
    return scopedClients.run(new TestActorClient(options), operation)
}

class DefaultActorClientProvider {
    private client: ActorClientTransport = new RemoteActorClient()

    get(): ActorClientTransport {
        return this.client
    }

    configure(options: DurableObjectsClientOptions): void {
        this.client = new RemoteActorClient(options)
    }
}

const defaultClients = new DefaultActorClientProvider()

class TestActorClient implements ActorClientTransport {
    private readonly serializer = new JsonActorStateSerializer()
    private readonly requestId: () => string
    private readonly invokeTest: ActorTestInvoker

    constructor(options: ActorTestClientOptions) {
        this.requestId = options.requestId ?? (() => globalThis.crypto.randomUUID())
        this.invokeTest = options.invoke
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        const requestId = validateActorComponent("request ID", this.requestId())
        const serializedArgs = this.serializer.clone(args, "actor arguments")
        if (!Array.isArray(serializedArgs)) throw new ActorProtocolError("actor arguments must be a JSON array")
        const request: ActorInvocationRequest = {
            requestId,
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            method: validateActorComponent("actor method", method),
            args: serializedArgs
        }
        return this.invokeTest(request)
    }
}

interface ActorInvocationRequest {
    readonly requestId: string
    readonly actorType: string
    readonly actorId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}

interface ActorTestClientOptions {
    readonly requestId?: () => string
    readonly invoke: ActorTestInvoker
}

type ActorTestInvoker = (request: ActorInvocationRequest) => Promise<unknown>

export { actorClient, configureDurableObjects, runWithActorClientForTests }
export type { ActorInvocationRequest, ActorTestClientOptions, DurableObjectsClientOptions }
