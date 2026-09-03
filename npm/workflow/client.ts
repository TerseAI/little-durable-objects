import { AsyncLocalStorage } from "node:async_hooks"

import { ActorProtocolError } from "../shared/errors.js"
import type { ActorConnection } from "../shared/socket.js"
import { JsonActorStateSerializer, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

import { RemoteActorClient } from "./remoteClient.js"
import type { DurableObjectsClientOptions } from "./remoteClient.js"

interface ActorClientTransport {
    invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>
    connect(actorType: string, actorId: string, metadata: unknown): Promise<ActorConnection>
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
    private readonly connectTest: ActorTestConnector

    constructor(options: ActorTestClientOptions) {
        this.requestId = options.requestId ?? (() => globalThis.crypto.randomUUID())
        this.invokeTest = options.invoke
        this.connectTest = options.connect ?? (() => Promise.reject(new ActorProtocolError("socket connections are not configured for this test")))
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

    async connect(actorType: string, actorId: string, metadata: unknown): Promise<ActorConnection> {
        const request: ActorConnectionRequest = {
            requestId: validateActorComponent("request ID", this.requestId()),
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            metadata: this.serializer.clone(metadata, "socket metadata")
        }
        return this.connectTest(request)
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
    readonly connect?: ActorTestConnector
}

type ActorTestInvoker = (request: ActorInvocationRequest) => Promise<unknown>
type ActorTestConnector = (request: ActorConnectionRequest) => Promise<ActorConnection>

interface ActorConnectionRequest {
    readonly requestId: string
    readonly actorType: string
    readonly actorId: string
    readonly metadata: JsonValue
}

export { actorClient, configureDurableObjects, runWithActorClientForTests }
export type { ActorConnectionRequest, ActorInvocationRequest, ActorTestClientOptions, DurableObjectsClientOptions }
