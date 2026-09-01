import { ActorProtocolError } from "../shared/errors.js"
import { JsonActorStateSerializer, validateActorComponent } from "../shared/types.js"
import type { JsonValue } from "../shared/types.js"

import { RemoteActorClient } from "./remoteClient.js"
import type { DurableObjectsClientOptions } from "./remoteClient.js"

interface ActorClientTransport {
    invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>
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

function configureActorClientForTests(options?: ActorTestClientOptions): void {
    testActorClient = options === undefined ? undefined : new TestActorClient(options)
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

export { actorClient, configureActorClientForTests, configureDurableObjects }
export type { ActorInvocationRequest, ActorTestClientOptions, DurableObjectsClientOptions }
