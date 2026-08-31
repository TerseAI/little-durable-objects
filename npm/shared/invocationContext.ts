import { AsyncLocalStorage } from "node:async_hooks"

const invocationStorage = new AsyncLocalStorage<ActorInvocationContext>()

function runInActorInvocation<T>(context: ActorInvocationContext, operation: () => T): T {
    return invocationStorage.run(context, operation)
}

function currentActorInvocation(): ActorInvocationContext | undefined {
    return invocationStorage.getStore()
}

interface ActorInvocationContext {
    readonly deadline: number
}

export { currentActorInvocation, runInActorInvocation }
export type { ActorInvocationContext }
