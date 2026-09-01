import { AsyncLocalStorage } from "node:async_hooks"

const invocationStorage = new AsyncLocalStorage<true>()

function runInActorInvocation<T>(operation: () => T): T {
    return invocationStorage.run(true, operation)
}

function currentActorInvocation(): true | undefined {
    return invocationStorage.getStore()
}

export { currentActorInvocation, runInActorInvocation }
