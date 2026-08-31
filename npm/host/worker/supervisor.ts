import { Worker } from "node:worker_threads"

import { findActorDefinition } from "../../shared/actor.js"
import { errorMessage, failedReply } from "../../shared/types.js"
import type { ActorExecutorCommand, ActorExecutorReply, ActorWorkerData, ActorWorkerRequest, EvictCommand, InvokeCommand } from "../../shared/types.js"

const DEFAULT_ACTOR_IDLE_TIMEOUT_MS = 60_000
const MAX_RESIDENT_ACTORS = 32
const MAX_QUEUED_INVOCATIONS_PER_ACTOR = 32

class ActorWorkerSupervisor {
    private readonly actorEntrypointUrl: string
    private readonly actorIdleTimeoutMs: number
    private readonly actors = new Map<string, ResidentActorWorker>()

    constructor(options: ActorWorkerSupervisorOptions) {
        this.actorEntrypointUrl = options.actorEntrypointUrl
        this.actorIdleTimeoutMs = options.actorIdleTimeoutMs ?? DEFAULT_ACTOR_IDLE_TIMEOUT_MS
        if (!Number.isInteger(this.actorIdleTimeoutMs) || this.actorIdleTimeoutMs <= 0) {
            throw new Error("actor idle timeout must be a positive integer")
        }
    }

    async handle(command: ActorExecutorCommand): Promise<ActorExecutorReply> {
        switch (command.type) {
            case "invoke":
                return this.invoke(command)
            case "evict":
                return this.evict(command)
            default:
                throw command satisfies never
        }
    }

    private invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        const definition = findActorDefinition(command.actor.actor_type)
        if (definition === undefined) {
            return Promise.resolve(failedReply("actor_type_not_found", `actor type ${command.actor.actor_type} is not loaded in this customer process`))
        }
        const key = actorKey(command)
        let actor = this.actors.get(key)
        if (actor === undefined) {
            if (!this.makeRoomForActor()) {
                return Promise.resolve(failedReply("resource_exhausted", `actor host already has ${MAX_RESIDENT_ACTORS} active actor Workers`))
            }
            actor = new ResidentActorWorker({
                actorType: definition.actorType,
                moduleUrl: this.actorEntrypointUrl,
                idleTimeoutMs: this.actorIdleTimeoutMs,
                onIdle: candidate => this.removeIfCurrent(key, candidate)
            })
            this.actors.set(key, actor)
        }
        return actor.invoke(command)
    }

    private evict(command: EvictCommand): ActorExecutorReply {
        const key = actorKey(command)
        const actor = this.actors.get(key)
        if (actor !== undefined) {
            this.actors.delete(key)
            actor.terminate("resident actor was evicted by the Rust host")
        }
        return { type: "evicted" }
    }

    private makeRoomForActor(): boolean {
        if (this.actors.size < MAX_RESIDENT_ACTORS) return true
        const idle = [...this.actors.entries()].filter(([, actor]) => actor.isIdle()).sort((left, right) => left[1].lastCompletedAt - right[1].lastCompletedAt)[0]
        if (idle === undefined) return false
        this.actors.delete(idle[0])
        idle[1].terminate("resident actor was evicted by the LRU capacity limit")
        return true
    }

    private removeIfCurrent(key: string, actor: ResidentActorWorker): void {
        if (this.actors.get(key) !== actor || !actor.isIdle()) return
        this.actors.delete(key)
        actor.terminate(`resident actor was idle for ${this.actorIdleTimeoutMs}ms`)
    }
}

class ResidentActorWorker {
    readonly actorType: string
    readonly moduleUrl: string
    readonly idleTimeoutMs: number
    readonly onIdle: (actor: ResidentActorWorker) => void
    lastCompletedAt = Date.now()
    private worker: ActorWorker | undefined
    private tail = Promise.resolve()
    private outstanding = 0
    private idleTimer: NodeJS.Timeout | undefined

    constructor(options: ResidentActorWorkerOptions) {
        this.actorType = options.actorType
        this.moduleUrl = options.moduleUrl
        this.idleTimeoutMs = options.idleTimeoutMs
        this.onIdle = options.onIdle
    }

    invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        if (this.outstanding > MAX_QUEUED_INVOCATIONS_PER_ACTOR) {
            return Promise.resolve(failedReply("resource_exhausted", `actor already has ${MAX_QUEUED_INVOCATIONS_PER_ACTOR} queued invocations`))
        }
        this.outstanding += 1
        if (this.idleTimer !== undefined) clearTimeout(this.idleTimer)
        const invocation = this.tail.then(() => this.invokeNext(command))
        this.tail = invocation.then(
            () => undefined,
            () => undefined
        )
        return invocation
    }

    isIdle(): boolean {
        return this.outstanding === 0
    }

    terminate(reason: string): void {
        if (this.idleTimer !== undefined) clearTimeout(this.idleTimer)
        this.worker?.terminate(reason)
        this.worker = undefined
    }

    private async invokeNext(command: InvokeCommand): Promise<ActorExecutorReply> {
        this.worker ??= new ActorWorker({ actorType: this.actorType, moduleUrl: this.moduleUrl })
        let reply: ActorExecutorReply
        try {
            reply = await this.worker.invoke(command)
        } catch (error) {
            reply = failedReply(error instanceof ActorWorkerTerminatedError ? "actor_worker_terminated" : "actor_worker_failed", errorMessage(error))
        } finally {
            this.outstanding -= 1
            this.lastCompletedAt = Date.now()
        }
        if (reply.type === "failed") {
            this.worker.terminate("actor invocation failed")
            this.worker = undefined
        }
        if (this.outstanding === 0) {
            this.idleTimer = setTimeout(() => this.onIdle(this), this.idleTimeoutMs)
            this.idleTimer.unref()
        }
        return reply
    }
}

class ActorWorker {
    private readonly worker: Worker
    private replyResolve: ((reply: ActorExecutorReply) => void) | undefined
    private replyReject: ((error: Error) => void) | undefined
    private terminalError: Error | undefined

    constructor(data: ActorWorkerData) {
        this.worker = new Worker(new URL("./entrypoint.js", import.meta.url), { workerData: data })
        this.worker.on("message", (reply: ActorExecutorReply) => this.reply(reply))
        this.worker.once("error", error => this.fail(error))
        this.worker.once("exit", code => this.fail(new Error(`actor Worker exited with code ${code}`)))
        this.worker.unref()
    }

    async invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        if (this.terminalError !== undefined) throw this.terminalError
        if (this.replyResolve !== undefined) throw new Error("actor Worker received concurrent invocations")
        this.worker.ref()
        try {
            return await new Promise<ActorExecutorReply>((resolve, reject) => {
                this.replyResolve = resolve
                this.replyReject = reject
                this.post({ type: "invoke", command })
            })
        } finally {
            if (this.replyResolve === undefined) this.worker.unref()
        }
    }

    terminate(reason: string): void {
        if (this.terminalError !== undefined) return
        const error = new ActorWorkerTerminatedError(reason)
        this.fail(error)
        void this.worker.terminate()
    }

    private reply(reply: ActorExecutorReply): void {
        const resolve = this.replyResolve
        if (resolve === undefined) return
        this.replyResolve = undefined
        this.replyReject = undefined
        resolve(reply)
        this.worker.unref()
    }

    private post(message: ActorWorkerRequest): void {
        this.worker.postMessage(message)
    }

    private fail(error: Error): void {
        if (this.terminalError !== undefined) return
        this.terminalError = error
        this.replyReject?.(error)
        this.replyResolve = undefined
        this.replyReject = undefined
        this.worker.unref()
    }
}

class ActorWorkerTerminatedError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "ActorWorkerTerminatedError"
    }
}

function actorKey(command: Pick<InvokeCommand | EvictCommand, "actor">): string {
    const actor = command.actor
    return `${actor.namespace_id}\u001f${actor.actor_type}\u001f${actor.actor_id}`
}

interface ActorWorkerSupervisorOptions {
    readonly actorEntrypointUrl: string
    readonly actorIdleTimeoutMs?: number
}

interface ResidentActorWorkerOptions {
    readonly actorType: string
    readonly moduleUrl: string
    readonly idleTimeoutMs: number
    readonly onIdle: (actor: ResidentActorWorker) => void
}

export { ActorWorkerSupervisor, DEFAULT_ACTOR_IDLE_TIMEOUT_MS, MAX_QUEUED_INVOCATIONS_PER_ACTOR, MAX_RESIDENT_ACTORS }
export type { ActorWorkerSupervisorOptions }
