import { Worker } from "node:worker_threads"

import type { ActorDefinition } from "../../shared/actor.js"
import { findActorDefinition } from "../../shared/actor.js"
import { errorMessage, failedReply } from "../../shared/types.js"
import type { ActorExecutorCommand, ActorExecutorReply, ActorWorkerData, ActorWorkerRequest, CancelCommand, InvokeCommand } from "../../shared/types.js"

const DEFAULT_CANCELLATION_GRACE_MS = 1_000

class ActorWorkerSupervisor {
    private readonly actorEntrypointUrl: string
    private readonly cancellationGraceMs: number
    private readonly active = new Map<string, ActiveWorkerInvocation>()

    constructor(options: ActorWorkerSupervisorOptions) {
        this.actorEntrypointUrl = options.actorEntrypointUrl
        this.cancellationGraceMs = options.cancellationGraceMs ?? DEFAULT_CANCELLATION_GRACE_MS
        if (!Number.isInteger(this.cancellationGraceMs) || this.cancellationGraceMs <= 0) {
            throw new Error("actor cancellation grace must be a positive integer")
        }
    }

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
        const definition = findActorDefinition(command.actor.actor_type)
        if (definition === undefined) {
            return failedReply("actor_type_not_found", `actor type ${command.actor.actor_type} is not loaded in this customer process`)
        }
        const key = invocationKey(command)
        const worker = this.worker(definition)
        const active: ActiveWorkerInvocation = { worker }
        this.active.set(key, active)
        let reply: ActorExecutorReply
        try {
            reply = await worker.invoke(command)
        } catch (error) {
            reply = failedReply(error instanceof ActorWorkerTerminatedError ? "actor_worker_terminated" : "actor_worker_failed", errorMessage(error))
        } finally {
            if (active.terminationTimer !== undefined) clearTimeout(active.terminationTimer)
            if (this.active.get(key) === active) this.active.delete(key)
            worker.terminate("actor invocation completed")
        }
        return reply
    }

    private cancel(command: CancelCommand): ActorExecutorReply {
        const key = invocationKey(command)
        const active = this.active.get(key)
        if (active === undefined) return { type: "cancelled" }

        active.worker.cancel(command)
        active.terminationTimer ??= setTimeout(() => {
            if (this.active.get(key) !== active) return
            active.worker.terminate(`actor invocation did not terminate within ${this.cancellationGraceMs}ms of cancellation`)
        }, this.cancellationGraceMs)
        return { type: "cancelled" }
    }

    private worker(definition: ActorDefinition): ActorWorker {
        return new ActorWorker({
            actorType: definition.actorType,
            moduleUrl: this.actorEntrypointUrl
        })
    }
}

class ActorWorker {
    private readonly worker: Worker
    private replyResolve: ((reply: ActorExecutorReply) => void) | undefined
    private replyReject: ((error: Error) => void) | undefined
    private terminalError: Error | undefined

    constructor(data: ActorWorkerData) {
        this.worker = new Worker(new URL("./entrypoint.js", import.meta.url), { workerData: data })
        this.worker.once("message", (reply: ActorExecutorReply) => this.reply(reply))
        this.worker.once("error", error => this.fail(error))
        this.worker.once("exit", code => {
            if (this.terminalError === undefined && this.replyResolve !== undefined) this.fail(new Error(`actor Worker exited before replying with code ${code}`))
        })
        this.worker.unref()
    }

    async invoke(command: InvokeCommand): Promise<ActorExecutorReply> {
        if (this.terminalError !== undefined) throw this.terminalError
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

    cancel(command: CancelCommand): void {
        if (this.terminalError === undefined) this.post({ type: "cancel", command })
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

function invocationKey(command: Pick<InvokeCommand, "actor" | "request_id">): string {
    const actor = command.actor
    return `${actor.namespace_id}\u001f${actor.actor_type}\u001f${actor.actor_id}\u001f${command.request_id}`
}

interface ActiveWorkerInvocation {
    readonly worker: ActorWorker
    terminationTimer?: NodeJS.Timeout
}

interface ActorWorkerSupervisorOptions {
    readonly actorEntrypointUrl: string
    readonly cancellationGraceMs?: number
}

export { ActorWorkerSupervisor, DEFAULT_CANCELLATION_GRACE_MS }
export type { ActorWorkerSupervisorOptions }
