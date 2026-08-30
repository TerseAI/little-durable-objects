import { parentPort, workerData } from "node:worker_threads"

import { findActorDefinition } from "../../shared/actor.js"
import { errorMessage, failedReply } from "../../shared/types.js"
import type { ActorExecutorReply, ActorWorkerData, ActorWorkerRequest } from "../../shared/types.js"
import { loadActorEntrypoint } from "../actorModule.js"

import { ActorRuntime } from "./runtime.js"

const port = parentPort
if (port === null) throw new Error("actor Worker requires a parent message port")

const data = workerData as ActorWorkerData
try {
    await loadActorEntrypoint(data.moduleUrl)
    const definition = findActorDefinition(data.actorType)
    if (definition === undefined) throw new Error(`actor entrypoint ${data.moduleUrl} does not export ${data.actorType}`)
    const runtime = new ActorRuntime(definition)

    port.on("message", (message: ActorWorkerRequest) => {
        switch (message.type) {
            case "invoke":
                void runtime.handle(message.command).then(
                    reply => post(reply),
                    error => post(failedReply("actor_worker_failed", errorMessage(error)))
                )
                break
            case "cancel":
                void runtime.handle(message.command)
                break
            default:
                throw message satisfies never
        }
    })
} catch (error) {
    post(failedReply("actor_worker_failed", errorMessage(error)))
}

function post(reply: ActorExecutorReply): void {
    port!.postMessage(reply)
}
