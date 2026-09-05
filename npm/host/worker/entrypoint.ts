import { parentPort, workerData } from "node:worker_threads"

import { findActorDefinition } from "../../shared/actor.js"
import { errorMessage, failedReply } from "../../shared/types.js"
import type { ActorWorkerData, ActorWorkerMessage, ActorWorkerRequest } from "../../shared/types.js"
import { loadActorEntrypoint } from "../actorModule.js"

import { ActorRuntime } from "./runtime.js"

const port = parentPort
if (port === null) throw new Error("actor Worker requires a parent message port")

const data = workerData as ActorWorkerData
try {
    const actorTypes = await loadActorEntrypoint(data.moduleUrl)
    let runtime: ActorRuntime | undefined

    port.on("message", (message: ActorWorkerRequest) => {
        const definition = findActorDefinition(message.command.actor.actor_type)
        if (definition === undefined) {
            post(failedReply("actor_type_not_found", `actor entrypoint ${data.moduleUrl} does not export ${message.command.actor.actor_type}`))
            return
        }
        runtime ??= new ActorRuntime(definition)
        void runtime.handle(message.command).then(
            reply => post(reply),
            error => post(failedReply("actor_worker_failed", errorMessage(error)))
        )
    })
    post({ type: "ready", actorTypes })
} catch (error) {
    post(failedReply("actor_worker_failed", errorMessage(error)))
}

function post(message: ActorWorkerMessage): void {
    port!.postMessage(message)
}
