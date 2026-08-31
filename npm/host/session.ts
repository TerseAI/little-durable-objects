import { writeFile } from "node:fs/promises"
import { type Socket, createConnection } from "node:net"
import { z } from "zod"

import { ActorConfigurationError, ActorProtocolError, ActorSessionError } from "../shared/errors.js"
import { parseActorSessionServerMessage } from "../shared/types.js"
import type { ActorExecutorCommand, ActorExecutorReply, ActorSessionClientMessage } from "../shared/types.js"

import { loadActorEntrypoint, resolveActorEntrypoint } from "./actorModule.js"
import { ActorWorkerSupervisor } from "./worker/supervisor.js"

const DEFAULT_ACTOR_STARTUP_TIMEOUT_MS = 10_000
const DEFAULT_ACTOR_IDLE_TIMEOUT_MS = 60_000
const MAX_IDLE_TIMEOUT_MS = 86_400_000
const MAX_MESSAGE_BYTES = 16 * 1024 * 1024

const actorSessionSettingsSchema = z.object({
    DURABLE_OBJECT_EXECUTOR_SOCKET: z.string().trim().min(1),
    DURABLE_OBJECT_ENTRYPOINT: z.string().trim().min(1).optional()
})

class ActorSession {
    private readonly cancellationGraceMs: number | undefined
    private startup: Promise<void> | undefined
    private connection: ActorSessionConnection | undefined

    constructor(options: ActorSessionOptions = {}) {
        this.cancellationGraceMs = options.cancellationGraceMs
    }

    start(): Promise<void> {
        this.startup ??= this.initialize()
        return this.startup
    }

    private async initialize(): Promise<void> {
        const settings = ActorSessionSettings.getInstance()
        const actorEntrypointUrl = await resolveActorEntrypoint(settings.actorEntrypoint)
        const actorTypes = await loadActorEntrypoint(actorEntrypointUrl)
        const supervisor = new ActorWorkerSupervisor({
            actorEntrypointUrl,
            cancellationGraceMs: this.cancellationGraceMs,
            actorIdleTimeoutMs: settings.actorIdleTimeoutMs
        })
        const commandHandler = (command: ActorExecutorCommand): Promise<ActorExecutorReply> => supervisor.handle(command)
        this.connection = await ActorSessionConnection.open(settings.socketPath, actorTypes, commandHandler, settings.startupTimeoutMs)
    }

    waitUntilDisconnected(): Promise<void> {
        if (this.connection === undefined) throw new ActorSessionError("actor session has not started")
        return this.connection.closed()
    }
}

class ActorSessionConnection {
    private buffer = ""
    private attachedResolve: (() => void) | undefined
    private attachedReject: ((error: Error) => void) | undefined
    private readonly attachedPromise: Promise<void>
    private closedResolve: (() => void) | undefined
    private readonly closedPromise: Promise<void>

    private constructor(
        private readonly socket: Socket,
        private readonly commandHandler: ActorCommandHandler
    ) {
        this.attachedPromise = new Promise<void>((resolve, reject) => {
            this.attachedResolve = resolve
            this.attachedReject = reject
        })
        this.closedPromise = new Promise<void>(resolve => {
            this.closedResolve = resolve
        })
        this.bindSocket()
    }

    static async open(socketPath: string, actorTypes: readonly string[], commandHandler: ActorCommandHandler, timeoutMs: number): Promise<ActorSessionConnection> {
        if (actorTypes.length === 0) throw new ActorSessionError("the actor entrypoint does not export any actor classes")
        const socket = await connectSocket(socketPath)
        const connection = new ActorSessionConnection(socket, commandHandler)
        connection.send({ type: "attach", protocol: 9, actor_types: actorTypes })
        await connection.waitUntilAttached(timeoutMs)
        return connection
    }

    closed(): Promise<void> {
        return this.closedPromise
    }

    private waitUntilAttached(timeoutMs: number): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(() => {
                const error = new ActorSessionError(`actor session attachment timed out after ${timeoutMs}ms`)
                this.fail(error)
                reject(error)
            }, timeoutMs)
            void this.attachedPromise.then(
                () => {
                    clearTimeout(timeout)
                    resolve()
                },
                error => {
                    clearTimeout(timeout)
                    reject(error)
                }
            )
        })
    }

    private bindSocket(): void {
        this.socket.setEncoding("utf8")
        this.socket.on("data", (chunk: string) => this.acceptChunk(chunk))
        this.socket.once("error", error => this.fail(error))
        this.socket.once("close", () => this.close())
    }

    private acceptChunk(chunk: string): void {
        this.buffer += chunk
        if (Buffer.byteLength(this.buffer) > MAX_MESSAGE_BYTES) {
            this.fail(new ActorProtocolError("actor session message is too large"))
            return
        }

        let newline = this.buffer.indexOf("\n")
        while (newline !== -1) {
            const document = this.buffer.slice(0, newline)
            this.buffer = this.buffer.slice(newline + 1)
            void this.handle(document)
            newline = this.buffer.indexOf("\n")
        }
    }

    private async handle(document: string): Promise<void> {
        try {
            const message = parseActorSessionServerMessage(document)
            switch (message.type) {
                case "attached":
                    this.attachedResolve?.()
                    this.attachedResolve = undefined
                    this.attachedReject = undefined
                    break
                case "command":
                    this.send({
                        type: "reply",
                        message_id: message.message_id,
                        reply: await this.commandHandler(message.command)
                    })
                    break
                default:
                    throw message satisfies never
            }
        } catch (error) {
            this.fail(sessionError(error))
        }
    }

    private send(message: ActorSessionClientMessage): void {
        const document = `${JSON.stringify(message)}\n`
        if (Buffer.byteLength(document) > MAX_MESSAGE_BYTES) throw new ActorSessionError("actor session message is too large")
        this.socket.write(document)
    }

    private fail(error: Error): void {
        this.attachedReject?.(error)
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.socket.destroy()
    }

    private close(): void {
        this.attachedReject?.(new ActorSessionError("Rust host disconnected from actor session"))
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.closedResolve?.()
        this.closedResolve = undefined
    }
}

function connectSocket(socketPath: string): Promise<Socket> {
    return new Promise((resolve, reject) => {
        const socket = createConnection(socketPath)
        const onError = (error: Error): void => {
            socket.off("connect", onConnect)
            socket.destroy()
            reject(new ActorSessionError(`could not attach to Rust host at ${socketPath}`, { cause: error }))
        }
        const onConnect = (): void => {
            socket.off("error", onError)
            resolve(socket)
        }
        socket.once("error", onError)
        socket.once("connect", onConnect)
    })
}

function sessionError(error: unknown): Error {
    return error instanceof Error ? error : new ActorSessionError(String(error))
}

function parseStartupTimeout(value: string | undefined): number {
    if (value === undefined) return DEFAULT_ACTOR_STARTUP_TIMEOUT_MS
    const parsed = Number(value)
    if (!Number.isInteger(parsed) || parsed <= 0) throw new ActorConfigurationError("DURABLE_OBJECT_HOST_STARTUP_MS must be a positive integer")
    return parsed
}

function parseActorIdleTimeout(value: string | undefined): number {
    if (value === undefined) return DEFAULT_ACTOR_IDLE_TIMEOUT_MS
    const parsed = Number(value)
    if (!Number.isInteger(parsed) || parsed <= 0 || parsed > MAX_IDLE_TIMEOUT_MS) {
        throw new ActorConfigurationError(`DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS must be an integer between 1 and ${MAX_IDLE_TIMEOUT_MS}`)
    }
    return parsed
}

class ActorSessionSettings {
    private static instance: ActorSessionSettings | undefined

    private constructor(
        readonly socketPath: string,
        readonly actorEntrypoint: string | undefined,
        readonly startupTimeoutMs: number,
        readonly actorIdleTimeoutMs: number
    ) {}

    static getInstance(): ActorSessionSettings {
        this.instance ??= ActorSessionSettings.fromEnvironment(process.env)
        return this.instance
    }

    static resetForTests(): void {
        this.instance = undefined
    }

    static fromEnvironment(environment: NodeJS.ProcessEnv): ActorSessionSettings {
        const result = actorSessionSettingsSchema.safeParse(environment)
        if (!result.success) throw new ActorConfigurationError(`actor-host session settings are invalid: ${result.error.message}`)
        return new ActorSessionSettings(
            result.data.DURABLE_OBJECT_EXECUTOR_SOCKET,
            result.data.DURABLE_OBJECT_ENTRYPOINT,
            parseStartupTimeout(environment.DURABLE_OBJECT_HOST_STARTUP_MS),
            parseActorIdleTimeout(environment.DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS)
        )
    }
}

/** Start the JavaScript executor inside a dedicated actor-host sandbox. */
async function runActorHost(): Promise<never> {
    const session = new ActorSession()
    await session.start()

    const readyFile = process.env.DURABLE_OBJECT_HOST_READY_FILE
    if (readyFile) await writeFile(readyFile, `${Date.now()}\n`, { mode: 0o600 })

    await session.waitUntilDisconnected()
    throw new ActorSessionError("Rust host disconnected from actor session")
}

interface ActorSessionOptions {
    readonly cancellationGraceMs?: number
}

type ActorCommandHandler = (command: ActorExecutorCommand) => Promise<ActorExecutorReply>

export { ActorSession, ActorSessionSettings, runActorHost }
export type { ActorSessionOptions }
